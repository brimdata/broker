#pragma once

#include <map>
#include <vector>

#include <caf/behavior.hpp>

#include "broker/alm/multipath.hh"
#include "broker/alm/routing_table.hh"
#include "broker/atoms.hh"
#include "broker/detail/assert.hh"
#include "broker/detail/lift.hh"
#include "broker/detail/prefix_matcher.hh"
#include "broker/error.hh"
#include "broker/filter_type.hh"
#include "broker/logger.hh"
#include "broker/message.hh"

namespace broker::alm {

/// CRTP base class that represents a Broker peer in the network. This class
/// implements subscription and path management for the overlay. Data transport
/// as well as shipping data to local subscribers is implemented by `Derived`.
///
/// The derived class *must* provide the following interface:
///
/// ~~~
/// class Derived {
///   template <class... Ts>
///   void send(const CommunicationHandle& receiver, Ts&&... xs);
///
///   const PeerId& id() const noexcept;
/// };
/// ~~~
///
/// The derived class *can* extend any of the callback member functions by
/// hiding the implementation of ::peer.
///
/// The peer registers these message handlers:
///
/// ~~~
/// (atom::get, atom::id) -> peer_id_type
/// => id()
///
/// (atom::publish, data_message msg) -> void
/// => publish_data(msg)
///
/// (atom::publish, command_message msg) -> void
/// => publish_command(msg)
///
/// (atom::subscribe, filter_type filter) -> void
/// => subscribe(filter)
///
/// (atom::publish, node_message msg) -> void
/// => handle_publication(msg)
///
/// (atom::subscribe, peer_id_list path, filter_type filter, lamport_timestamp)
/// -> void
/// => handle_filter_update(path, filter, t)
/// ~~~
template <class Derived, class PeerId, class CommunicationHandle>
class peer {
public:
  // -- member types -----------------------------------------------------------

  using routing_table_type = routing_table<PeerId, CommunicationHandle>;

  using peer_id_type = PeerId;

  using peer_id_list = std::vector<peer_id_type>;

  using communication_handle_type = CommunicationHandle;

  using message_type = generic_node_message<peer_id_type>;

  using multipath_type = multipath<peer_id_type>;

  // -- properties -------------------------------------------------------------

  auto& tbl() noexcept {
    return tbl_;
  }

  const auto& tbl() const noexcept {
    return tbl_;
  }

  const auto& filter() const noexcept {
    return filter_;
  }

  const auto& peer_filters() const noexcept {
    return peer_filters_;
  }

  auto peer_filter(const peer_id_type& x) const {
    auto i = peer_filters_.find(x);
    if (i != peer_filters_.end())
      return i->second;
    return filter_type{};
  }

  auto timestamp() const noexcept {
    return timestamp_;
  }

  auto peer_handles() const {
    std::vector<caf::actor> result;
    for (auto& kvp : tbl_)
      result.emplace_back(kvp.second.hdl);
    return result;
  }

  // -- convenience functions for subscription information ---------------------

  bool has_remote_subscriber(const topic& x) const noexcept {
    detail::prefix_matcher matches;
    for (const auto& [peer, filter] : peer_filters_)
      if (matches(filter, x))
        return true;
    return false;
  }

  // -- publish and subscribe functions ----------------------------------------

  /// Floods the subscriptions on this peer to all other peers.
  /// @note The functions does *not* bump the Lamport timestamp before sending.
  void flood_subscriptions() {
    peer_id_list path{dref().id()};
    vector_timestamp ts{timestamp_};
    for_each_direct(tbl_, [&](auto&, auto& hdl) {
      dref().send(hdl, atom::subscribe::value, path, ts, filter_);
    });
  }

  void subscribe(const filter_type& what) {
    BROKER_TRACE(BROKER_ARG(what));
    auto not_internal = [](const topic& x) { return !is_internal(x); };
    if (!filter_extend(filter_, what, not_internal)) {
      BROKER_DEBUG("already subscribed to topic");
      return;
    }
    ++timestamp_;
    flood_subscriptions();
  }

  template <class T>
  void publish(const T& content) {
    const auto& topic = get_topic(content);
    detail::prefix_matcher matches;
    peer_id_list receivers;
    for (const auto& [peer, filter] : peer_filters_)
      if (matches(filter, topic))
        receivers.emplace_back(peer);
    if (receivers.empty()) {
      BROKER_DEBUG("no subscribers found for topic" << topic);
      return;
    }
    ship(content, receivers);
  }

  void publish(node_message_content& content) {
    if (is_data_message(content))
      publish(get<data_message>(content));
    else
      publish(get<command_message>(content));
  }

  void publish_data(data_message& content) {
    publish(content);
  }

  void publish_command(command_message& content) {
    publish(content);
  }

  void handle_filter_update(peer_id_list& path, vector_timestamp path_ts,
                            const filter_type& filter) {
    BROKER_TRACE(BROKER_ARG(path) << BROKER_ARG(path_ts) << BROKER_ARG(filter));
    // Drop nonsense messages.
    if (path.empty()) {
      BROKER_WARNING("drop message: path empty");
      return;
    }
    if (path.size() != path_ts.size()) {
      BROKER_WARNING("drop message: path and timestamp have different sizes");
      return;
    }
    // Sanity check: we can only receive messages from direct connections.
    auto forwarder = find_row(tbl_, path.back());
    if (forwarder == nullptr) {
      BROKER_WARNING("received subscription from an unrecognized peer");
      return;
    }
    if (forwarder->hdl == nullptr) {
      BROKER_WARNING("received subscription from a peer we don't have a direct "
                     " connection to");
      return;
    }
    // Drop all paths that contain loops.
    auto contains = [](const std::vector<peer_id_type>& ids,
                       const peer_id_type& id) {
      auto predicate = [&](const peer_id_type& pid) { return pid == id; };
      return std::any_of(ids.begin(), ids.end(), predicate);
    };
    if (contains(path, dref().id())) {
      BROKER_DEBUG("drop message: path contains a loop");
      return;
    }
    // The reverse path leads to the sender.
    std::vector<peer_id_type> new_peers;
    auto is_new = [this](const auto& id) { return !reachable(tbl_, id); };
    for (const auto& id : path)
      if (is_new(id))
        new_peers.emplace_back(id);
    auto added_tbl_entry = add_or_update_path(
      tbl_, path[0], peer_id_list{path.rbegin(), path.rend()},
      vector_timestamp{path_ts.rbegin(), path_ts.rend()});
    BROKER_ASSERT(new_peers.empty() || added_tbl_entry);
    // We increase our own timestamp only if we have changed the routing table .
    // Otherwise, we would cause infinite flooding, because the peers would
    // never agree on a vector time.
    if (added_tbl_entry) {
      BROKER_DEBUG("increase local time");
      ++timestamp_;
    }
    // Store the subscription if it's new.
    const auto& subscriber = path[0];
    peer_timestamps_[subscriber] = path_ts[0];
    peer_filters_[subscriber] = filter;
    // Forward message to all other neighbors.
    path.emplace_back(dref().id());
    path_ts.emplace_back(timestamp_);
    for_each_direct(tbl_, [&](auto& pid, auto& hdl) {
      if (!contains(path, pid))
        dref().send(hdl, atom::subscribe::value, path, path_ts, filter);
    });
    // If we have larned a new peer, we flood our own subscriptions as well.
    if (!new_peers.empty()) {
      BROKER_DEBUG("learned new peers: " << new_peers);
      for (auto& id : new_peers)
        dref().peer_discovered(id);
      // TODO: This primarly makes sure that eventually all peers know each
      //       other. There may be more efficient ways to ensure connectivity,
      //       though.
      flood_subscriptions();
    }
  }

  void handle_publication(message_type& msg) {
    // Verify that we are supposed to handle this message.
    auto& path = get_unshared_path(msg);
    if (path.head() != dref().id()) {
      BROKER_WARNING("Received a message for a different node: drop.");
      return;
    }
    // Dispatch locally if we are on the list of receivers.
    auto& receivers = get_unshared_receivers(msg);
    auto i = std::remove(receivers.begin(), receivers.end(), dref().id());
    if (i != receivers.end()) {
      receivers.erase(i, receivers.end());
      if (is_data_message(msg))
        dref().ship_locally(get_data_message(msg));
      else
        dref().ship_locally(get_command_message(msg));
    }
    if (receivers.empty()) {
      if (!path.nodes().empty())
        BROKER_WARNING("More nodes in path but list of receivers is empty.");
      return;
    }
    for (auto& node : path.nodes()) {
      message_type nmsg{caf::get<0>(msg), std::move(node), receivers};
      ship(nmsg);
    }
  }

  /// Forwards `msg` to all receivers.
  void ship(message_type& msg) {
    const auto& path = get_path(msg);
    if (auto i = tbl_.find(path.head()); i != tbl_.end()) {
      dref().send(i->second.hdl, atom::publish::value, std::move(msg));
    } else {
      BROKER_WARNING("no path found to " << path.head());
    }
  }

  /// Forwards `data_msg` to a single `receiver`.
  template <class T>
  void ship(T& data_msg, const peer_id_type& receiver) {
    if (auto ptr = shortest_path(tbl_, receiver)) {
      message_type msg{std::move(data_msg),
                       multipath_type{ptr->begin(), ptr->end()},
                       peer_id_list{receiver}};
      ship(msg);
    } else {
      BROKER_WARNING("no path found to " << receiver);
    }
  }

  /// Forwards `data_msg` to all `receivers`.
  template <class T>
  void ship(T& data_msg, const peer_id_list& receivers) {
    std::vector<multipath_type> paths;
    std::vector<peer_id_type> unreachables;
    generate_paths(receivers, tbl_, paths, unreachables);
    for (auto& path : paths) {
      message_type nmsg{data_msg, std::move(path), receivers};
      ship(nmsg);
    }
    if (!unreachables.empty())
      BROKER_WARNING("no paths to " << unreachables);
  }

  // -- callbacks --------------------------------------------------------------

  /// Called whenever new data for local subscribers became available.
  /// @param msg Data or command message, either received by peers or generated
  ///            from a local publisher.
  /// @tparam T Either ::data_message or ::command_message.
  template <class T>
  void ship_locally([[maybe_unused]] const T& msg) {
    // nop
  }

  /// Called whenever this peer discovers a new peer in the network.
  /// @param peer_id ID of the new peer.
  /// @note The new peer gets stored in the routing table *before* calling this
  ///       member function.
  void peer_discovered([[maybe_unused]] const peer_id_type& peer_id) {
    // nop
  }

  /// Called whenever this peer established a new connection.
  /// @param peer_id ID of the newly connected peer.
  /// @param hdl Communication handle for exchanging messages with the new peer.
  ///            The handle is default-constructed if no direct connection
  ///            exists (yet).
  /// @note The new peer gets stored in the routing table *before* calling this
  ///       member function.
  void peer_connected([[maybe_unused]] const peer_id_type& peer_id,
                      [[maybe_unused]] const communication_handle_type& hdl) {
    // nop
  }

  /// Called whenever this peer lost a connection to a remote peer.
  /// @param peer_id ID of the disconnected peer.
  /// @param hdl Communication handle of the disconnected peer.
  /// @param reason None if we closed the connection gracefully, otherwise
  ///               contains the transport-specific error code.
  void peer_disconnected([[maybe_unused]] const peer_id_type& peer_id,
                         [[maybe_unused]] const communication_handle_type& hdl,
                         [[maybe_unused]] const error& reason) {
    BROKER_TRACE(BROKER_ARG(peer_id) << BROKER_ARG(hdl) << BROKER_ARG(reason));
    // Do the same cleanup steps we do for removed peers. We intentionally do
    // *not* dispatch through dref() to not trigger undesired side effects.
    peer_removed(peer_id, hdl);
  }

  /// Called whenever this peer removed a direct connection to a remote peer.
  /// @param peer_id ID of the removed peer.
  /// @param hdl Communication handle of the removed peer.
  void peer_removed([[maybe_unused]] const peer_id_type& peer_id,
                    [[maybe_unused]] const communication_handle_type& hdl) {
    BROKER_TRACE(BROKER_ARG(peer_id) << BROKER_ARG(hdl));
    auto on_drop = [this](const peer_id_type& whom) {
      dref().peer_unreachable(whom);
    };
    erase_direct(tbl_, peer_id, on_drop);
  }

  /// Called after removing the last path to `peer_id` from the routing table.
  /// @param peer_id ID of the (now unreachable) peer.
  void peer_unreachable([[maybe_unused]] const peer_id_type& peer_id) {
    peer_filters_.erase(peer_id);
  }

  /// Called whenever the user tried to unpeer from an unknown peer.
  /// @param xs Either a peer ID, an actor handle or a network info.
  template <class T>
  void cannot_remove_peer([[maybe_unused]] const T& x) {
    BROKER_DEBUG("cannot unpeer from uknown peer" << x);
  }

  /// Called whenever establishing a connection to a remote peer failed.
  /// @param xs Either a peer ID or a network info.
  template <class T>
  void peer_unavailable([[maybe_unused]] const T& x) {
    // nop
  }

  // -- factories --------------------------------------------------------------

  template <class... Fs>
  caf::behavior make_behavior(Fs... fs) {
    using detail::lift;
    auto& d = dref();
    return {
      std::move(fs)...,
      lift<atom::publish>(d, &Derived::publish_data),
      lift<atom::publish>(d, &Derived::publish_command),
      lift<atom::subscribe>(d, &Derived::subscribe),
      lift<atom::publish>(d, &Derived::handle_publication),
      lift<atom::subscribe>(d, &Derived::handle_filter_update),
      [=](atom::get, atom::id) { return dref().id(); },
      [=](atom::get, atom::peer, atom::subscriptions) {
        // For backwards-compatibility, we only report the filter of our
        // direct peers. Returning all filter would make more sense in an
        // ALM setting, but that would change the semantics of
        // endpoint::peer_filter.
        auto is_direct_peer = [this](const auto& peer_id) {
          return tbl_.count(peer_id) != 0;
        };
        filter_type result;
        for (const auto& [peer, filter] : peer_filters_)
          if (is_direct_peer(peer))
            filter_extend(result, filter);
        return result;
      },
      [=](atom::shutdown) {
        // TODO: this handler exists only for backwards-compatibility. Consider
        //       simply using CAF's exit messages instead of using this
        //       anti pattern.
        BROKER_DEBUG("received 'shutdown', call quit");
        dref().self()->quit(caf::exit_reason::user_shutdown);
      },
      [=](atom::publish, atom::local, command_message& msg) {
        dref().ship_locally(msg);
      },
      [=](atom::publish, atom::local, data_message& msg) {
        dref().ship_locally(msg);
      },
    };
  }

private:
  Derived& dref() {
    return static_cast<Derived&>(*this);
  }

  /// Stores routing information for reaching other peers. The *transport* adds
  /// new entries to this table (before calling ::peer_connected) and the peer
  /// removes entries in its ::peer_disconnected callback implementation.
  routing_table_type tbl_;

  /// A logical timestamp.
  lamport_timestamp timestamp_;

  /// Keeps track of the logical timestamps last seen from other peers.
  std::unordered_map<peer_id_type, lamport_timestamp> peer_timestamps_;

  /// Stores prefixes with subscribers on this peer.
  filter_type filter_;

  /// Stores all filters from other peers.
  std::unordered_map<peer_id_type, filter_type> peer_filters_;
};

} // namespace broker::alm
