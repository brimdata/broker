#pragma once

#include <map>
#include <vector>

#include <caf/behavior.hpp>

#include "broker/alm/routing_table.hh"
#include "broker/atoms.hh"
#include "broker/detail/assert.hh"
#include "broker/detail/lift.hh"
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
/// (atom::subscribe, peer_id_list path, filter_type filter, uint64_t t) -> void
/// => handle_subscription(path, filter, t)
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

  // -- properties -------------------------------------------------------------

  auto& tbl() noexcept {
    return tbl_;
  }

  const auto& tbl() const noexcept {
    return tbl_;
  }

  const auto& subscriptions() const noexcept {
    return subscriptions_;
  }

  const auto& peer_subscriptions() const noexcept {
    return peer_subscriptions_;
  }

  auto peer_subscriptions(const peer_id_type& x) const {
    auto predicate = [&](const auto& y) { return x == y; };
    filter_type result;
    for (const auto& kvp : peer_subscriptions_)
      if (std::any_of(kvp.second.begin(), kvp.second.end(), predicate))
        result.emplace_back(kvp.first);
    return result;
  }

  auto ttl() const noexcept {
    return ttl_;
  }

  auto timestamp() const noexcept {
    return timestamp_;
  }

  auto peer_handles() const noexcept {
    std::vector<caf::actor> result;
    for (auto& kvp : tbl_)
      result.emplace_back(kvp.second.hdl);
    return result;
  }

  // -- convenience functions for routing information --------------------------

  optional<size_t> distance_to(const peer_id_type& remote_peer) {
    // Check for direct connection first.
    auto i = tbl_.find(remote_peer);
    if (i != tbl_.end())
      return size_t{1};
    // Get the path with the lowest distance.
    peer_id_type bucket_name;
    auto distance = std::numeric_limits<size_t>::max();
    for (auto& kvp : tbl_) {
      auto i = kvp.second.distances.find(remote_peer);
      if (i != kvp.second.distances.end())
        if (i->second < distance)
          distance = i->second;
    }
    if (distance != std::numeric_limits<size_t>::max())
      return distance;
    return nil;
  }

  // -- publish and subscribe functions ----------------------------------------

  void subscribe(const filter_type& what) {
    BROKER_TRACE(BROKER_ARG(what));
    // The new topic
    if (!filter_extend(subscriptions_, what)) {
      BROKER_DEBUG("already subscribed to topic");
      return;
    }
    ++timestamp_;
    peer_id_list path{dref().id()};
    for (auto& kvp : tbl_)
      dref().send(kvp.second.hdl, atom::subscribe::value, path, subscriptions_,
                  timestamp_);
  }

  template <class T>
  void publish(const T& content) {
    auto& topic = get_topic(content);
    peer_id_list receivers;
    for (auto& kvp : peer_subscriptions_)
      if (kvp.first.prefix_of(topic))
        receivers.insert(receivers.end(), kvp.second.begin(), kvp.second.end());
    if (receivers.empty()) {
      BROKER_DEBUG("no subscribers found for topic");
      return;
    }
    message_type msg{content, ttl_, std::move(receivers)};
    BROKER_ASSERT(ttl_ > 0);
    dref().ship(msg);
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

  void handle_subscription(peer_id_list& path, const filter_type& filter,
                           uint64_t timestamp) {
    BROKER_TRACE(BROKER_ARG(path)
                 << BROKER_ARG(filter) << BROKER_ARG(timestamp));
    // Drop nonsense messages.
    if (path.empty() || filter.empty()) {
      BROKER_WARNING("drop nonsense message");
      return;
    }
    auto src_iter = tbl_.find(path.back());
    if (src_iter == tbl_.end()) {
      BROKER_WARNING("received subscription from an unrecognized connection");
      return;
    }
    auto& src_entry = src_iter->second;
    // Drop all paths that contain loops.
    auto contains = [](const std::vector<peer_id_type>& ids,
                       const peer_id_type& id) {
      auto predicate = [&](const peer_id_type& pid) { return pid == id; };
      return std::any_of(ids.begin(), ids.end(), predicate);
    };
    if (contains(path, dref().id())) {
      BROKER_DEBUG("drop path containing a loop");
      return;
    }
    // Update distance of indirect paths.
    size_t distance = path.size();
    if (distance > std::numeric_limits<uint16_t>::max()) {
      BROKER_WARNING("detected path with distance > 65535: drop");
      return;
    }
    // TODO: consider re-calculating the TTL from all distances, because
    //       currently the TTL only grows and represents the peak distance.
    ttl_ = std::max(ttl_, static_cast<uint16_t>(distance));
    if (distance > 1) {
      auto& ipaths = src_entry.distances;
      auto i = ipaths.find(path[0]);
      if (i == ipaths.end())
        ipaths.emplace(path[0], distance);
      else if (i->second > distance)
        i->second = distance;
    }
    // Forward subscription to all peers.
    path.emplace_back(dref().id());
    for (auto& [pid, entry] : tbl_)
      if (!contains(path, pid))
        dref().send(entry.hdl, atom::subscribe::value, path, filter, timestamp);
    // Store the subscription if it's new.
    auto subscriber = path[0];
    auto& ts = peer_timestamps_[subscriber];
    if (ts < timestamp) {
      // TODO: we can do this more efficient.
      for (auto& kvp : peer_subscriptions_) {
        auto& vec = kvp.second;
        vec.erase(std::remove(vec.begin(), vec.end(), subscriber), vec.end());
      }
      for (auto& x : filter)
        peer_subscriptions_[x].emplace_back(subscriber);
      ts = timestamp;
    }
  }

  void handle_publication(message_type& msg) {
    auto ttl = --get_unshared_ttl(msg);
    auto& receivers = get_unshared_receivers(msg);
    auto i = std::remove(receivers.begin(), receivers.end(), dref().id());
    if (i != receivers.end()) {
      receivers.erase(i, receivers.end());
      if (is_data_message(msg))
        dref().ship_locally(get_data_message(msg));
      else
        dref().ship_locally(get_command_message(msg));
    }
    if (!receivers.empty()) {
      if (ttl == 0) {
        BROKER_WARNING("drop message: TTL expired");
        return;
      }
      ship(msg);
    }
  }

  /// Forwards `msg` to all receivers.
  void ship(message_type& msg) {
    // Use one bucket for each direct connection. Then put all receivers into
    // the bucket with the shortest path to that receiver. On a tie, we pick
    // the alphabetically first bucket.
    using handle_type = communication_handle_type;
    using value_type = std::pair<handle_type, peer_id_list>;
    std::map<peer_id_type, value_type> buckets;
    for (auto& kvp : tbl_)
      buckets.emplace(kvp.first,
                      std::pair(kvp.second.hdl, peer_id_list{}));
    auto get_bucket = [&](const peer_id_type& x) -> peer_id_list* {
      // Check for direct connection.
      auto i = buckets.find(x);
      if (i != buckets.end())
        return &i->second.second;
      // Get the path with the lowest distance.
      peer_id_type bucket_name;
      auto distance = std::numeric_limits<size_t>::max();
      for (auto& kvp : tbl_) {
        auto i = kvp.second.distances.find(x);
        if (i != kvp.second.distances.end()) {
          if (i->second < distance) {
            bucket_name = kvp.first;
            distance = i->second;
          }
        }
      }
      // Sanity check.
      bool bucket_invalid;
      if constexpr (std::is_constructible<bool, peer_id_type>::value)
        bucket_invalid = !static_cast<bool>(bucket_name);
      else
        bucket_invalid = bucket_name.empty();
      if (bucket_invalid) {
        BROKER_DEBUG("no path found for " << x);
        return nullptr;
      }
      return &buckets[bucket_name].second;
    };
    for (auto& receiver : get_receivers(msg)) {
      if (auto bucket_ptr = get_bucket(receiver))
        bucket_ptr->emplace_back(receiver);
    }
    for (auto& [first_hop, entry] : buckets) {
      auto& [hdl, bucket] = entry;
      if (!bucket.empty()) {
        // TODO: we always make one copy more than necessary here.
        auto msg_cpy = msg;
        get_unshared_receivers(msg_cpy) = std::move(bucket);
        dref().send(hdl, atom::publish::value, std::move(msg_cpy));
      }
    }
  }

  /// Forwards `msg` to `receiver`.
  void ship(data_message& data_msg, const peer_id_type& receiver) {
    // Prepare node message.
      message_type msg{std::move(data_msg), ttl_, {receiver}};
    // Check for direct connection.
    if (auto i = tbl_.find(receiver); i != tbl_.end()) {
      dref().send(i->second.hdl, atom::publish::value, std::move(msg));
      return;
    }
    // Find the peer with the shortest path to the receiver. On a tie, we pick
    // the alphabetically first peer.
    peer_id_type hop_id;
    communication_handle_type hop_hdl;
    size_t distance = std::numeric_limits<size_t>::max();
    for (auto& [peer_id, entry] : tbl_) {
      if (auto i = entry.distances.find(receiver); i != entry.distances.end()) {
        auto x = i->second;
        if (x < distance || (x == distance && peer_id < hop_id)) {
          hop_id = peer_id;
          hop_hdl = entry.hdl;
          distance = x;
        }
      }
    }
    if (hop_hdl) {
      dref().send(hop_hdl, atom::publish::value, std::move(msg));
    } else {
      BROKER_DEBUG("no path found for " << receiver);
    }
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

  /// Called whenever this peer established a new connection.
  /// @param remote_id ID of the newly connected peer.
  /// @param hdl Communication handle for exchanging messages with the new peer.
  /// @note The new peer gets stored in the routing table *before* calling this
  ///       member function.
  void peer_connected([[maybe_unused]] const peer_id_type& remote_id,
                      [[maybe_unused]] const communication_handle_type& hdl) {
    // nop
  }

  /// Called whenever this peer lost connection to a remote peer.
  /// @param remote_id ID of the disconnected peer.
  /// @param hdl Communication handle for exchanging messages with the new peer.
  /// @param reason None if we closed the connection gracefully, otherwise
  ///               contains the transport-specific error code.
  void peer_disconnected([[maybe_unused]] const peer_id_type& remote_id,
                         [[maybe_unused]] const communication_handle_type& hdl,
                         [[maybe_unused]] const error& reason) {
    tbl_.erase(remote_id);
    if (distance_to(remote_id) == nil) {
      auto& subs = peer_subscriptions_;
      for (auto i = subs.begin(); i != subs.end();) {
        auto& lst = i->second;
        lst.erase(std::remove(lst.begin(), lst.end(), remote_id), lst.end());
        if (lst.empty())
          i = subs.erase(i);
        else
          ++i;
      }
    }
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
      lift<atom::subscribe>(d, &Derived::handle_subscription),
      [=](atom::get, atom::id) { return dref().id(); },
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

  /// Stores the maximum distance to any node.
  uint16_t ttl_ = 0;

  /// A logical timestamp.
  uint64_t timestamp_ = 0;

  std::unordered_map<peer_id_type, uint64_t> peer_timestamps_;

  /// Stores subscriptions from local subscribers.
  std::vector<topic> subscriptions_;

  /// Stores all subscriptions from other peers.
  std::map<topic, peer_id_list> peer_subscriptions_;
};

} // namespace broker::alm
