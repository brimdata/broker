#pragma once

#include <map>
#include <vector>

#include <caf/behavior.hpp>

#include "broker/alm/routing_table.hh"
#include "broker/atoms.hh"
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
/// (atom::get, atom::id)
/// -> peer_id_type id()
///
/// (atom::publish, data_message)
/// -> void publish_data(...)
///
/// (atom::publish, command_message)
/// -> void publish_command(...)
///
/// (atom::subscribe, filter_type)
/// -> void subscribe(...)
///
/// (atom::publish, node_message)
/// -> void handle_publication(...)
///
/// (atom::subscribe, vector<peer_id_type>, vector<topic>, uint64_t)
/// -> void handle_subscription(...)
/// ~~~
template <class Derived, class PeerId, class CommunicationHandle>
class peer {
public:
  // -- member types -----------------------------------------------------------

  using routing_table_type = routing_table<PeerId, CommunicationHandle>;

  using peer_id_type = PeerId;

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

  auto ttl() const noexcept {
    return ttl_;
  }

  auto timestamp() const noexcept {
    return timestamp_;
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
    std::vector<peer_id_type> path{dref().id()};
    for (auto& kvp : tbl_)
      dref().send(kvp.second.hdl, atom::subscribe::value, path, subscriptions_,
                  timestamp_);
  }

  template <class T>
  void publish(const T& content) {
    auto& topic = get_topic(content);
    std::vector<peer_id_type> receivers;
    for (auto& kvp : peer_subscriptions_)
      if (kvp.first.prefix_of(topic))
        receivers.insert(receivers.end(), kvp.second.begin(), kvp.second.end());
    if (receivers.empty()) {
      BROKER_DEBUG("no subscribers found for topic");
      puts("no subscribers found for topic");
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

  void handle_subscription(std::vector<peer_id_type>& path,
                           const std::vector<topic>& topics,
                           uint64_t timestamp) {
    BROKER_TRACE(BROKER_ARG(path)
                 << BROKER_ARG(topics) << BROKER_ARG(timestamp));
    // Drop nonsense messages.
    if (path.empty() || topics.empty()) {
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
        dref().send(entry.hdl, atom::subscribe::value, path, topics, timestamp);
    // Store the subscription if it's new.
    auto subscriber = path[0];
    auto& ts = peer_timestamps_[subscriber];
    if (ts < timestamp) {
      // TODO: we can do this more efficient.
      for (auto& kvp : peer_subscriptions_) {
        auto& vec = kvp.second;
        vec.erase(std::remove(vec.begin(), vec.end(), subscriber), vec.end());
      }
      for (auto& x : topics)
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

  /// Forwards `msg` to all `receivers`.
  void ship(message_type& msg) {
    // Use one bucket for each direct connection. Then put all receivers into
    // the bucket with the shortest path to that receiver. On a tie, we pick
    // the alphabetically first bucket.
    using handle_type = communication_handle_type;
    using value_type=std::pair<handle_type, std::vector<peer_id_type>>;
    std::map<peer_id_type, value_type> buckets;
    for (auto& kvp : tbl_)
      buckets.emplace(kvp.first,
                      std::pair(kvp.second.hdl, std::vector<peer_id_type>{}));
    auto get_bucket = [&](const peer_id_type& x) -> std::vector<peer_id_type>* {
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
      if (bucket_name.empty()) {
        std::cerr << "no path found for " << x << '\n';
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
  std::map<topic, std::vector<peer_id_type>> peer_subscriptions_;
};

} // namespace broker::alm
