#pragma once

#include <map>
#include <vector>

#include <caf/behavior.hpp>

#include "broker/alm/routing_table.hh"
#include "broker/atoms.hh"
#include "broker/logger.hh"
#include "broker/message.hh"

namespace broker::alm {

/// CRTP base class for represents a Broker peer in the network. The `Derived`
/// class must provide the following interface:
///
/// ~~~
/// class Derived {
///   template <class... Ts>
///   void send(const CommunicationHandle& receiver, Ts&&... xs);
///
///   void ship_locally(message_type&);
///
///   const PeerId& id() const noexcept;
/// };
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
  void publish(T& content) {
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
    message_type msg{std::move(content), ttl_, std::move(receivers)};
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

  // -- callback ---------------------------------------------------------------

  template <class T>
  void ship_locally(const T&) {
    // nop; fallback implementation
  }

private:
  Derived& dref() {
    return static_cast<Derived&>(*this);
  }

  /// Stores routing information for reaching other peers.
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
