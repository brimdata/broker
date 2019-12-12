#pragma once

#include <map>
#include <vector>

#include "broker/alm/routing_table.hh"

namespace broker::alm {

/// CRTP base class for represents a Broker peer in the network. The `Derived`
/// class must provide the following interface:
///
/// ~~~
/// class Derived {
///   template <class... Ts>
///   void send(const CommunicationHandle& receiver, Ts&&... xs);
///
///   const PeerId& id() const noexcept;
/// };
/// ~~~
template <class Derived, class PeerId, class CommunicationHandle>
class peer {
public:
  using routing_table_type = routing_table<PeerId, CommunicationHandle>;

  using peer_id_type = PeerId;

  using communication_handle_type = CommunicationHandle;

  void subscribe(topic what) {
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

  bool add_connection(peer_id_type remote_peer, communication_handle_type hdl) {
    BROKER_TRACE(BROKER_ARG(remote_peer) << BROKER_ARG(hdl));
    return tbl_.emplace(std::move(remote_peer), std::move(hdl)).second;
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
      BROKER_WARNING("received join from an unrecognized connection");
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

  void handle_publication(std::vector<peer_id_type>& receivers,
                          data_message msg, uint16_t ttl) {
    auto i = std::remove(receivers.begin(), receivers.end(), dref().id());
    if (i != receivers.end()) {
      receivers.erase(i, receivers.end());
      // TODO: ship to local subscribers
    }
    if (!receivers.empty()) {
      if (--ttl == 0) {
        BROKER_WARNING("drop published message: TTL expired");
        return;
      }
      ship(receivers, msg, ttl);
    }
  }

  void publish(data_message msg) {
    std::vector<peer_id_type> receivers;
    auto& topic = get_topic(msg);
    for (auto& kvp : peer_subscriptions_)
      if (kvp.first.prefix_of(topic))
        receivers.insert(receivers.end(), kvp.second.begin(), kvp.second.end());
    if (receivers.empty()) {
      BROKER_DEBUG("no subscribers found for topic");
      puts("no subscribers found for topic");
      return;
    }
    ship(receivers, msg, initial_ttl_);
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

private:
  /// Forwards `msg` to all `receivers`.
  void ship(const std::vector<peer_id_type>& receivers, data_message& msg,
            uint16_t ttl) {
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
    for (auto& receiver : receivers) {
      if (auto bucket_ptr = get_bucket(receiver))
        bucket_ptr->emplace_back(receiver);
    }
    for (auto& [first_hop, entry] : buckets) {
      auto& [hdl, bucket] = entry;
      if (!bucket.empty())
        dref().send(hdl, atom::publish::value, std::move(bucket), msg, ttl);
    }
  }

  Derived& dref() {
    return static_cast<Derived&>(*this);
  }

  /// Stores routing information for reaching other peers.
  routing_table_type tbl_;

  /// Initial TTL count.
  uint16_t initial_ttl_ = 20;

  /// A logical timestamp.
  uint64_t timestamp_ = 0;

  std::unordered_map<peer_id_type, uint64_t> peer_timestamps_;

  /// Stores subscriptions from local subscribers.
  std::vector<topic> subscriptions_;

  /// Stores all subscriptions from other peers.
  std::map<topic, std::vector<peer_id_type>> peer_subscriptions_;
};

} // namespace broker::alm
