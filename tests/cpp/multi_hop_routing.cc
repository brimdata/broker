#define SUITE multi_hop_routing

#include "broker/core_actor.hh"

#include "test.hh"

#include <caf/test/io_dsl.hpp>

#include "broker/configuration.hh"
#include "broker/endpoint.hh"
#include "broker/logger.hh"

using namespace caf;
using namespace broker;
using namespace broker::detail;

namespace {

using peer_id = std::string;

/// Stores direct connections to peers as well as distances to all other peers
/// that we can reach indirectly.
template <class PeerId, class CommunicationHandle>
class routing_table_row {
public:
  using peer_id_type = PeerId;

  using communication_handle_type = CommunicationHandle;

  /// Stores an implementation-specific handle for talking to the peer.
  CommunicationHandle hdl;

  /// Associates peer IDs with distance (in hops) on this path.
  std::map<PeerId, size_t> distances;

  explicit routing_table_row(CommunicationHandle hdl) : hdl(std::move(hdl)) {
    // nop
  }
};

/// Stores direct connections to peers as well as distances to all other peers
/// that we can reach indirectly.
template <class PeerId, class CommunicationHandle>
using routing_table
  = std::map<PeerId, routing_table_row<PeerId, CommunicationHandle>>;

template <class Inspector, class PeerId, class CommunicationHandle>
auto inspect(Inspector& f, routing_table_row<PeerId, CommunicationHandle>& x) {
  return f(caf::meta::type_name("row"), x.hdl, x.distances);
}

bool contains(const std::vector<peer_id>& ids, const peer_id& id) {
  auto predicate = [&](const peer_id& pid) { return pid == id; };
  return std::any_of(ids.begin(), ids.end(), predicate);
}

/// Represents a Broker peer in the network.
template <class Derived, class PeerId, class CommunicationHandle>
class peer {
public:
  using routing_table_type = routing_table<PeerId, CommunicationHandle>;

  using peer_id_type = PeerId;

  using communication_handle_type = CommunicationHandle;

  void subscribe(topic what) {
    BROKER_TRACE(BROKER_ARG(what));
    if (!subscriptions_.emplace(std::move(what)).second) {
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
    peer_id bucket_name;
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

  void handle_subscription(std::vector<peer_id>& path,
                           const std::set<topic>& topics, uint64_t timestamp) {
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
      for (auto& kvp : peer_subscriptions_)
        kvp.second.erase(subscriber);
      for (auto&x:topics)
        peer_subscriptions_[x].emplace(subscriber);
      ts = timestamp;
    }
  }

  void handle_publication(std::set<peer_id>& receivers, data_message msg) {
    if (receivers.erase(dref().id()) > 0) {
      // TODO: ship to local subscribers
    }
    ship(receivers, msg);
  }

  void publish(data_message msg) {
    std::set<peer_id> receivers;
    auto& topic = get_topic(msg);
    std::cout << "subs: " << deep_to_string(peer_subscriptions_) << '\n';
    for (auto& kvp : peer_subscriptions_)
      if (kvp.first.prefix_of(topic))
        receivers.insert(kvp.second.begin(), kvp.second.end());
    if (receivers.empty()) {
      BROKER_DEBUG("no subscribers found for topic");
      puts("no subscribers found for topic");
      return;
    }
    ship(receivers, msg);
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
  void ship(const std::set<peer_id_type>& receivers, data_message& msg) {
    // Use one bucket for each direct connection. Then put all receivers into
    // the bucket with the shortest path to that receiver. On a tie, we pick
    // the alphabetically first bucket.
    using handle_type = communication_handle_type;
    std::map<peer_id, std::pair<handle_type, std::set<peer_id>>> buckets;
    for (auto& kvp : tbl_)
      buckets.emplace(kvp.first,
                      std::pair(kvp.second.hdl, std::set<peer_id>{}));
    auto get_bucket = [&](const peer_id& x) -> std::set<peer_id>* {
      // Check for direct connection.
      auto i = buckets.find(x);
      if (i != buckets.end())
        return &i->second.second;
      // Get the path with the lowest distance.
      peer_id bucket_name;
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
        bucket_ptr->emplace(receiver);
    }
    for (auto& [first_hop, entry] : buckets) {
      auto& [hdl, bucket] = entry;
      if (!bucket.empty())
        dref().send(hdl, atom::publish::value, std::move(bucket), msg);
    }
  }

  Derived& dref() {
    return static_cast<Derived&>(*this);
  }

  /// Stores routing information for reaching other peers.
  routing_table_type tbl_;

  /// A logical timestamp.
  uint64_t timestamp_ = 0;

  std::unordered_map<peer_id_type, uint64_t> peer_timestamps_;

  /// Stores subscriptions from local subscribers.
  std::set<topic> subscriptions_;

  /// Stores all subscriptions from other peers.
  std::map<topic, std::set<peer_id_type>> peer_subscriptions_;
};

class peer_actor_state : public peer<peer_actor_state, peer_id, caf::actor> {
public:
  peer_actor_state(event_based_actor* self) : self_(self) {
    // nop
  }

  template <class... Ts>
  void send(const caf::actor& receiver, Ts&&... xs) {
    self_->send(receiver, std::forward<Ts>(xs)...);
  }

  const auto& id() const noexcept {
    return id_;
  }

  void id(peer_id new_id) noexcept{
    id_ = std::move(new_id);
  }

private:
  event_based_actor* self_;
  peer_id id_;
};

using peer_actor_type = caf::stateful_actor<peer_actor_state>;

template <class AtomPrefix, class T, class U, class R, class... Ts>
auto lift(T& obj, R (U::*fun)(Ts...)) {
  if constexpr (std::is_same<AtomPrefix, broker::none>::value)
    return [&obj, fun](Ts... xs) { return (obj.*fun)(xs...); };
  else
    return [&obj, fun](AtomPrefix, Ts... xs) { return (obj.*fun)(xs...); };
}

caf::behavior peer_actor(peer_actor_type* self, peer_id id) {
  using state_type = peer_actor_state;
  self->state.id(std::move(id));
  return {
    lift<broker::none>(self->state, &state_type::add_connection),
    lift<atom::publish>(self->state, &state_type::publish),
    lift<atom::subscribe>(self->state, &state_type::subscribe),
    lift<atom::publish>(self->state, &state_type::handle_publication),
    lift<atom::subscribe>(self->state, &state_type::handle_subscription),
  };
}

// In this fixture, we're setting up this messy topology full of loops:
//
//                                     +---+
//                               +-----+ D +-----+
//                               |     +---+     |
//                               |               |
//                             +---+           +---+
//                       +-----+ B |           | I +-+
//                       |     +---+           +---+ |
//                       |       |               |   |
//                       |       |     +---+     |   |
//                       |       +-----+ E +-----+   |
//                       |             +---+         |
//                     +---+                       +---+
//                     | A +-----------------------+ J |
//                     +---+                       +---+
//                       |             +---+        | |
//                       |       +-----+ F |        | |
//                       |       |     +-+-+        | |
//                       |       |       |          | |
//                       |     +---+   +-+-+        | |
//                       +-----+ C +---+ G +--------+ |
//                             +---+   +-+-+          |
//                               |       |            |
//                               |     +-+-+          |
//                               +-----+ H +----------+
//                                     +---+
//
struct fixture : test_coordinator_fixture<> {
  using peer_ids = std::vector<peer_id>;

  fixture(){
    for (auto& id : peer_ids{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"})
      peers[id] = sys.spawn(peer_actor, id);
    std::map<peer_id, peer_ids> connections{
      {"A", {"B", "C", "J"}},
      {"B", {"A", "D", "E"}},
      {"C", {"A", "F", "G", "H"}},
      {"D", {"B", "I"}},
      {"E", {"B", "I"}},
      {"F", {"C", "G"}},
      {"I", {"D", "E", "J"}},
      {"G", {"C", "F", "H", "J"}},
      {"H", {"C", "G", "J"}},
      {"J", {"A", "I", "G","H"}},
    };
    for (auto& [id, links] : connections)
      for (auto& link : links)
        anon_send(peers[id], link, peers[link]);
    run();
  }

  ~fixture() {
    for (auto& kvp : peers)
      anon_send_exit(kvp.second, exit_reason::user_shutdown);
  }

  auto& get(const peer_id& id) {
    return deref<peer_actor_type>(peers[id]).state;
  }

  std::map<peer_id, caf::actor> peers;
};

} // namespace

FIXTURE_SCOPE(multi_hop_routing_tests, fixture)

#define CHECK_DISTANCE(src, dst, val)                                          \
  CHECK_EQUAL(get(src).distance_to(dst), size_t{val})

TEST(topologies with loops resolve to simple forwarding tables) {
  using peer_set = std::set<peer_id>;
  MESSAGE("after all links are connected, G subscribes to topic 'foo'");
  anon_send(peers["G"], atom::subscribe::value, topic{"foo"});
  run();
  MESSAGE("after the subscription, all routing tables store a distance to G");
  CHECK_DISTANCE("A", "G", 2);
  CHECK_DISTANCE("B", "G", 3);
  CHECK_DISTANCE("C", "G", 1);
  CHECK_DISTANCE("D", "G", 3);
  CHECK_DISTANCE("E", "G", 3);
  CHECK_DISTANCE("F", "G", 1);
  CHECK_DISTANCE("H", "G", 1);
  CHECK_DISTANCE("I", "G", 2);
  CHECK_DISTANCE("J", "G", 1);
  MESSAGE("publishing to foo on A will send through C");
  anon_send(peers["A"], atom::publish::value, make_data_message("foo", 42));
  expect((atom_value, data_message), from(_).to(peers["A"]));
  expect((atom_value, peer_set, data_message),
         from(peers["A"])
           .to(peers["C"])
           .with(_, peer_set{"G"}, make_data_message("foo", 42)));
  expect((atom_value, peer_set, data_message),
         from(peers["C"])
           .to(peers["G"])
           .with(_, peer_set{"G"}, make_data_message("foo", 42)));
}

FIXTURE_SCOPE_END()
