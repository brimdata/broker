#define SUITE alm.peer

#include "broker/core_actor.hh"

#include "test.hh"

#include <caf/test/io_dsl.hpp>

#include "broker/alm/peer.hh"
#include "broker/configuration.hh"
#include "broker/detail/lift.hh"
#include "broker/endpoint.hh"
#include "broker/logger.hh"

using broker::alm::peer;
using broker::detail::lift;

using namespace broker;

namespace {

using peer_id = std::string;

class peer_actor_state : public peer<peer_actor_state, peer_id, caf::actor> {
public:
  peer_actor_state(caf::event_based_actor* self) : self_(self) {
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
  caf::event_based_actor* self_;
  peer_id id_;
};

using peer_actor_type = caf::stateful_actor<peer_actor_state>;

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
      anon_send_exit(kvp.second, caf::exit_reason::user_shutdown);
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
  using peer_vec = std::vector<peer_id>;
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
  expect((atom_value, peer_vec, data_message, uint16_t),
         from(peers["A"])
           .to(peers["C"])
           .with(_, peer_vec{"G"}, make_data_message("foo", 42), _));
  expect((atom_value, peer_vec, data_message, uint16_t),
         from(peers["C"])
           .to(peers["G"])
           .with(_, peer_vec{"G"}, make_data_message("foo", 42), _));
}

FIXTURE_SCOPE_END()
