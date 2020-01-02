#define SUITE mixin.notifier

#include "broker/mixin/notifier.hh"

#include "test.hh"

#include "broker/alm/peer.hh"
#include "broker/alm/stream_transport.hh"

using broker::alm::peer;
using broker::alm::stream_transport;

using namespace broker;

namespace {

using peer_id = std::string;

struct dummy_cache {
  template <class OnSuccess, class OnError>
  void fetch(const caf::actor&, OnSuccess f, OnError g) {
    if (enabled)
      f(network_info{"localhost", 8080});
    else
      g(make_error(caf::sec::cannot_connect_to_node));
  }

  bool enabled = true;
};

class stream_peer_manager
  : public //
    caf::extend<stream_transport<stream_peer_manager, peer_id>,
                stream_peer_manager>:: //
    with<mixin::notifier> {
public:
  using super = extended_base;

  stream_peer_manager(caf::event_based_actor* self) : super(self) {
    // nop
  }

  const auto& id() const noexcept {
    return id_;
  }

  void id(peer_id new_id) noexcept {
    id_ = std::move(new_id);
  }

  dummy_cache cache;

private:
  peer_id id_;
};

struct stream_peer_actor_state {
  caf::intrusive_ptr<stream_peer_manager> mgr;
  bool connected_to(const caf::actor& hdl) const noexcept {
    return mgr->connected_to(hdl);
  }
};

using stream_peer_actor_type = caf::stateful_actor<stream_peer_actor_state>;

caf::behavior stream_peer_actor(stream_peer_actor_type* self, peer_id id) {
  auto& mgr = self->state.mgr;
  mgr = caf::make_counted<stream_peer_manager>(self);
  mgr->id(std::move(id));
  return mgr->make_behavior();
}

struct subscriber_state {
  std::vector<std::string> log;
};

using subscriber_type = caf::stateful_actor<subscriber_state>;

caf::behavior subscriber(subscriber_type* self, caf::actor aut) {
  return {
    [=](atom::local, status& x) {
      if (self->current_sender() == aut)
        self->state.log.emplace_back(to_string(x.code()));
    },
  };
}

struct fixture : test_coordinator_fixture<> {
  using peer_ids = std::vector<peer_id>;

  fixture() {
    for (auto& id : peer_ids{"A", "B"})
      peers[id] = sys.spawn(stream_peer_actor, id);
    auto& groups = sys.groups();
    logger = sys.spawn_in_groups({groups.get_local("broker/errors"),
                                  groups.get_local("broker/statuses")},
                                 subscriber, peers["A"]);
    run();
  }

  ~fixture() {
    for (auto& kvp : peers)
      anon_send_exit(kvp.second, caf::exit_reason::user_shutdown);
    anon_send_exit(logger, caf::exit_reason::user_shutdown);
  }

  auto& get(const peer_id& id) {
    return deref<stream_peer_actor_type>(peers[id]).state;
  }

  auto& log() {
    return deref<subscriber_type>(logger).state.log;
  }

  template <class... Ts>
  std::vector<std::string> make_log(Ts&&... xs) {
    return {std::forward<Ts>(xs)...};
  }

  std::map<peer_id, caf::actor> peers;

  caf::actor logger;
};

} // namespace

FIXTURE_SCOPE(notifier_tests, fixture)

TEST(connect and graceful disconnect emits peer_added and peer_lost) {
  anon_send(peers["A"], atom::peer::value, "B", peers["B"]);
  run();
  CHECK_EQUAL(log(), make_log("peer_added"));
  anon_send_exit(peers["B"], caf::exit_reason::user_shutdown);
  run();
  CHECK_EQUAL(log(), make_log("peer_added", "peer_lost"));
}

TEST(connect and ungraceful disconnect emits peer_added and peer_lost) {
  anon_send(peers["A"], atom::peer::value, "B", peers["B"]);
  run();
  CHECK_EQUAL(log(), make_log("peer_added"));
  anon_send_exit(peers["B"], caf::exit_reason::kill);
  run();
  CHECK_EQUAL(log(), make_log("peer_added", "peer_lost"));
}

FIXTURE_SCOPE_END()
