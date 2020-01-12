#include "broker/core_actor.hh"

namespace broker {

caf::behavior core_manager::make_behavior() {
  return super::make_behavior(
    [this](atom::publish, endpoint_info& receiver, data_message& msg) {
      ship(msg, receiver.node);
    });
}

caf::behavior core_actor(core_actor_type* self, filter_type initial_filter,
                         broker_options options, endpoint::clock* clock) {
  auto& mgr = self->state.mgr;
  mgr = caf::make_counted<core_manager>(clock, self);
  mgr->subscribe(initial_filter);
  mgr->cache().set_use_ssl(! options.disable_ssl);
  return mgr->make_behavior();
}

} // namespace broker
