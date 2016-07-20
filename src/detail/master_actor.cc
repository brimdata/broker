#include "broker/logger.hh" // Needs to come before CAF includes.

#include <caf/all.hpp>

#include "broker/atoms.hh"
#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/error.hh"
#include "broker/expected.hh"
#include "broker/topic.hh"
#include "broker/message.hh"
#include "broker/snapshot.hh"
#include "broker/time.hh"

#include "broker/detail/abstract_backend.hh"
#include "broker/detail/die.hh"
#include "broker/detail/master_actor.hh"
#include "broker/detail/type_traits.hh"

namespace broker {
namespace detail {

// TODO: The following aspects still need to be thought through:
// - Expiration of values.
// - Error handling when asynchronous operations fail.

caf::behavior master_actor(caf::stateful_actor<master_state>* self,
                           caf::actor core, std::string name,
                           std::unique_ptr<abstract_backend> backend) {
  self->state.backend = std::move(backend);
  auto next_seq = [=] { return ++self->state.sequence_number; };
  auto broadcast = [=](caf::message&& msg) {
    auto t = name / topics::reserved / topics::clone;
    self->send(core, std::move(t), std::move(msg), core);
  };
  self->set_down_handler(
    [=](const caf::down_msg& msg) {
      BROKER_DEBUG("lost connection to clone" << to_string(msg.source));
      self->state.clones.erase(msg.source);
    }
  );
  auto commands = caf::message_handler{
    [=](atom::put, data& key, data& value, count seq) {
      BROKER_DEBUG("PUT" << ('#' + std::to_string(seq) + ':')
                   << key << "->" << value);
      auto result = self->state.backend->put(key, value);
      if (!result) {
        BROKER_WARNING("failed to put" << key << "->" << value);
        return; // TODO: propagate failure? to all clones? as status msg?
      }
      if (!self->state.clones.empty())
        broadcast(caf::make_message(atom::put::value, std::move(key),
                                    std::move(value), next_seq()));
    },
    [=](atom::erase, data& key, count seq) {
      BROKER_DEBUG("erase" << ('#' + std::to_string(seq) + ':') << key);
      auto result = self->state.backend->erase(key);
      if (!result) {
        BROKER_WARNING("failed to erase" << key);
        return; // TODO: propagate failure? to all clones? as status msg?
      }
      if (!self->state.clones.empty())
        broadcast(caf::make_message(atom::erase::value, std::move(key),
                                    next_seq()));
    },
    [=](atom::add, data& key, data& value, count seq) {
      BROKER_DEBUG("add" << ('#' + std::to_string(seq) + ':') << key);
      auto result = self->state.backend->add(key, value);
      if (!result) {
        BROKER_WARNING("failed to add" << value << "to" << key);
        return; // TODO: propagate failure? to all clones? as status msg?
      }
      if (!self->state.clones.empty())
        broadcast(caf::make_message(atom::add::value, std::move(key),
                                    std::move(value), next_seq()));
    },
    [=](atom::remove, data& key, data& value, count seq) {
      BROKER_DEBUG("remove" << ('#' + std::to_string(seq) + ':') << key);
      auto result = self->state.backend->remove(key, value);
      if (!result) {
        BROKER_WARNING("failed to add" << value << "to" << key);
        return; // TODO: propagate failure? to all clones? as status msg?
      }
      if (!self->state.clones.empty())
        broadcast(caf::make_message(atom::remove::value, std::move(key),
                                    std::move(value), next_seq()));
    },
    [=](atom::snapshot, const caf::actor& clone) {
      BROKER_DEBUG("got snapshot request from" << to_string(clone));
      auto ss = self->state.backend->snapshot();
      if (!ss)
        die("failed to snapshot master");
      self->send(clone, std::move(*ss));
      self->monitor(clone);
      self->state.clones.insert(clone->address());
    },
  };
  auto dispatch = caf::message_handler{
    [=](topic& t, message& msg, const caf::actor& source) mutable {
      BROKER_DEBUG("dispatching message with topic" << t << "from core"
                   << to_string(source));
      commands(msg);
    }
  };
  auto expiration = caf::message_handler{
    [=](atom::expire, const data& key, time::duration expiry) {
      BROKER_DEBUG("expiring key" << key << "after" << expiry.count() << "ns");
      // TODO
    }
  };
  auto user = caf::message_handler{
    [=](atom::get, const data& key) -> expected<data> {
      BROKER_DEBUG("GET" << key);
      return self->state.backend->get(key);
    },
    [=](atom::get, const data& key, const data& value) -> expected<data> {
      BROKER_DEBUG("GET" << key << "->" << value);
      return self->state.backend->get(key, value);
    },
    [=](atom::get) {
      return name;
    },
  };
  return dispatch.or_else(expiration).or_else(commands).or_else(user);
}

} // namespace detail
} // namespace broker