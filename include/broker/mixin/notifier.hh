#pragma once

#include <caf/group.hpp>

#include "broker/atoms.hh"
#include "broker/detail/assert.hh"
#include "broker/endpoint_info.hh"
#include "broker/error.hh"
#include "broker/logger.hh"
#include "broker/status.hh"

namespace broker::mixin {

template <class Base, class Subtype>
class notifier : public Base {
public:
  using extended_base = notifier;

  using super = Base;

  using peer_id_type = typename super::peer_id_type;

  using communication_handle_type = typename Base::communication_handle_type;

  template <class... Ts>
  explicit notifier(Ts&&... xs) : super(std::forward<Ts>(xs)...) {
    auto& groups = super::self()->system().groups();
    errors_ = groups.get_local("broker/errors");
    statuses_ = groups.get_local("broker/statuses");
  }

  void peer_connected(const peer_id_type& remote_id,
                      const communication_handle_type& hdl) {
    emit(remote_id, sc::peer_added, "handshake successful");
    super::peer_connected(remote_id, hdl);
  }

  void peer_disconnected(const peer_id_type& remote_id,
                         const communication_handle_type& hdl,
                         const error& reason) {
    emit(remote_id, sc::peer_lost, "lost connection to remote peer");
    super::peer_disconnected(remote_id, hdl, reason);
  }

private:
  auto& dref() {
    return *static_cast<Subtype*>(this);
  }

  /// Reports an error to all status subscribers.
  template <class Enum>
  void emit(const communication_handle_type& hdl, Enum code, const char* msg) {
    auto self = super::self();
    auto emit = [=](network_info x) {
      BROKER_INFO("emit:" << code << x);
      if constexpr (std::is_same<Enum, sc>::value)
        self->send(statuses_, atom::local::value,
                   status::make(code, endpoint_info{hdl.node(), std::move(x)},
                                msg));
      else
        self->send(errors_, atom::local::value,
                   make_error(code, endpoint_info{hdl.node(), std::move(x)},
                              msg));
    };
    if (self->node() != hdl.node()) {
      auto on_cache_hit = [=](network_info x) { emit(std::move(x)); };
      auto on_cache_miss = [=](caf::error) { emit({}); };
      dref().cache.fetch(hdl, on_cache_hit, on_cache_miss);
    } else {
      emit({});
    }
  }

  template <class Enum>
  void emit(const peer_id_type& remote_id, Enum code, const char* msg) {
    auto& tbl = dref().tbl();
    auto i = tbl.find(remote_id);
    if (i != tbl.end()) {
      emit(i->second.hdl, code, msg);
    } else {
      BROKER_ERROR("no entry found in routing table for" << remote_id);
    }
  }

  /// Caches the CAF group for error messages.
  caf::group errors_;

  /// Caches the CAF group for status messages.
  caf::group statuses_;
};

} // namespace broker::mixin
