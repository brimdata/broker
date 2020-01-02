#pragma once

#include <caf/allowed_unsafe_message_type.hpp>
#include <caf/response_promise.hpp>

#include "broker/atoms.hh"
#include "broker/detail/lift.hh"
#include "broker/detail/network_cache.hh"
#include "broker/error.hh"

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(caf::response_promise)

namespace broker::mixin {

/// Adds these handlers:
/// (atom::peer, network_info)
template <class Base, class Subtype>
class connector : public Base {
public:
  using extended_base = connector;

  using super = Base;

  using peer_id_type = typename super::peer_id_type;

  using communication_handle_type = typename Base::communication_handle_type;

  template <class... Ts>
  explicit connector(Ts&&... xs)
    : super(std::forward<Ts>(xs)...), cache_(super::self()) {
    // nop
  }

  void try_peering(const network_info& addr, caf::response_promise rp,
                   uint32_t count) {
    auto self = super::self();
    auto deliver_err = [=](error& err) mutable { rp.deliver(std::move(err)); };
    // Fetch the comm. handle from the cache and with that fetch the ID from the
    // remote peer via direct request messages.
    cache_.fetch(
      addr,
      [=](communication_handle_type hdl) {
        // TODO: replace infinite with some useful default / config parameter
        self->request(hdl, caf::infinite, atom::get::value, atom::id::value)
          .then(
            [=](const peer_id_type& remote_id) mutable {
              dref().start_peering(remote_id, hdl, std::move(rp));
            },
            [=](error& err) mutable { rp.deliver(std::move(err)); });
      },
      [=](error err) mutable {
        if (addr.retry.count() == 0 && ++count < 10) {
          rp.deliver(std::move(err));
        } else {
          self->delayed_send(self, addr.retry, atom::peer::value,
                             atom::retry::value, addr, std::move(rp), count);
        }
      });
  }

  template <class... Fs>
  caf::behavior make_behavior(Fs... fs) {
    using detail::lift;
    auto& d = dref();
    return super::make_behavior(
      std::move(fs)...,
      [=](atom::peer, const network_info& addr) {
        dref().try_peering(addr, super::self()->make_response_promise(), 0);
      },
      [=](atom::peer, atom::retry, const network_info& addr,
          caf::response_promise& rp,
          uint32_t count) { dref().try_peering(addr, std::move(rp), count); },
      lift<atom::peer>(d, &Subtype::try_peering));
  }

  auto& cache() {
    return cache_;
  }

private:
  Subtype& dref() {
    return static_cast<Subtype&>(*this);
  }

  /// Associates network addresses to remote actor handles and vice versa.
  detail::network_cache cache_;
};

} // namespace broker::mixin
