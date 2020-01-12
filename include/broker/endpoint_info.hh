#pragma once

#include <caf/meta/type_name.hpp>
#include <caf/node_id.hpp>

#include "broker/fwd.hh"
#include "broker/network_info.hh"
#include "broker/optional.hh"

namespace broker {

using caf::node_id;

/// Information about an endpoint.
/// @relates endpoint
struct endpoint_info {
  node_id node;                   ///< A unique context ID per machine/process.
  optional<network_info> network; ///< Optional network-level information.
};

/// @relates endpoint_info
inline bool operator==(const endpoint_info& x, const endpoint_info& y) {
  return x.node == y.node && x.network == y.network;
}

/// @relates endpoint_info
inline bool operator!=(const endpoint_info& x, const endpoint_info& y) {
  return !(x == y);
}

/// @relates endpoint_info
template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, endpoint_info& info) {
  return f(caf::meta::type_name("endpoint_info"), info.node, info.network);
}

} // namespace broker
