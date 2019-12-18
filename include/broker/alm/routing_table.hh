#pragma once

#include <algorithm>
#include <cstddef>
#include <map>

#include "broker/optional.hh"

namespace broker::alm {

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

template <class Inspector, class PeerId, class CommunicationHandle>
auto inspect(Inspector& f, routing_table_row<PeerId, CommunicationHandle>& x) {
  return f(caf::meta::type_name("row"), x.hdl, x.distances);
}

/// Stores direct connections to peers as well as distances to all other peers
/// that we can reach indirectly.
template <class PeerId, class CommunicationHandle>
using routing_table
  = std::map<PeerId, routing_table_row<PeerId, CommunicationHandle>>;

template <class PeerId, class CommunicationHandle>
optional<PeerId>
get_peer_id(const routing_table<PeerId, CommunicationHandle>& tbl,
            const CommunicationHandle& hdl) {
  auto predicate = [&](const auto& kvp) { return kvp.second.hdl == hdl; };
  auto e = tbl.end();
  auto i = std::find_if(tbl.begin(), e, predicate);
  if (i != e)
    return i->first;
  return nil;
}

} // namespace broker::alm
