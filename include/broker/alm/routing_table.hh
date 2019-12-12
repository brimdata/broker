#pragma once

#include <cstddef>
#include <map>

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

} // namespace broker::alm
