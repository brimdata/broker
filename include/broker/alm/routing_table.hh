#pragma once

#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <set>
#include <tuple>
#include <utility>

#include "broker/alm/lamport_timestamp.hh"
#include "broker/detail/algorithms.hh"
#include "broker/detail/assert.hh"
#include "broker/detail/iterator_range.hh"
#include "broker/detail/map_index_iterator.hh"
#include "broker/optional.hh"

namespace broker::alm {

/// Compares two paths by size, falling back to lexicographical comparison on
/// equal sizes.
template <class PeerId>
struct path_less {
  using path_type = std::vector<PeerId>;

  /// Returns `true` if X is shorter than Y or both paths have equal length but
  /// X comes before Y lexicographically, `false` otherwise.
  bool operator()(const path_type& x, const path_type& y) const noexcept {
    if (x.size() < y.size())
      return true;
    if (x.size() == y.size())
      return x < y;
    return false;
  }
};

/// Stores paths to all peers. For direct connection, also stores a
/// communication handle for reaching the peer.
template <class PeerId, class CommunicationHandle>
class routing_table_row {
public:
  using peer_id_type = PeerId;

  using communication_handle_type = CommunicationHandle;

  /// Stores a linear path to another peer with logical timestamps for when this
  /// route was announced.
  using path_type = std::vector<peer_id_type>;

  /// Stores an implementation-specific handle for talking to the peer. The
  /// handle is null if no direct connection exists.
  CommunicationHandle hdl;

  /// Stores all paths leading to this peer, using a vector timestamp for
  /// versioning (stores only the latest version). Sorted by path length.
  std::map<path_type, vector_timestamp, path_less<PeerId>> versioned_paths;

  /// Returns the sorted paths without the associated vector timestamp.
  auto paths() const {
    using namespace detail;
    return make_iterator_range(map_first(versioned_paths.begin()),
                               map_first(versioned_paths.end()));
  }

  routing_table_row() = default;

  explicit routing_table_row(CommunicationHandle hdl) : hdl(std::move(hdl)) {
    // nop
  }
};

template <class Inspector, class PeerId, class CommunicationHandle>
auto inspect(Inspector& f, routing_table_row<PeerId, CommunicationHandle>& x) {
  return f(caf::meta::type_name("row"), x.hdl, x.distances, x.paths);
}

/// Stores direct connections to peers as well as distances to all other peers
/// that we can reach indirectly.
template <class Id, class Handle>
using routing_table = std::map<Id, routing_table_row<Id, Handle>>;

/// Returns the ID  of the peer if `hdl` is a direct connection, `nil`
/// otherwise.
template <class Id, class Handle>
optional<Id>
get_peer_id(const routing_table<Id, Handle>& tbl, const Handle& hdl) {
  auto predicate = [&](const auto& kvp) { return kvp.second.hdl == hdl; };
  auto e = tbl.end();
  auto i = std::find_if(tbl.begin(), e, predicate);
  if (i != e)
    return i->first;
  return nil;
}

/// Returns all hops to the destination (including `dst` itself) or
/// `nullptr` if the destination is unreachable.
template <class RoutingTable>
const std::vector<typename RoutingTable::key_type>*
shortest_path(const RoutingTable& tbl,
              const typename RoutingTable::key_type& peer) {
  if (auto i = tbl.find(peer);
      i != tbl.end() && !i->second.versioned_paths.empty())
    return std::addressof(i->second.versioned_paths.begin()->first);
  return nullptr;
}

/// Checks whether the routing table `tbl` contains a path to the `peer`.
template <class RoutingTable>
bool reachable(const RoutingTable& tbl,
               const typename RoutingTable::key_type& peer) {
  return tbl.count(peer) != 0;
}

/// Returns the hop count on the shortest path or `nil` if no route to the peer
/// exists.
template <class RoutingTable>
optional<size_t> distance_to(const RoutingTable& tbl,
                             const typename RoutingTable::key_type& peer) {
  if (auto ptr = shortest_path(tbl, peer))
    return ptr->size();
  return nil;
}

/// Erases connection state for a direct connection. Routing paths to the peer
/// may still remain on the table if the peer is reachable through others.
/// @returns `true` if a direct connection was removed, `false` otherwise.
template <class RoutingTable>
bool erase_direct(RoutingTable& tbl,
                  const typename RoutingTable::key_type& peer) {
  using mapped_type = typename RoutingTable::mapped_type;
  using handle_type = typename mapped_type::communication_handle_type;
  if (auto i = tbl.find(peer); i != tbl.end()) {
    auto& row = i->second;
    if constexpr (std::is_integral<handle_type>::value)
      row.hdl = 0;
    else
      row.hdl = nullptr;
    for (auto i = row.versioned_paths.begin(); i != row.versioned_paths.end();) {
      auto& path = i->first;
      if (path[0] == peer)
        i = row.versioned_paths.erase(i);
      else
        ++i;
    }
    if (row.versioned_paths.empty())
      tbl.erase(peer);
    return true;
  }
  return false;
}

/// Erases all state for the peer.
template <class RoutingTable>
void erase(RoutingTable& tbl, const typename RoutingTable::key_type& peer) {
  auto stale = [&](const auto& path) {
    return std::find(path.begin(), path.end(), peer) != path.end();
  };
  tbl.erase(peer);
  for (auto& kvp : tbl) {
    auto& paths = kvp.second.versioned_paths;
    for (auto i = paths.begin(); i != paths.end();) {
      if (stale(i->first))
        i = paths.erase(i);
      else
        ++i;
    }
  }
}

template <class Id, class Handle, class F>
void for_each_direct(const routing_table<Id, Handle>& tbl, F fun) {
  for (auto& [peer, row] : tbl)
    if (row.hdl)
      fun(peer, row.hdl);
}

/// Returns a pointer to the row of the remote peer if it exists, `nullptr`
/// otherwise.
template <class Id, class Handle>
auto* find_row(const routing_table<Id, Handle>& tbl,
               const typename routing_table<Id, Handle>::key_type& peer) {
  using pointer = const typename routing_table<Id, Handle>::mapped_type*;
  if (auto i = tbl.find(peer); i != tbl.end())
    return std::addressof(i->second);
  return static_cast<pointer>(nullptr);
}

/// @copydoc find_row.
template <class Id, class Handle>
auto* find_row(routing_table<Id, Handle>& tbl,
               const typename routing_table<Id, Handle>::key_type& peer) {
  using pointer = typename routing_table<Id, Handle>::mapped_type*;
  if (auto i = tbl.find(peer); i != tbl.end())
    return std::addressof(i->second);
  return static_cast<pointer>(nullptr);
}

/// Adds a path to the peer, inserting a new row for the peer is it does not
/// exist yet.
/// @return `true` if a new entry was added to `tbl`, `false` otherwise.
template <class RoutingTable>
bool add_or_update_path(RoutingTable& tbl,
                        const typename RoutingTable::key_type& peer,
                        std::vector<typename RoutingTable::key_type> path,
                        vector_timestamp ts) {
  auto& versioned_paths = tbl[peer].versioned_paths;
  if (auto i = versioned_paths.find(path); i != versioned_paths.end()) {
    if (i->second < ts)
      i->second = std::move(ts);
    return false;
  }
  versioned_paths.emplace(std::move(path), std::move(ts));
  return true;
}

/// A 3-tuple for storing a revoked path between two peers with the logical time
/// when the connection was severed.
template <class PeerId>
struct blacklist_entry {
  /// The source of the event.
  PeerId revoker;

  /// Time of the connection loss, as seen by `revoker`.
  lamport_timestamp ts;

  /// The disconnected hop.
  PeerId hop;
};

/// @relates blacklist_entry
template <class PeerId>
bool operator<(const blacklist_entry<PeerId>& x,
               const blacklist_entry<PeerId>& y) noexcept {
  return std::tie(x.revoker, x.ts, x.hop) < std::tie(y.revoker, y.ts, y.hop);
}

/// A list for storing path revocations.
template <class PeerId>
using blacklist = std::vector<blacklist_entry<PeerId>>;

/// Checks whether `path` routes through either `revoker -> hop` or
/// `hop -> revoker` with a timestamp <= `revoke_time`.
template <class PeerId>
bool blacklisted(const std::vector<PeerId>& path,
                 const vector_timestamp& path_ts, const PeerId& revoker,
                 lamport_timestamp ts, const PeerId& hop) {
  BROKER_ASSERT(path.size() == path_ts.size());
  // Short-circuit trivial cases.
  if (path.size() <= 1)
    return false;
  // Scan for the revoker anywhere in the path and see if it's next to the
  // revoked hop.
  if (path.front() == revoker)
    return path_ts.front() <= ts && path[1] == hop;
  for (size_t index = 1; index < path.size() - 1; ++index)
    if (path[index] == revoker)
      return path_ts[index] <= ts
             && (path[index - 1] == hop || path[index + 1] == hop);
  if (path.back() == revoker)
    return path_ts.back() <= ts && path[path.size() - 2] == hop;
  return false;
}

/// @copydoc blacklisted
template <class PeerId>
bool blacklisted(const std::vector<PeerId>& path, const vector_timestamp& ts,
                 const blacklist_entry<PeerId>& entry) {
  return blacklisted(path, ts, entry.revoker, entry.timestamp,
                     entry.revoked_hop);
}

/// Removes all entries form `tbl` where `blacklisted` returns true for given
/// arguments.
template <class PeerId, class Handle, class OnRemovePeer>
void revoke(routing_table<PeerId, Handle>& tbl, const PeerId& revoker,
            lamport_timestamp revoke_time, const PeerId& hop,
            OnRemovePeer callback) {
  auto i = tbl.begin();
  while (i != tbl.end()) {
    detail::erase_if(i->second.versioned_paths, [&](auto& kvp) {
      return blacklisted(kvp.first, kvp.second, revoker, revoke_time, hop);
    });
    if (i->second.versioned_paths.empty()) {
      callback(i->first);
      i = tbl.erase(i);
    } else {
      ++i;
    }
  }
}

/// @copydoc revoke
template <class PeerId, class Handle, class OnRemovePeer>
void revoke(routing_table<PeerId, Handle>& tbl,
            const blacklist_entry<PeerId>& entry, OnRemovePeer callback) {
  return revoke(tbl.entry.revoker, entry.ts, entry.hop, callback);
}

} // namespace broker::alm
