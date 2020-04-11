#pragma once

#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <set>
#include <utility>

#include "broker/optional.hh"

namespace broker::alm {

/// Stores a linear path to another peer with a logical timestamp for when this
/// route was announced.
template <class PeerId>
class routing_path {
public:
  using value_type = PeerId;

  using container_type = std::vector<PeerId>;

  using iterator = typename container_type::iterator;

  using const_iterator = typename container_type::const_iterator;

  routing_path(uint64_t timestamp, container_type content)
    : timestamp_(timestamp), content_(std::move(content)) {
    // nop
  }

  template <class Iterator>
  routing_path(uint64_t timestamp, Iterator first, Iterator last)
    : timestamp_(timestamp), content_(first, last) {
    // nop
  }

  routing_path() noexcept(
    std::is_nothrow_default_constructible<container_type>::value)
    = default;

  routing_path(routing_path&&) noexcept(
    std::is_nothrow_move_constructible<container_type>::value)
    = default;

  routing_path(const routing_path&) = default;

  routing_path& operator=(routing_path&&) noexcept(
    std::is_nothrow_move_assignable<container_type>::value)
    = default;

  routing_path& operator=(const routing_path&) = default;

  // -- properties -------------------------------------------------------------

  auto empty() const noexcept {
    return content_.empty();
  }

  auto size() const noexcept {
    return content_.size();
  }

  const auto& container() const noexcept {
    return content_;
  }

  const auto& timestamp() const noexcept {
    return timestamp_;
  }

  // -- element access ---------------------------------------------------------

  auto& operator[](size_t index) noexcept {
    return content_[index];
  }

  const auto& operator[](size_t index) const noexcept {
    return content_[index];
  }

  auto& at(size_t index) {
    return content_.at(index);
  }

  const auto& at(size_t index) const {
    return content_.at(index);
  }

  auto* data() noexcept {
    return content_.data();
  }

  const auto* data() const noexcept {
    return content_.data();
  }

  // -- iterator interface -----------------------------------------------------

  auto begin() noexcept {
    return content_.begin();
  }

  auto begin() const noexcept {
    return content_.begin();
  }

  auto end() noexcept {
    return content_.end();
  }

  auto end() const noexcept {
    return content_.end();
  }

  // -- modifiers --------------------------------------------------------------

  void swap(routing_path&other) noexcept {
    using std::swap;
    swap(content_, other.content_);
    swap(timestamp_, other.timestamp_);
  }

  template <class... Ts>
  auto insert(Ts&&... xs) {
    return content_.insert(std::forward<Ts>(xs)...);
  }

private:
  uint64_t timestamp_ = 0;

  container_type content_;
};

/// Returns `true` if X is shorter than Y or both paths have equal length but
/// X comes before Y lexicographically, `false` otherwise.
template <class PeerId>
bool operator<(const routing_path<PeerId>& x,
               const routing_path<PeerId>& y) noexcept {
  if (x.size() < y.size())
    return true;
  if (x.size() == y.size())
    return x.container() < y.container();
  return false;
}

/// Returns whether `x` and `y` have the same content. Ignores timestamps for
/// the comparison.
template <class PeerId>
bool operator==(const routing_path<PeerId>& x,
                const routing_path<PeerId>& y) noexcept {
  return x.container() == y.container();
}

template <class PeerId>
bool operator!=(const routing_path<PeerId>& x,
                const routing_path<PeerId>& y) noexcept {
  return !(x == y);
}

/// Stores paths to all peers. For direct connection, also stores a
/// communication handle for reaching the peer.
template <class PeerId, class CommunicationHandle>
class routing_table_row {
public:
  using peer_id_type = PeerId;

  using path_type = routing_path<peer_id_type>;

  using communication_handle_type = CommunicationHandle;

  /// Stores an implementation-specific handle for talking to the peer. The
  /// handle is null if no direct connection exists.
  CommunicationHandle hdl;

  /// Stores paths leading to this peer, sorted by path length.
  std::set<path_type> paths;

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
template <class Id, class Handle>
const routing_path<Id>*
shortest_path(const routing_table<Id, Handle>& tbl,
              const typename routing_table<Id, Handle>::key_type& peer) {
  if (auto i = tbl.find(peer); i != tbl.end() && !i->second.paths.empty())
    return std::addressof(*i->second.paths.begin());
  return nullptr;
}

/// Checks whether the routing table `tbl` contains a path to the `peer`.
template <class Id, class Handle>
bool reachable(const routing_table<Id, Handle>& tbl,
               const typename routing_table<Id, Handle>::key_type& peer) {
  return tbl.count(peer) != 0;
}

/// Returns the hop count on the shortest path or `nil` if no route to the peer
/// exists.
template <class Id, class Handle>
optional<size_t>
distance_to(const routing_table<Id, Handle>& tbl,
            const typename routing_table<Id, Handle>::key_type& peer) {
  if (auto ptr = shortest_path(tbl, peer))
    return ptr->size();
  return nil;
}

/// Erases connection state for a direct connection. Routing paths to the peer
/// may still remain on the table if the peer is reachable through others.
/// @returns `true` if a direct connection was removed, `false` otherwise.
template <class Id, class Handle>
bool erase_direct(routing_table<Id, Handle>& tbl,
                  const typename routing_table<Id, Handle>::key_type& peer) {
  if (auto i = tbl.find(peer); i != tbl.end()) {
    auto& row = i->second;
    if constexpr (std::is_integral<Handle>::value)
      row.hdl = 0;
    else
      row.hdl = nullptr;
    for (auto i = row.paths.begin(); i != row.paths.end();) {
      auto& path = *i;
      if (path[0] == peer)
        i = row.paths.erase(i);
      else
        ++i;
    }
    if (row.paths.empty())
      tbl.erase(peer);
    return true;
  }
  return false;
}

/// Erases all state for the peer.
template <class Id, class Handle>
void erase(routing_table<Id, Handle>& tbl,
           const typename routing_table<Id, Handle>::key_type& peer) {
  auto stale = [&](const auto& path) {
    return std::find(path.begin(), path.end(), peer) != path.end();
  };
  tbl.erase(peer);
  for (auto& kvp : tbl) {
    auto& paths = kvp.second.paths;
    for (auto i = paths.begin(); i != paths.end();) {
      if (stale(*i))
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
template <class Id, class Handle>
void add_path(routing_table<Id, Handle>& tbl,
              const typename routing_table<Id, Handle>::key_type& peer,
              uint64_t timestamp, std::vector<Id> path) {
  tbl[peer].paths.emplace(timestamp, std::move(path));
}

} // namespace broker::alm
