#pragma once

#include <algorithm>
#include <memory>
#include <type_traits>
#include <utility>

#include "caf/meta/omittable_if_empty.hpp"
#include "caf/meta/type_name.hpp"

namespace broker::alm {

/// Helper class for giving multipath children an STL-like interface.
template <class Pointer>
class multipath_node_set {
public:
  explicit multipath_node_set(Pointer this_ptr) : this_(this_ptr) {
    //nop
  }

  auto begin() const {
    return this_->nodes_begin();
  }

  auto end() const {
    return this_->nodes_end();
  }

  auto size() const {
    return this_->nodes_count();
  }

  auto empty() const {
    return size() == 0;
  }

  template <class... Ts>
  decltype(auto) emplace(Ts&&... xs) {
    return this_->emplace_node(std::forward<Ts>(xs)...);
  }

private:
  Pointer this_;
};

/// A recoursive data structure for encoding branching paths for source routing.
template <class PeerId>
class multipath {
public:
  using pointer = multipath*;

  using const_pointer = const multipath*;

  using iterator = pointer;

  static constexpr size_t block_size = 16;

  static constexpr bool nothrow_move
    = std::is_nothrow_move_constructible<PeerId>::value;

  static constexpr bool nothrow_assign
    = std::is_nothrow_move_assignable<PeerId>::value;

  struct node_less {
    bool operator()(const multipath& x, const PeerId& y) const noexcept {
      return x.id() < y;
    }

    bool operator()(const PeerId& x, const multipath& y) const noexcept {
      return x < y.id();
    }
  };

  multipath() noexcept(std::is_nothrow_constructible<PeerId>::value) = default;

  explicit multipath(PeerId id) noexcept(nothrow_move) : id_(std::move(id)) {
    // nop
  }

  multipath(multipath&& other) noexcept(nothrow_move)
    : id_(std::move(other.id_)),
      nodes_(nullptr),
      size_(other.size_),
      reserved_(other.reserved_) {
    using std::swap;
    swap(nodes_, other.nodes_);
  }

  multipath& operator=(multipath&& other) noexcept(nothrow_assign) {
    using std::swap;
    swap(id_, other.id_);
    swap(nodes_, other.nodes_);
    size_ = other.size_;
    reserved_ = other.reserved_;
    return *this;
  }

  ~multipath() noexcept {
    delete[] nodes_;
  }

  const auto& id() const noexcept {
    return id_;
  }

  auto nodes() noexcept {
    return multipath_node_set<pointer>{this};
  }

  auto nodes() const noexcept {
    return multipath_node_set<const_pointer>{this};
  }

  auto nodes_begin() noexcept {
    return nodes_;
  }

  auto nodes_end() noexcept {
    return nodes_ + size_;
  }

  auto nodes_begin() const noexcept {
    return nodes_;
  }

  auto nodes_end() const noexcept {
    return nodes_ + size_;
  }

  auto nodes_count() const noexcept {
    return size_;
  }

  std::pair<iterator, bool> emplace_node(PeerId id) {
    node_less pred;
    auto insertion_point = std::lower_bound(nodes_begin(), nodes_end(), id,
                                            pred);
    if (insertion_point == nodes_end()) {
      return {append(std::move(id)), true};
    } else if (insertion_point->id() == id) {
      return {insertion_point, false};
    } else {
      return {insert_at(insertion_point, std::move(id)), true};
    }
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, multipath& x) {
    return f(
      std::forward_as_tuple(x.id_, caf::meta::omittable_if_empty(), x.nodes()));
  }

private:
  iterator append(PeerId&& id) {
    grow_if_needed();
    auto ptr = nodes_ + size_;
    ++size_;
    ptr->id_ = std::move(id);
    return ptr;
  }

  iterator insert_at(iterator pos, PeerId&& id) {
    grow_if_needed();
    auto last = nodes_end();
    std::move_backward(pos, last, last + 1);
    *pos = multipath{std::move(id)};
    ++size_;
    return pos;
  }

  void grow_if_needed() {
    if (size_ == reserved_) {
      auto tmp = std::make_unique<multipath[]>(reserved_ + block_size);
      if constexpr (nothrow_assign)
        std::move(nodes_begin(), nodes_end(), tmp.get());
      else
        std::copy(nodes_begin(), nodes_end(), tmp.get());
      auto ptr = nodes_;
      nodes_ = tmp.release();
      tmp.reset(ptr);
      reserved_ += block_size;
    }
  }

  /// Unique identifier of this node.
  PeerId id_;

  // Unfortunately, `std::vector` is not guaranteed to work with incomplete
  // types, so we have to roll our own dynamic memory management here.

  /// Stores the childen of this node, if any.
  multipath* nodes_ = nullptr;

  /// Current size of `nodes_`.
  size_t size_ = 0;

  /// Reserved capacity for `nodes_`.
  size_t reserved_ = 0;
};

} // namespace broker::alm
