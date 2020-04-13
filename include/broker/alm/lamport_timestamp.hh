#pragma once

#include <cstdint>

namespace broker::alm {

/// A logical clock using a 64-bit counter.
struct lamport_timestamp {
  uint64_t value = 1;

  lamport_timestamp& operator++() {
    ++value;
    return *this;
  }
};

/// @relates lamport_timestamp
constexpr bool operator<(lamport_timestamp x, lamport_timestamp y) {
  return x.value < y.value;
}

/// @relates lamport_timestamp
constexpr bool operator<=(lamport_timestamp x, lamport_timestamp y) {
  return x.value <= y.value;
}

/// @relates lamport_timestamp
constexpr bool operator>(lamport_timestamp x, lamport_timestamp y) {
  return x.value > y.value;
}

/// @relates lamport_timestamp
constexpr bool operator>=(lamport_timestamp x, lamport_timestamp y) {
  return x.value >= y.value;
}

/// @relates lamport_timestamp
constexpr bool operator==(lamport_timestamp x, lamport_timestamp y) {
  return x.value == y.value;
}

/// @relates lamport_timestamp
constexpr bool operator!=(lamport_timestamp x, lamport_timestamp y) {
  return x.value != y.value;
}

/// @relates lamport_timestamp
template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, lamport_timestamp& x) {
  return f(x.value);
}

} // namespace broker::alm
