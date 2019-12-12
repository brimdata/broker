#pragma once

#include <type_traits>

#include "broker/none.hh"

namespace broker::detail {

/// Lifts a member function pointer to a message handler, prefixed with
/// `AtomPrefix`.
template <class AtomPrefix, class T, class U, class R, class... Ts>
auto lift(T& obj, R (U::*fun)(Ts...)) {
  if constexpr (std::is_same<AtomPrefix, none>::value)
    return [&obj, fun](Ts... xs) { return (obj.*fun)(xs...); };
  else
    return [&obj, fun](AtomPrefix, Ts... xs) { return (obj.*fun)(xs...); };
}

} // namespace broker::detail
