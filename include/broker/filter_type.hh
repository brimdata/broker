#pragma once

#include <vector>

#include "broker/topic.hh"

namespace broker {

using filter_type = std::vector<topic>;

/// Extends the filter `f` with `x` such that the filter contains the minimal
/// set of subscriptions. For example, subscribing to `/foo/bar` does not change
/// the filter when it already contains a subscription to `/foo`. Further,
/// subscribing to `/foo` if the filter already contains `/foo/bar` replaces the
/// existing entry with the less specific '/foo'.
/// @return `true` if the filter changed, `false` otherwise.
bool filter_extend(filter_type& f, const topic& x);

/// Convenience function for calling `filter_extend` with each topic in `other`.
bool filter_extend(filter_type& f, const filter_type& other);

} // namespace broker
