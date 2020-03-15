#pragma once

#include <utility>
#include <vector>

#include <caf/cow_tuple.hpp>
#include <caf/message.hpp>

#include "broker/filter_type.hh"
#include "broker/topic.hh"

namespace broker::detail {

struct prefix_matcher {
  bool operator()(const filter_type& filter, const topic& t) const;

  template <class T>
  bool operator()(const filter_type& filter, const T& x) const {
    return (*this)(filter, get_topic(x));
  }
};

} // namespace broker::detail
