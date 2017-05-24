#ifndef BROKER_SUBSCRIBER_BASE_HH
#define BROKER_SUBSCRIBER_BASE_HH

#include <vector>

#include <caf/actor.hpp>
#include <caf/duration.hpp>
#include <caf/intrusive_ptr.hpp>
#include <caf/ref_counted.hpp>

#include "broker/data.hh"
#include "broker/fwd.hh"
#include "broker/topic.hh"

#include "broker/detail/shared_subscriber_queue.hh"

namespace broker {

/// Provides blocking access to a stream of data.
template <class ValueType>
class subscriber_base {
public:
  // --- nested types ----------------------------------------------------------
  
  using value_type = ValueType;

  using queue_type = detail::shared_subscriber_queue<value_type>;

  using queue_ptr = detail::shared_subscriber_queue_ptr<value_type>;

  // --- constructors and destructors ------------------------------------------

  subscriber_base()
    : queue_(detail::make_shared_subscriber_queue<value_type>()) {
    // nop
  }

  subscriber_base(subscriber_base&&) = default;

  subscriber_base& operator=(subscriber_base&&) = default;

  virtual ~subscriber_base() {
    // nop
  }

  subscriber_base(const subscriber_base&) = delete;

  subscriber_base& operator=(const subscriber_base&) = delete;

  // --- access to values ------------------------------------------------------

  /// Pulls a single value out of the stream. Blocks the current thread until
  /// at least one value becomes available.
  value_type get() {
    auto tmp = get(1);
    BROKER_ASSERT(tmp.size() == 1);
    return std::move(tmp.front());
  }

  /// Pulls a single value out of the stream. Blocks the current thread until
  /// at least one value becomes available or a timeout occurred.
  caf::optional<value_type> get(caf::duration timeout) {
    auto tmp = get(1, timeout);
    if (tmp.size() == 1)
      return std::move(tmp.front());
    return caf::none;
  }

  /// Pulls `num` values out of the stream. Blocks the current thread until
  /// `num` elements are available or a timeout occurs. Returns a partially
  /// filled or empty vector on timeout, otherwise a vector containing exactly
  /// `num` elements.
  std::vector<value_type> get(size_t num,
                              caf::duration timeout = caf::infinite) {
    std::vector<value_type> result;
    if (num == 0)
      return result;
    auto t0 = std::chrono::high_resolution_clock::now();
    t0 += timeout;
    for (;;) {
      if (!timeout.valid())
        queue_->wait_on_flare();
      else if (!queue_->wait_on_flare_abs(t0))
        return result;
      queue_->consume(num - result.size(), [&](value_type&& x) {
        result.emplace_back(std::move(x));
      });
      if (result.size() == num)
        return result;
    }
  }
  
  /// Returns all currently available values without blocking.
  std::vector<value_type> poll() {
    std::vector<value_type> result;
    queue_->consume(std::numeric_limits<size_t>::max(),
                    [&](value_type&& x) { result.emplace_back(std::move(x)); });
    return result;
  }
  
  // --- accessors -------------------------------------------------------------

  /// Returns the amound of values than can be extracted immediately without
  /// blocking.
  size_t available() const {
    return queue_->buffer_size();
  }

  /// Returns a file handle for integrating this publisher into a `select` or
  /// `poll` loop.
  int fd() const {
    return queue_->fd();
  }

protected:
  queue_ptr queue_;
};

} // namespace broker

#endif // BROKER_SUBSCRIBER_BASE_HH
