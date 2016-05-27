#ifndef BROKER_CONFIGURATION_HH
#define BROKER_CONFIGURATION_HH

#include <caf/actor_system_config.hpp>

namespace broker {

/// Provides an execution context for brokers.
class configuration : public caf::actor_system_config {
public:
  /// Default-constructs a configuration.
  configuration() = default;

  /// Constructs a configuration from the command line.
  configuration(int argc, char** argv);

private:
  void init() override;
};

} // namespace broker

#endif // BROKER_CONFIGURATION_HH