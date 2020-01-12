#include "broker/configuration.hh"

#include <cstdlib>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include <caf/atom.hpp>
#include <caf/io/middleman.hpp>
#include <caf/openssl/manager.hpp>

#include "broker/address.hh"
#include "broker/data.hh"
#include "broker/endpoint.hh"
#include "broker/internal_command.hh"
#include "broker/port.hh"
#include "broker/snapshot.hh"
#include "broker/status.hh"
#include "broker/store.hh"
#include "broker/subnet.hh"
#include "broker/time.hh"
#include "broker/topic.hh"
#include "broker/version.hh"

namespace broker {

configuration::configuration(broker_options opts) : options_(std::move(opts)) {
  // Add runtime type information for Broker types.
  add_message_types(*this);
  // Load CAF modules.
  load<caf::io::middleman>();
  if (not options_.disable_ssl)
    load<caf::openssl::manager>();
  // Ensure that we're only talking to compatible Broker instances.
  set("middleman.app-identifier",
      "broker.v" + std::to_string(version::protocol));
  // Add custom options to the CAF parser.
  opt_group{custom_options_, "?broker"}
    .add(options_.disable_ssl, "disable_ssl",
         "forces Broker to use unencrypted communication")
    .add(options_.ttl, "ttl", "drop messages after traversing TTL hops")
    .add<std::string>("recording-directory",
                      "path for storing recorded meta information")
    .add<size_t>("output-generator-file-cap",
                 "maximum number of entries when recording published messages");
  // Override CAF default file names.
  set("logger.file-name", "broker_[PID]_[TIMESTAMP].log");
  set("logger.file-verbosity", caf::atom("quiet"));
  set("logger.console-verbosity", caf::atom("quiet"));
  // Check for supported environment variables.
  if (auto env = getenv("BROKER_DEBUG_VERBOSE")) {
    if (*env && *env != '0') {
      set("logger.file-verbosity", caf::atom("DEBUG"));
      set("logger.console-verbosity", caf::atom("DEBUG"));
      set("logger.component-filter", "");
    }
  }
  if (auto env = getenv("BROKER_DEBUG_LEVEL")) {
    char level[10];
    strncpy(level, env, sizeof(level));
    level[sizeof(level) - 1] = '\0';
    set("logger.file-verbosity", caf::atom(level));
    set("logger.console-verbosity", caf::atom(level));
  }
  if (auto env = getenv("BROKER_DEBUG_COMPONENT_FILTER"))
    set("logger.component-filter", env);
  if (auto env = getenv("BROKER_RECORDING_DIRECTORY"))
    set("broker.recording-directory", env);
  if (auto env = getenv("BROKER_OUTPUT_GENERATOR_FILE_CAP")) {
    try {
      auto value = static_cast<size_t>(std::stoi(env));
      if (value < 0)
        throw std::runtime_error("expected a positive number");
      set("broker.output-generator-file-cap", static_cast<size_t>(value));
    } catch (...) {
      std::cerr << "*** invalid value for BROKER_OUTPUT_GENERATOR_FILE_CAP: "
                << env << " (expected a positive number)";
    }
  }
}

configuration::configuration(int argc, char** argv) : configuration{} {
  parse(argc, argv);
}

caf::settings configuration::dump_content() const {
  auto result = super::dump_content();
  auto& grp = result["broker"].as_dictionary();
  put_missing(grp, "disable_ssl", options_.disable_ssl);
  put_missing(grp, "ttl", options_.ttl);
  put_missing(grp, "forward", options_.forward);
  if (auto path = get_if<std::string>(&content, "broker.recording-directory"))
    put_missing(grp, "recording-directory", *path);
  if (auto cap = get_if<size_t>(&content, "broker.output-generator-file-cap"))
    put_missing(grp, "output-generator-file-cap", *cap);
  return result;
}

#define ADD_MSG_TYPE(name) cfg.add_message_type<name>(#name)

void configuration::add_message_types(caf::actor_system_config& cfg) {
  ADD_MSG_TYPE(broker::data);
  ADD_MSG_TYPE(broker::address);
  ADD_MSG_TYPE(broker::subnet);
  ADD_MSG_TYPE(broker::port);
  ADD_MSG_TYPE(broker::timespan);
  ADD_MSG_TYPE(broker::timestamp);
  ADD_MSG_TYPE(broker::enum_value);
  ADD_MSG_TYPE(broker::vector);
  ADD_MSG_TYPE(broker::set);
  ADD_MSG_TYPE(broker::status);
  ADD_MSG_TYPE(broker::table);
  ADD_MSG_TYPE(broker::topic);
  ADD_MSG_TYPE(broker::optional<broker::timestamp>);
  ADD_MSG_TYPE(broker::optional<broker::timespan>);
  ADD_MSG_TYPE(broker::snapshot);
  ADD_MSG_TYPE(broker::internal_command);
  ADD_MSG_TYPE(broker::command_message);
  ADD_MSG_TYPE(broker::data_message);
  ADD_MSG_TYPE(broker::node_message);
  ADD_MSG_TYPE(broker::node_message_content);
  ADD_MSG_TYPE(broker::set_command);
  ADD_MSG_TYPE(broker::store::stream_type::value_type);
  cfg.add_message_type<std::vector<caf::node_id>>("peer_list");
}

#undef ADD_MSG_TYPE

} // namespace broker
