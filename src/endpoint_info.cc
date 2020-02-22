#include "broker/endpoint_info.hh"

#include <regex>

#include <caf/expected.hpp>
#include <caf/node_id.hpp>
#include <caf/uri.hpp>

#include "broker/data.hh"

namespace broker {

// TODO: CAF currently has no from_string/convert function for converting a
//       `to_string(node_id)` output back. Contribute this back to CAF (but
//       without the heavy-handed regex-based conversion).
bool tmp_convert(const std::string& src, node_id& dst) {
  std::regex default_node_format{"([a-zA-Z0-9]+)#([0-9]+)"};
  std::smatch match;
  if (std::regex_match(src, match, default_node_format)) {
    try {
      auto hid = match[1].str();
      auto pid = std::stoi(match[2].str());
      if (pid < 0)
        return false;
      if (auto id = caf::make_node_id(static_cast<uint32_t>(pid), hid)) {
        dst = std::move(*id);
        return true;
      }
    } catch (...) {
      // nop
    }
  } else if (auto uri_id = caf::make_uri(src)) {
    dst = caf::make_node_id(std::move(*uri_id));
    return true;
  }
  return false;
}

bool convert(const data& src, endpoint_info& dst) {
  if (!is<vector>(src))
    return false;
  // Types: <string, string, port, count>. Fields 1 can be none.
  // Field 2 -4 are either all none or all defined.
  auto& xs = get<vector>(src);
  if (xs.size() != 4)
    return false;
  // Parse the node (field 1).
  if (auto str = get_if<std::string>(xs[0])) {
    if (!tmp_convert(*str, dst.node))
      return false;
  } else if (is<none>(xs[0])) {
    dst.node = caf::node_id{};
  } else {
    // Type mismatch.
    return false;
  }
  // Parse the network (fields 2 - 4).
  if (is<none>(xs[1]) && is<none>(xs[2]) && is<none>(xs[3])) {
    dst.network = nil;
  } else if (is<std::string>(xs[1]) && is<port>(xs[2]) && is<count>(xs[3])) {
    dst.network = network_info{};
    auto& net = *dst.network;
    net.address = get<std::string>(xs[1]);
    net.port = get<port>(xs[2]).number();
    net.retry = timeout::seconds{get<count>(xs[3])};
  } else {
    // Type mismatch.
    return false;
  }
  return true;
}

bool convert(const endpoint_info& src, data& dst) {
  vector result;
  result.resize(4);
  if (src.node)
    result[0] = to_string(src.node);
  if (src.network) {
    result[1] = src.network->address;
    result[2] = port{src.network->port, port::protocol::tcp};
    result[3] = static_cast<count>(src.network->retry.count());
  }
  dst = std::move(result);
  return true;
}

} // namespace broker