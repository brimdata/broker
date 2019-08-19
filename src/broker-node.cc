#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <iterator>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <caf/atom.hpp>
#include <caf/behavior.hpp>
#include <caf/config_option_adder.hpp>
#include <caf/deep_to_string.hpp>
#include <caf/downstream.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/exit_reason.hpp>
#include <caf/send.hpp>
#include <caf/term.hpp>
#include <caf/uri.hpp>

#include "broker/atoms.hh"
#include "broker/configuration.hh"
#include "broker/convert.hh"
#include "broker/data.hh"
#include "broker/detail/generator_file_reader.hh"
#include "broker/endpoint.hh"
#include "broker/publisher.hh"
#include "broker/status.hh"
#include "broker/subscriber.hh"
#include "broker/topic.hh"

using std::string;

using broker::count;
using broker::data;
using broker::topic;

// -- process-wide state -------------------------------------------------------

namespace {

string node_name;

} // namespace <anonymous>

// -- I/O utility --------------------------------------------------------------

namespace detail {

namespace {

std::mutex ostream_mtx;

} // namespace <anonymous>

int print_impl(std::ostream& ostr, const char* x) {
  ostr << x;
  return 0;
}

int print_impl(std::ostream& ostr, const string& x) {
  ostr << x;
  return 0;
}

int print_impl(std::ostream& ostr, const caf::term& x) {
  ostr << x;
  return 0;
}

template <class T>
int print_impl(std::ostream& ostr, const T& x) {
  return print_impl(ostr, caf::deep_to_string(x));
}

template <class... Ts>
void println(std::ostream& ostr, Ts&&... xs) {
  std::unique_lock<std::mutex> guard{ostream_mtx};
  std::initializer_list<int>{print_impl(ostr, std::forward<Ts>(xs))...};
  ostr << caf::term::reset_endl;
}

} // namespace detail

namespace out {

template <class... Ts>
void println(Ts&&... xs) {
  detail::println(std::cout, std::forward<Ts>(xs)...);
}

} // namespace out

namespace err {

template <class... Ts>
void println(Ts&&... xs) {
  detail::println(std::cerr, caf::term::red, node_name, ": ",
                  std::forward<Ts>(xs)...);
}

} // namespace err

namespace verbose {

namespace {

std::atomic<bool> enabled;

} // namespace <anonymous>

template <class... Ts>
void println(Ts&&... xs) {
  if (enabled)
    detail::println(std::clog, caf::term::blue,
                    std::chrono::system_clock::now(), " ", node_name, ": ",
                    std::forward<Ts>(xs)...);
}

} // namespace verbose

// -- CAF setup ----------------------------------------------------------------

using namespace caf;

namespace {

// -- constants ----------------------------------------------------------------

size_t default_payload_size = 0;

timespan default_rendezvous_retry = std::chrono::milliseconds(250);

size_t default_ping_count = 100;

// -- atom constants -----------------------------------------------------------

using ping_atom = atom_constant<atom("ping")>;

using pong_atom = atom_constant<atom("pong")>;

using relay_atom = atom_constant<atom("relay")>;

using blocking_atom = atom_constant<atom("blocking")>;

using stream_atom = atom_constant<atom("stream")>;

// -- type aliases -------------------------------------------------------------

using uri_list = std::vector<uri>;

using topic_list = std::vector<topic>;

using string_list = std::vector<string>;

using mode_fun = void (*)(broker::endpoint&, topic_list);

// -- constants ----------------------------------------------------------------

constexpr size_t max_cap = std::numeric_limits<size_t>::max();

// -- program options ----------------------------------------------------------

class config : public broker::configuration {
public:
  config() {
    opt_group{custom_options_, "global"}
      .add<bool>("verbose,v", "print status and debug output")
      .add<string>("name,N", "set node name in verbose output")
      .add<string_list>("topics,t", "topics for sending/receiving messages")
      .add<atom_value>("mode,m", "'relay', 'generate', 'ping', or 'pong'")
      .add<string>("generator-file,g",
                   "path to a generator file ('generate' mode only)")
      .add<size_t>("payload-size,s",
                   "additional number of bytes for the ping message")
      .add<timespan>("rendezvous-retry",
                     "timeout before repeating the first rendezvous ping "
                     "message (default: 50ms)")
      .add<size_t>("num-pings,n",
                   "number of pings (default: 100, 'ping' mode only)")
      .add<uri_list>("peers,p",
                     "list of peers we connect to on startup in "
                     "<tcp://$host:$port> notation")
      .add<uint16_t>("local-port,l",
                     "local port for publishing this endpoint at");
  }
};

// -- convenience get_or and get_if overloads for enpoint ----------------------

template <class T>
auto get_or(broker::endpoint& d, string_view key, const T& default_value)
-> decltype(caf::get_or(d.system().config(), key, default_value)) {
  return caf::get_or(d.system().config(), key, default_value);
}

template <class T>
auto get_if(broker::endpoint* d, string_view key)
-> decltype(caf::get_if<T>(&(d->system().config()), key)) {
  return caf::get_if<T>(&(d->system().config()), key);
}

// -- message creation and introspection ---------------------------------------

/// @pre `is_ping_msg(x) || is_pong_msg(x)`
count msg_id(const broker::data& x) {
  auto& vec = caf::get<broker::vector>(x);
  return caf::get<count>(vec[1]);
}

bool is_ping_msg(const broker::data& x) {
  if (auto vec = caf::get_if<broker::vector>(&x)) {
    if (vec->size() == 3) {
      auto& xs = *vec;
      auto str = caf::get_if<string>(&xs[0]);
      return str && *str == "ping"
             && caf::holds_alternative<count>(xs[1])
             && caf::holds_alternative<string>(xs[2]);
    }
  }
  return false;
}

bool is_pong_msg(const broker::data& x) {
  if (auto vec = caf::get_if<broker::vector>(&x)) {
    if (vec->size() == 2) {
      auto& xs = *vec;
      auto str = caf::get_if<string>(&xs[0]);
      return str && *str == "pong" && caf::holds_alternative<count>(xs[1]);
    }
  }
  return false;
}

bool is_pong_msg(const broker::data& x, count id) {
  return is_pong_msg(x) && msg_id(x) == id;
}

bool is_stop_msg(const broker::data& x) {
  auto str = caf::get_if<string>(&x);
  return str && *str == "stop";
}

broker::data make_ping_msg(count id, size_t payload_size) {
  return broker::vector{"ping", id, string(payload_size, 'x')};
}

broker::data make_pong_msg(count id) {
  return broker::vector{"pong", id};
}

broker::data make_stop_msg() {
  return "stop";
}

// -- mode implementations -----------------------------------------------------

void relay_mode(broker::endpoint& ep, topic_list topics) {
  verbose::println("relay messages");
  auto in = ep.make_subscriber(topics);
  for (;;) {
    auto x = in.get();
    auto& val = get_data(x);
    if (is_ping_msg(val)) {
      verbose::println("received ping ", msg_id(val));
    } else if (is_pong_msg(val)) {
      verbose::println("received pong ", msg_id(val));
    } else if (is_stop_msg(val)) {
      verbose::println("received stop");
      return;
    } else {
      verbose::println("received: ", val);
    }
  }
}

void generator(caf::event_based_actor* self, caf::actor core,
               std::shared_ptr<size_t> count, const std::string& file_name,
               broker::detail::generator_file_reader_ptr ptr) {
  using generator_ptr = broker::detail::generator_file_reader_ptr;
  using value_type = broker::node_message::value_type;
  self->make_source(
    core,
    [&](generator_ptr& g) {
      // Take ownership of `ptr`.
      g = std::move(ptr);
    },
    [=](generator_ptr& g, caf::downstream<value_type>& out, size_t hint) {
      if (g == nullptr || g->at_end())
        return;
      for (size_t i = 0; i < hint; ++i) {
        if (g->at_end()) {
          *count += i;
          return;
        }
        value_type x;
        if (auto err = g->read(x)) {
          err::println("error while parsing ", file_name, ": ",
                       self->system().render(err));
          g = nullptr;
          *count += i;
          return;
        }
        out.push(std::move(x));
      }
      *count += hint;
    },
    [](const generator_ptr& g) { return g == nullptr || g->at_end(); });
}

void generate_mode(broker::endpoint& ep, topic_list) {
  auto file_name = get_or(ep, "generator-file", "");
  if (file_name.empty())
    return err::println("got no path to a generator file");
  verbose::println("generate messages from: ", file_name);
  auto generator_ptr = broker::detail::make_generator_file_reader(file_name);
  if (generator_ptr == nullptr)
    return err::println("unable to open generator file: ", file_name);
  auto count = std::make_shared<size_t>(0u);
  caf::scoped_actor self{ep.system()};
  auto t0 = std::chrono::system_clock::now();
  auto g = self->spawn(generator, ep.core(), count, file_name,
                       std::move(generator_ptr));
  self->wait_for(g);
  auto t1 = std::chrono::system_clock::now();
  auto delta = t1 - t0;
  using fractional_seconds = std::chrono::duration<double>;
  auto delta_s = std::chrono::duration_cast<fractional_seconds>(delta);
  verbose::println("shipped ", *count, "messages in ", delta_s.count(), "s");
  verbose::println("AVG: ", *count / delta_s.count());
}

void ping_mode(broker::endpoint& ep, topic_list topics) {
  assert(topics.size() > 0);
  auto topic = topics[0];
  verbose::println("send pings to topic ", topic);
  std::vector<timespan> xs;
  auto n = get_or(ep, "num-pings", default_ping_count);
  auto s = get_or(ep, "payload-size", default_payload_size);
  if (n == 0) {
    err::println("send no pings: n = 0");
    return;
  }
  auto in = ep.make_subscriber({topic});
  // Rendezvous between ping and pong. The first ping (id 0) is not part of our
  // measurement. We repeat this initial message until we receive a pong to
  // make sure all broker nodes are up and running.
  bool connected = false;
  auto retry_timeout = get_or(ep, "rendezvous-retry", default_rendezvous_retry);
  ep.publish(topic, make_ping_msg(0, 0));
  while (!connected) {
    auto x = in.get(caf::duration{retry_timeout});
    if (x && is_pong_msg(get_data(*x), 0))
      connected = true;
    else
      ep.publish(topic, make_ping_msg(0, 0));
  }
  // Measurement.
  timespan total_time{0};
  for (count i = 1; i <= n; ++i) {
    bool done = false;
    auto t0 = std::chrono::system_clock::now();
    ep.publish(topic, make_ping_msg(i, s));
    do {
      auto x = in.get();
      done = is_pong_msg(get_data(x), i);
    } while (!done);
    auto t1 = std::chrono::system_clock::now();
    auto roundtrip = std::chrono::duration_cast<timespan>(t1 - t0);
    total_time += roundtrip;
    out::println(roundtrip.count());
  }
  verbose::println("AVG: ", total_time / n);
}

void pong_mode(broker::endpoint& ep, topic_list topics) {
  assert(topics.size() > 0);
  verbose::println("receive pings from topics ", topics);
  auto in = ep.make_subscriber(topics);
  for (;;) {
    auto x = in.get();
    auto& val = get_data(x);
    if (is_ping_msg(val)) {
      verbose::println("received ping ", msg_id(val));
      ep.publish(get_topic(x), make_pong_msg(msg_id(val)));
    } else if (is_stop_msg(val)) {
      verbose::println("received stop");
      return;
    }
  }
}

} // namespace <anonymous>

// -- main function ------------------------------------------------------------

int main(int argc, char** argv) {
  // Parse CLI parameters using our config.
  config cfg;
  cfg.parse(argc, argv);
  broker::endpoint ep{std::move(cfg)};
  // Get mode (mandatory).
  auto mode = get_if<atom_value>(&ep, "mode");
  if (!mode) {
    node_name = "unnamed-node";
    err::println("no mode specified");
    return EXIT_FAILURE;
  }
  // Get process name, using the mode name as fallback.
  if (auto cfg_name = get_if<string>(&ep, "name"))
    node_name = *cfg_name;
  else
    node_name = to_string(*mode);
  // Get topics (mandatory) and make sure this endpoint at least forwards them.
  topic_list topics;
  { // Lifetime scope of temporary variables.
    auto topic_names = get_or(ep, "topics", string_list{});
    if (topics.empty()) {
      err::println("no topics specified");
      return EXIT_FAILURE;
    }
    for (auto& topic_name : topic_names)
      topics.emplace_back(std::move(topic_name));
  }
  ep.forward(topics);
  // Enable verbose output if demanded by user.
  actor verbose_logger;
  if (get_or(ep, "verbose", false)) {
    verbose::enabled = true;
    // Launch background worker when running in verbose mode.
    auto& sys = ep.system();
    auto& groups = sys.groups();
    auto g1 = groups.get_local("broker/errors");
    auto g2 = groups.get_local("broker/statuses");
    verbose_logger = sys.spawn_in_groups({g1, g2}, [](event_based_actor* self) {
      return behavior{
        [=](broker::atom::local, broker::error& x) {
          verbose::println(x);
        },
        [=](broker::atom::local, broker::status& x) {
          verbose::println(x);
        }
      };
    });
  }
  // Publish endpoint at demanded port.
  if (auto local_port = get_if<uint16_t>(&ep, "local-port")) {
    verbose::println("listen for peers on port ", *local_port);
    ep.listen({}, *local_port);
  }
  // Select function f based on the mode.
  mode_fun f = nullptr;
  switch (static_cast<uint64_t>(*mode)) {
    case relay_atom::uint_value():
      f = relay_mode;
      break;
    case ping_atom::uint_value():
      f = ping_mode;
      break;
    case pong_atom::uint_value():
      f = pong_mode;
      break;
    default:
      err::println("invalid mode: ", mode);
      return EXIT_FAILURE;
  }
  // Connect to peers.
  auto peers = get_or(ep, "peers", uri_list{});
  for (auto& peer : peers) {
    auto& auth = peer.authority();
    if (peer.scheme() != "tcp") {
      err::println("unrecognized scheme (expected tcp) in: <", peer, '>');
    } else if (auth.empty()) {
      err::println("no authority component in: <", peer, '>');
    } else {
      auto host = to_string(auth.host);
      auto port = auth.port;
      verbose::println("connect to ", host, " on port ", port, " ...");
      ep.peer(host, port);
    }
  }
  f(ep, std::move(topics));
  // Disconnect from peers.
  for (auto& peer : peers) {
    auto& auth = peer.authority();
    if (peer.scheme() == "tcp" && !auth.empty()) {
      auto host = to_string(auth.host);
      auto port = auth.port;
      verbose::println("diconnect from ", host, " on port ", port, " ...");
      ep.unpeer_nosync(host, port);
    }
  }
  // Stop utility actors.
  anon_send_exit(verbose_logger, exit_reason::user_shutdown);
}

