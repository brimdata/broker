#define SUITE multi_hop_routing

#include "broker/core_actor.hh"

#include "test.hh"

#include <caf/test/io_dsl.hpp>

#include "broker/configuration.hh"
#include "broker/endpoint.hh"
#include "broker/logger.hh"

using namespace caf;
using namespace broker;
using namespace broker::detail;

namespace {

using peer_id = std::string;

struct peer {
  peer_id id;
  std::vector<peer> next_hops;

  explicit peer(peer_id id) : id(std::move(id)) {
    // nop
  }
};

struct indirect_entry {
  size_t weight;
  peer_id next_hop;
};

template <class Inspector>
auto inspect(Inspector& f, indirect_entry& x) {
  return f(caf::meta::type_name("indirect_entry"), x.weight, x.next_hop);
}

class routing_table {
public:
  void add(peer_id id, caf::actor hdl) {
    direct_paths.emplace(std::move(id), std::move(hdl));
  }

  std::map<peer_id, caf::actor> direct_paths;

  std::map<peer_id, indirect_entry> indirect_paths;
};

std::string to_string(const routing_table& tbl) {
  std::string result;
  result = "{\"direct_paths\" = ";
  result += caf::deep_to_string(tbl.direct_paths);
  result += ", \"indirect_paths\" = ";
  result += caf::deep_to_string(tbl.indirect_paths);
  result += '}';
  return result;
}

void print(const peer& hop, std::string& buf) {
  buf += hop.id;
  if (!hop.next_hops.empty()) {
    buf += '(';
    auto i = hop.next_hops.begin();
    auto e = hop.next_hops.end();
    print(*i, buf);
    ++i;
    for (;i != e;++i) {
      buf += ',';
      print(*i, buf);
    }
    buf += ')';
  }
}

std::string to_string(const peer& hop) {
  std::string result;
  print(hop, result);
  return result;
}

using path_type = std::vector<peer_id>;

struct path_less {
  bool operator()(const path_type& x, const path_type& y) const {
    // We sort by shortest path first, then alphabetically.
    return x.size() != y.size() ? x.size() < y.size() : x < y;
  }
};

using path_set = std::set<path_type, path_less>;

struct peer_actor_state {
  routing_table tbl;

  std::map<topic, std::map<peer_id, path_set>> subscriptions;
};

bool contains(const std::vector<peer_id>& ids, const peer_id& id) {
  auto predicate = [&](const peer_id& pid) { return pid == id; };
  return std::any_of(ids.begin(), ids.end(), predicate);
}

using peer_actor_type = caf::stateful_actor<peer_actor_state>;

caf::behavior peer_actor(peer_actor_type* self, peer_id id) {
  return {
    [=](peer_id& id, caf::actor& hdl) {
      self->state.tbl.add(std::move(id), std::move(hdl));
    },
    [=](atom::join, const topic& what) {
      std::vector<peer_id> path{id};
      for (auto& kvp : self->state.tbl.direct_paths)
        self->send(kvp.second, atom::join::value, path, what);
    },
    [=](atom::join, std::vector<peer_id>& path, topic& what) {
      auto& st = self->state;
      // Drop nonsense messages.
      if (path.empty() || what.string().empty())
        return;
      if (st.tbl.direct_paths.count(path.back()) == 0)
        return;
      // Drop all paths that contain loops.
      if (contains(path, id))
        return;
      // Update weight of indirect paths if necessary.
      size_t weight = path.size();
      if (weight > 1) {
        auto& ipaths = st.tbl.indirect_paths;
        auto i = ipaths.find(path[0]);
        if (i == ipaths.end())
          ipaths.emplace(path[0], indirect_entry{weight, path.back()});
        else if (i->second.weight > weight)
          i->second.weight = weight;
      }
      // Forward subscription to all peers.
      path.emplace_back(id);
      for (auto& [pid, hdl] : st.tbl.direct_paths)
        if (!contains(path, pid))
          self->send(hdl, atom::join::value, path, what);
      // Store the subscription with the reverse path.
      path.pop_back();
      auto dest = std::move(path[0]);
      path.erase(path.begin());
      std::reverse(path.begin(), path.end());
      st.subscriptions[what][dest].emplace(std::move(path));
    },
  };
}

struct fixture : test_coordinator_fixture<> {};

} // namespace

FIXTURE_SCOPE(multi_hop_routing_tests, fixture)

TEST(paths are convertible to strings) {
  peer root{"A"};
  auto& b = root.next_hops.emplace_back("B");
  b.next_hops.emplace_back("D");
  b.next_hops.emplace_back("E");
  auto& c = root.next_hops.emplace_back("C");
  CHECK_EQUAL(to_string(root), "A(B(D,E),C)");
}

TEST(foo) {
  using peer_ids = std::vector<peer_id>;
  std::map<peer_id, caf::actor> peers;
  for (auto& id : peer_ids{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"})
    peers[id] = sys.spawn(peer_actor, id);
  std::map<peer_id, peer_ids> connections{
    {"A", {"B", "C", "J"}},
    {"B", {"A", "D", "E"}},
    {"C", {"A", "F", "H", "I"}},
    {"D", {"B", "G"}},
    {"E", {"B", "G"}},
    {"F", {"C", "H"}},
    {"G", {"D", "E", "J"}},
    {"H", {"C", "F", "I", "J"}},
    {"I", {"C", "H", "J"}},
    {"J", {"A", "G", "H","I"}},
  };
  for (auto& [id, links] : connections)
    for (auto& link : links)
      anon_send(peers[id], link, peers[link]);
  run();
  anon_send(peers["G"], atom::join::value, topic{"foo"});
  //anon_send(peers["H"], atom::join::value, topic{"foo"});
  run();
  //std::cout<<"A.subs = "<<caf::deep_to_string(deref<peer_actor_type>(peers["A"]).state.subscriptions)<<'\n';
  for (auto& kvp : peers)
    std::cout<<kvp.first<<".tbl= "<<caf::deep_to_string(deref<peer_actor_type>(kvp.second).state.tbl)<<'\n';
  run();
  for (auto& kvp : peers)
    anon_send_exit(kvp.second, exit_reason::user_shutdown);
}

FIXTURE_SCOPE_END()
