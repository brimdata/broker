#define SUITE alm.routing_table

#include "broker/alm/routing_table.hh"

#include "test.hh"

using namespace broker;

namespace {

struct fixture {
  using table_type = alm::routing_table<std::string, int>;

  fixture() {
    // We use the subset of the topology that we use in the peer unit test:
    //
    //                                     +---+
    //                               +-----+ D +-----+
    //                               |     +---+     |
    //                               |               |
    //                             +---+           +---+
    //                       +-----+ B |           | I +-+
    //                       |     +---+           +---+ |
    //                       |       |               |   |
    //                       |       |     +---+     |   |
    //                       |       +-----+ E +-----+   |
    //                       |             +---+         |
    //                     +---+                       +---+
    //                     | A +-----------------------+ J |
    //                     +---+                       +---+
    //
    auto add = [&](std::string id,
                   std::vector<std::vector<std::string>> paths) {
      auto& entry = tbl.emplace(id, table_type::mapped_type{0}).first->second;
      for (auto& path : paths)
        entry.versioned_paths.emplace(std::move(path),
                                      alm::vector_timestamp(path.size()));
    };
    add("B", {{"B"}, {"J", "I", "D", "B"}, {"J", "I", "E", "B"}});
    add("D", {{"B", "D"}, {"J", "I", "D"}});
    add("E", {{"B", "E"}, {"J", "I", "E"}});
    add("I", {{"B", "E", "I"}, {"B", "D", "I"}, {"J", "I"}});
    add("J", {{"J"}, {"B", "D", "I", "J"}, {"B", "E", "I", "J"}});
  }

  // Creates a list of IDs (strings).
  template <class... Ts>
  auto ls(Ts... xs) {
    return std::vector<std::string>{std::move(xs)...};
  }

  alm::routing_table<std::string, int> tbl;
};

} // namespace

FIXTURE_SCOPE(routing_table_tests, fixture)

TEST(erase removes all paths that to and from a peer) {
  MESSAGE("before removing J, the shortest path to I is: J -> I");
  {
    auto path = shortest_path(tbl, "I");
    REQUIRE(path != nullptr);
    CHECK_EQUAL(*path, ls("J", "I"));
  }
  MESSAGE("after removing J, the shortest path to I is: B -> D -> I");
  {
    erase(tbl, "J");
    auto path = shortest_path(tbl, "I");
    REQUIRE(path != nullptr);
    CHECK_EQUAL(*path, ls("B", "D", "I"));
  }
}

TEST(erase_direct drops the direct path but peers can remain reachable) {
  MESSAGE("before calling erase_direct(B), we reach B in one hop");
  {
    auto path = shortest_path(tbl, "B");
    REQUIRE(path != nullptr);
    CHECK_EQUAL(*path, ls("B"));
  }
  MESSAGE("after calling erase_direct(B), we need four hops to reach B");
  {
    erase_direct(tbl, "B");
    auto path = shortest_path(tbl, "B");
    REQUIRE(path != nullptr);
    CHECK_EQUAL(*path, ls("J", "I", "D", "B"));
  }
}

FIXTURE_SCOPE_END()
