#define SUITE alm.multipath

#include "broker/alm/multipath.hh"

#include "test.hh"

using broker::alm::multipath;

using namespace broker;

namespace {

using linear_path = std::vector<std::string>;

struct fixture {

};

} // namespace

FIXTURE_SCOPE(multipath_tests, fixture)

TEST(multipaths are default constructible) {
  multipath<std::string> p;
  CHECK_EQUAL(p.id(), "");
  CHECK_EQUAL(p.nodes().size(), 0u);
  CHECK_EQUAL(caf::deep_to_string(p), R"__((""))__");
}

TEST(users can fill multipaths with emplace_node) {
  multipath<std::string> p{"a"};
  auto ac = p.emplace_node("ac").first;
  ac->emplace_node("acb");
  ac->emplace_node("aca");
  auto ab = p.emplace_node("ab").first;
  ab->emplace_node("abb");
  ab->emplace_node("aba");
  CHECK_EQUAL(
    caf::deep_to_string(p),
    R"__(("a", [("ab", [("aba"), ("abb")]), ("ac", [("aca"), ("acb")])]))__");
}

TEST(multipaths are constructible from linear paths) {
  linear_path abc{"a", "b", "c"};
  multipath<std::string> path{abc.begin(), abc.end()};
  CHECK_EQUAL(caf::deep_to_string(path), R"__(("a", [("b", [("c")])]))__");
}

TEST(multipaths are copy constructible and comparable) {
  linear_path abc{"a", "b", "c"};
  multipath<std::string> path1{abc.begin(), abc.end()};
  auto path2 = path1;
  CHECK_EQUAL(caf::deep_to_string(path1), caf::deep_to_string(path2));
  CHECK_EQUAL(path1, path2);
}

TEST(splicing an empty or equal linear path is a nop) {
  linear_path abc{"a", "b", "c"};
  multipath<std::string> path1{abc.begin(), abc.end()};
  auto path2 = path1;
  linear_path empty_path;
  CHECK(path2.splice(empty_path.begin(), empty_path.end()));
  CHECK_EQUAL(path1, path2);
  CHECK(path2.splice(abc.begin(), abc.end()));
  CHECK_EQUAL(path1, path2);
}

TEST(splicing merges linear paths into multipaths) {
  linear_path abc{"a", "b", "c"};
  linear_path abd{"a", "b", "d"};
  linear_path aef{"a", "e", "f"};
  linear_path aefg{"a", "e", "f", "g"};
  multipath<std::string> path{"a"};
  for (const auto& lp : {abc, abd, aef, aefg})
    CHECK(path.splice(lp.begin(), lp.end()));
  CHECK_EQUAL(
    caf::deep_to_string(path),
    R"__(("a", [("b", [("c"), ("d")]), ("e", [("f", [("g")])])]))__");
}

FIXTURE_SCOPE_END()
