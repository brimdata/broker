#define SUITE alm.multipath

#include "broker/alm/multipath.hh"

#include "test.hh"

using broker::alm::multipath;

using namespace broker;

namespace {

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

FIXTURE_SCOPE_END()
