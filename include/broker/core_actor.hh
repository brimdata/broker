#pragma once

#include <fstream>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>

#include "broker/atoms.hh"
#include "broker/configuration.hh"
#include "broker/endpoint.hh"
#include "broker/endpoint_info.hh"
#include "broker/error.hh"
#include "broker/filter_type.hh"
#include "broker/logger.hh"
#include "broker/network_info.hh"
#include "broker/optional.hh"
#include "broker/peer_info.hh"
#include "broker/status.hh"

#include "broker/detail/core_policy.hh"
#include "broker/detail/network_cache.hh"
#include "broker/detail/radix_tree.hh"

namespace broker {

struct core_state {
  // --- nested types ----------------------------------------------------------

  using governor_type = caf::detail::stream_distribution_tree<detail::core_policy>;

  using governor_ptr = caf::intrusive_ptr<governor_type>;

  struct pending_peer_state {
    caf::stream_slot slot;
    caf::response_promise rp;
  };

  using pending_peers_map = std::unordered_map<caf::actor, pending_peer_state>;

  /// Identifies the two individual streams forming a bidirectional channel.
  /// The first ID denotes the *input*  and the second ID denotes the *output*.
  using stream_id_pair = std::pair<caf::stream_slot, caf::stream_slot>;

  // --- construction ----------------------------------------------------------

  core_state(caf::event_based_actor* ptr);

  /// Establishes all invariants.
  void init(filter_type initial_filter, broker_options opts,
            endpoint::clock* ep_clock);

  // --- filter management -----------------------------------------------------

  /// Sends the current filter to all peers.
  void update_filter_on_peers();

  /// Adds `xs` to our filter and update all peers on changes.
  void add_to_filter(filter_type xs);

  // --- convenience functions for querying state ------------------------------

  /// Returns whether `x` is either a pending peer or a connected peer.
  bool has_peer(const caf::actor& x);

  /// Returns whether a master for `name` probably exists already on one of our
  /// peers.
  bool has_remote_master(const std::string& name);

  /// Returns the policy object.
  detail::core_policy& policy();

  // --- convenience functions for sending errors and events -------------------

  template <ec ErrorCode>
  void emit_error(caf::actor hdl, const char* msg) {
    auto emit = [=](network_info x) {
      BROKER_INFO("error" << ErrorCode << x);
      // TODO: consider creating the data directly rather than going through the
      //       error object and converting it.
      auto err
        = make_error(ErrorCode, endpoint_info{hdl.node(), std::move(x)}, msg);
      governor->policy().local_push(
        make_data_message(topics::errors, get_as<data>(err)));
    };
    if (self->node() != hdl.node())
      cache.fetch(hdl,
                  [=](network_info x) { emit(std::move(x)); },
                  [=](caf::error) { emit({}); });
    else
      emit({});
  }

  template <ec ErrorCode>
  void emit_error(caf::strong_actor_ptr hdl, const char* msg) {
    emit_error<ErrorCode>(caf::actor_cast<caf::actor>(hdl), msg);
  }

  template <ec ErrorCode>
  void emit_error(network_info inf, const char* msg) {
    auto x = cache.find(inf);
    if (x)
      emit_error<ErrorCode>(std::move(*x), msg);
    else {
      BROKER_INFO("error" << ErrorCode << inf);
      auto err
        = make_error(ErrorCode, endpoint_info{node_id(), std::move(inf)}, msg);
      governor->policy().local_push(
        make_data_message(topics::errors, get_as<data>(err)));
    }
  }

  template <sc StatusCode>
  void emit_status(caf::actor hdl, const char* msg) {
    static_assert(StatusCode != sc::peer_added,
                  "Use emit_peer_added_status instead");
    auto emit = [=](network_info x) {
      BROKER_INFO("status" << StatusCode << x);
      // TODO: consider creating the data directly rather than going through the
      //       status object and converting it.
      auto stat = status::make<StatusCode>(
        endpoint_info{hdl.node(), std::move(x)}, msg);
      governor->policy().local_push(
        make_data_message(topics::statuses, get_as<data>(stat)));
    };
    if (self->node() != hdl.node())
      cache.fetch(hdl,
                  [=](network_info x) { emit(x); },
                  [=](caf::error) { emit({}); });
    else
      emit({});
  }

  void emit_peer_added_status(caf::actor hdl, const char* msg);

  template <sc StatusCode>
  void emit_status(caf::strong_actor_ptr hdl, const char* msg) {
    emit_status<StatusCode>(caf::actor_cast<caf::actor>(std::move(hdl)), msg);
  }

  void sync_with_status_subscribers(caf::actor new_peer);

  // --- member variables ------------------------------------------------------

  /// A copy of the current Broker configuration options.
  broker_options options;

  /// Stores all master actors created by this core.
  std::unordered_map<std::string, caf::actor> masters;

  /// Stores all clone actors created by this core.
  std::unordered_multimap<std::string, caf::actor> clones;

  /// Requested topics on this core.
  filter_type filter;

  /// Multiplexes local streams and streams for peers.
  governor_ptr governor;

  /// Maps pending peer handles to output IDs. An invalid stream ID indicates
  /// that only "step #0" was performed so far. An invalid stream ID
  /// corresponds to `peer_status::connecting` and a valid stream ID
  /// cooresponds to `peer_status::connected`. The status for a given handle
  /// `x` is `peer_status::peered` if `governor->has_peer(x)` returns true.
  pending_peers_map pending_peers;

  /// Points to the owning actor.
  caf::event_based_actor* self;

  /// Associates network addresses to remote actor handles and vice versa.
  detail::network_cache cache;

  /// Name shown in logs for all instances of this actor.
  static const char* name;

  /// Set to `true` after receiving a shutdown message from the endpoint.
  bool shutting_down;

  /// Required when spawning data stores.
  endpoint::clock* clock;

  /// Keeps track of all actors that subscribed to status updates.
  std::unordered_set<caf::actor> status_subscribers;

  /// Keeps track of all actors that currently wait for handshakes to complete.
  std::unordered_map<caf::actor, size_t> peers_awaiting_status_sync;

  /// Handle for recording all subscribed topics (if enabled).
  std::ofstream topics_file;

  /// Handle for recording all peers (if enabled).
  std::ofstream peers_file;
};

caf::behavior core_actor(caf::stateful_actor<core_state>* self,
                         filter_type initial_filter, broker_options opts,
                         endpoint::clock* clock);

} // namespace broker
