#pragma once

#include <vector>

#include <caf/cow_tuple.hpp>
#include <caf/detail/unordered_flat_map.hpp>

#include "broker/alm/peer.hh"
#include "broker/alm/routing_table.hh"
#include "broker/detail/lift.hh"
#include "broker/filter_type.hh"
#include "broker/message.hh"

namespace broker::alm {

template <class Derived, class PeerId>
class stream_transport : public peer<Derived, PeerId, caf::actor>,
                         public caf::stream_manager {
public:
  // -- member types -----------------------------------------------------------

  using peer_id_type = PeerId;

  /// Helper trait for defining streaming-related types for local actors
  /// (workers and stores).
  template <class T>
  struct local_trait {
    /// Type of a single element in the stream.
    using element = caf::cow_tuple<topic, T>;

    /// Type of a full batch in the stream.
    using batch = std::vector<element>;

    /// Type of the downstream_manager that broadcasts data to local actors.
    using manager = caf::broadcast_downstream_manager<element, filter_type,
                                                      detail::prefix_matcher>;
  };

  /// Streaming-related types for workers.
  using worker_trait = local_trait<data>;

  /// Streaming-related types for stores.
  using store_trait = local_trait<internal_command>;

  using peer_message = generic_node_message<peer_id_type>;

  /// Streaming-related types for peers.
  struct peer_trait {
    /// Type of a single element in the stream.
    using element = peer_message;

    using batch = std::vector<element>;

    /// Type of the downstream_manager that broadcasts data to local actors.
    using manager = caf::broadcast_downstream_manager<element, peer_filter>;
  };

  /// Composed downstream_manager type for bundled dispatching.
  using downstream_manager_type
    = caf::fused_downstream_manager<typename peer_trait::manager,
                                    typename worker_trait::manager,
                                    typename store_trait::manager>;

  /// Maps a peer handle to the slot for outbound communication. Our routing
  /// table translates peer IDs to actor handles, but we need one additional
  /// step to get to the associated stream.
  using hdl_to_slot_map
    = caf::detail::unordered_flat_map<caf::actor, caf::stream_slot>;

  // -- constructors, destructors, and assignment operators --------------------

  explicit stream_transport(caf::event_based_actor* self)
    : caf::stream_manager(self), out_(this) {
    continuous(true);
  }

  // -- properties -------------------------------------------------------------

  caf::event_based_actor* self() {
    // Our only constructor accepts an event-based actor. Hence, we know for
    // sure that this case is safe, even though the base type stores self_ as a
    // scheduled_actor pointer.
    return static_cast<caf::event_based_actor*>(this->self_);
  }

  /// Returns the slot for outgoing traffic to `hdl`.
  optional<caf::stream_slot> output_slot(const caf::actor& hdl) const noexcept {
    auto i = hdl_to_ostream_.find(hdl);
    if (i == hdl_to_ostream_.end())
      return nil;
    return i->second;
  }

  /// Returns the slot for incoming traffic from `hdl`.
  optional<caf::stream_slot> input_slot(const caf::actor& hdl) const noexcept {
    auto i = hdl_to_istream_.find(hdl);
    if (i == hdl_to_istream_.end())
      return nil;
    return i->second;
  }

  /// Returns whether this manager has inbound and outbound streams from and to
  /// `hdl`.`
  bool connected_to(const caf::actor& hdl) const noexcept {
    return output_slot(hdl) && input_slot(hdl);
  }

  auto& peer_manager() {
    return out_.template get<typename peer_trait::manager>();
  }

  // -- sending ----------------------------------------------------------------

  void stream_send(const caf::actor& receiver, peer_message& msg) {
    // Fetch the output slot for reaching the receiver.
    auto i = hdl_to_ostream_.find(receiver);
    if (i == hdl_to_ostream_.end()) {
      BROKER_WARNING("unable to locate output slot for receiver");
      return;
    }
    auto slot = i->second;
    // Fetch the buffer for that slot and enqueue the message.
    auto& nested = out_.template get<typename peer_trait::manager>();
    auto j = nested.states().find(slot);
    if (j == nested.states().end()) {
      BROKER_WARNING("unable to access state for output slot");
      return;
    }
    j->second.buf.emplace_back(std::move(msg));
  }

  /// Sends an asynchronous message instead of pushing the data to the stream.
  /// Required for initiating handshakes (because no stream exists at that
  /// point) or for any other communicaiton that should bypass the stream.
  template <class... Ts>
  void async_send(const caf::actor& receiver, Ts&&... xs) {
    self()->send(receiver, std::forward<Ts>(xs)...);
  }

  // Subscriptions bypass the stream.
  template <class... Ts>
  void send(const caf::actor& receiver, atom::subscribe atm, Ts&&... xs) {
    async_send(receiver, atm, std::forward<Ts>(xs)...);
  }

  // Published messages use the streams.
  template <class T>
  void send(const caf::actor& receiver, atom::publish, T msg) {
    stream_send(receiver, msg);
  }

  // -- peering ----------------------------------------------------------------

  // Initiates peering between A (this node) and B (remote peer).
  void start_peering(const peer_id_type& remote_peer, const caf::actor& hdl) {
    BROKER_TRACE(BROKER_ARG(remote_peer) << BROKER_ARG(hdl));
    auto& d = dref();
    // We avoid conflicts in the handshake process by always having the node
    // with the smaller ID initiate the peering. Otherwise, we could end up in a
    // deadlock during handshake if both sides send step 1 at the sime time.
    if (remote_peer < d.id()) {
      self()->send(hdl, atom::peer::value, d.id(), self());
      return;
    }
    if (d.tbl().count(remote_peer) != 0) {
      BROKER_INFO("start_peering ignored: already peering with "
                  << remote_peer);
      return;
    }
    if (!ongoing_peerings_.emplace(remote_peer).second) {
      BROKER_DEBUG("already started peering to " << remote_peer);
      return;
    }
    self()->send(hdl, atom::peer::value, self(), d.id(), d.subscriptions(),
                 d.timestamp());
  }

  // Establishes a stream from B to A.
  caf::outbound_stream_slot<peer_message, caf::actor, peer_id_type, filter_type,
                            uint64_t>
  handle_peering_request(const caf::actor& hdl, const peer_id_type& remote_id,
                         const filter_type& topics, uint64_t timestamp) {
    BROKER_TRACE(BROKER_ARG(hdl) << BROKER_ARG(remote_id) << BROKER_ARG(topics)
                                 << BROKER_ARG(timestamp));
    auto& d = dref();
    // Sanity checking.
    if (!hdl) {
      BROKER_WARNING("received peering handshake with invalid handle");
      return {};
    }
    // Check whether there's already a peering relation established or underway.
    if (dref().tbl().count(remote_id) > 0) {
      BROKER_DEBUG("drop peering request: already have a direct connection to "
                   << remote_id);
      return {};
    }
    if (hdl_to_ostream_.count(hdl) > 0 || hdl_to_istream_.count(hdl) > 0) {
      BROKER_DEBUG("drop peering request: already peering to " << remote_id);
    }
    // Open output stream (triggers handle_peering_handshake_1 on the remote),
    // sending our subscriptions, timestamp, etc. as handshake data.
    auto data = std::make_tuple(caf::actor_cast<caf::actor>(self()), d.id(),
                              d.subscriptions(), d.timestamp());
    auto slot = d.template add_unchecked_outbound_path<peer_message>(
      hdl, std::move(data));
    out_.template assign<typename peer_trait::manager>(slot);
    hdl_to_ostream_[hdl] = slot;
    return slot;
  }

  // Acks the stream from B to A and establishes a stream from A to B.
  caf::outbound_stream_slot<peer_message, caf::actor, peer_id_type>
  handle_peering_handshake_1(caf::stream<peer_message> in,
                             const caf::actor& hdl,
                             const peer_id_type& remote_id,
                             const filter_type& topics, uint64_t timestamp) {
    BROKER_TRACE(BROKER_ARG(hdl) << BROKER_ARG(remote_id) << BROKER_ARG(topics)
                                 << BROKER_ARG(timestamp));
    auto& d = dref();
    // Sanity checking. At this stage, we must have no direct connection routing
    // table entry and no open streams yet.
    if (!hdl) {
      BROKER_WARNING("received peering handshake with invalid handle");
      return {};
    }
    if (hdl_to_ostream_.count(hdl) != 0 || hdl_to_istream_.count(hdl) != 0) {
      BROKER_ERROR("drop handshake #1: already have open streams to "
                   << remote_id);
      return {};
    }
    // Add routing table entry for this direct connection.
    if (!d.tbl().emplace(remote_id, hdl).second) {
      BROKER_ERROR("drop handshake #1: already have a direct connection to "
                   << remote_id);
      return {};
    }
    // Add streaming slots for this connection.
    auto data = std::make_tuple(caf::actor_cast<caf::actor>(self()), d.id());
    auto oslot = d.template add_unchecked_outbound_path<peer_message>(
      hdl, std::move(data));
    out_.template assign<typename peer_trait::manager>(oslot);
    auto islot = d.add_unchecked_inbound_path(in);
    hdl_to_ostream_[hdl] = oslot;
    hdl_to_istream_[hdl] = islot;
    ongoing_peerings_.erase(remote_id);
    return oslot;
  }

  // Acks the stream from A to B.
  void handle_peering_handshake_2(caf::stream<peer_message> in,
                                  const caf::actor& hdl,
                                  const peer_id_type& remote_id) {
    BROKER_TRACE(BROKER_ARG(hdl) << BROKER_ARG(remote_id));
    auto& d = dref();
    // Sanity checking. At this stage, we must have an open output stream but no
    // input stream yet.
    if (!hdl) {
      BROKER_WARNING("received peering handshake with invalid handle");
      return;
    }
    if (hdl_to_ostream_.count(hdl) == 0) {
      BROKER_ERROR("drop handshake #2: no open output stream to " << remote_id);
      return;
    }
    if (hdl_to_istream_.count(hdl) != 0) {
      BROKER_ERROR("drop handshake #2: already have inbound stream from "
                   << remote_id);
      return;
    }
    // Add routing table entry for this direct connection.
    if (!d.tbl().emplace(remote_id, hdl).second) {
      BROKER_ERROR("drop handshake #1: already have a direct connection to "
                   << remote_id);
      return;
    }
    ongoing_peerings_.erase(remote_id);
    // Add inbound streaming slot for this connection.
    hdl_to_istream_[hdl] = d.add_unchecked_inbound_path(in);
  }

  // -- overridden member functions of caf::stream_manager ---------------------

  void
  handle(caf::inbound_path* path, caf::downstream_msg::batch& batch) override {
    BROKER_TRACE(CAF_ARG(path) << CAF_ARG(batch));
    using peer_batch = typename peer_trait::batch;
    if (batch.xs.template match_elements<peer_batch>()) {
      for (auto& x : batch.xs.template get_mutable_as<peer_batch>(0))
        dref().handle_publication(x);
    }
  }

  bool done() const override {
    return !continuous() && pending_handshakes_ == 0
           && inbound_paths_.empty() && out_.clean();
  }

  bool idle() const noexcept override {
    // Same as `stream_stage<...>`::idle().
    return out_.stalled() || (out_.clean() && this->inbound_paths_idle());
  }

  downstream_manager_type& out() override {
    return out_;
  }

  template <class... Fs>
  caf::behavior make_behavior(Fs... fs) {
    using detail::lift;
    auto& d = dref();
    return {
      std::move(fs)...,
      lift<atom::peer>(d, &Derived::start_peering),
      lift<atom::peer>(d, &Derived::handle_peering_request),
      lift<>(d, &Derived::handle_peering_handshake_1),
      lift<>(d, &Derived::handle_peering_handshake_2),
      lift<atom::publish>(d, &Derived::publish),
      lift<atom::subscribe>(d, &Derived::subscribe),
      lift<atom::publish>(d, &Derived::handle_publication),
      lift<atom::subscribe>(d, &Derived::handle_subscription),
    };
  }

protected:
  /// Organizes downstream communication to peers as well as local subscribers.
  downstream_manager_type out_;

  /// Maps communication handles to output slots.
  hdl_to_slot_map hdl_to_ostream_;

  /// Maps communication handles to input slots.
  hdl_to_slot_map hdl_to_istream_;

  /// Stores nodes we have in-flight peering handshakes to.
  std::set<peer_id_type> ongoing_peerings_;

private:
  Derived& dref() {
    return static_cast<Derived&>(*this);
  }
};

} // namespace broker::alm
