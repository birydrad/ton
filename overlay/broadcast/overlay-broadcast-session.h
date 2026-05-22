/*
    This file is part of TON Blockchain Library.

    TON Blockchain Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#pragma once

#include <algorithm>
#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "adnl/adnl-node-id.hpp"
#include "adnl/adnl.h"
#include "keys/keys.hpp"
#include "overlay/broadcast/algorithm.h"
#include "overlay/broadcast/overlay-broadcast-env.h"
#include "overlay/broadcast/overlay-broadcast.h"
#include "overlay/broadcast/profile.h"
#include "overlay/broadcast/storage.h"
#include "overlay/broadcast/wire.h"
#include "overlay/overlay.h"
#include "overlay/overlays.h"
#include "td/actor/coro_task.h"
#include "td/utils/StringBuilder.h"
#include "td/utils/buffer.h"

namespace ton {
namespace overlay {

namespace proto = broadcast::algorithm;

constexpr int VERBOSITY_NAME(OVERLAY_BROADCAST_WARNING) = verbosity_WARNING;
constexpr int VERBOSITY_NAME(OVERLAY_BROADCAST_INFO) = verbosity_DEBUG;

template <class T>
void append_unique(std::vector<T> &values, const T &value) {
  if (std::find(values.begin(), values.end(), value) == values.end()) {
    values.push_back(value);
  }
}

// One broadcast's full lifecycle. Owns its own state and drives its own logic against
// Env (host contract) plus narrow engine-owned services passed in at construction.
// Constructed by OverlayBroadcasts::start_*; methods are called by the
// engine router and by the session itself recursively (dispatch_event → run_actions →
// schedule another event).
struct SessionEvent {
  struct Receive {
    IncomingMessage incoming;
    adnl::AdnlNodeIdShort src_peer_id;
  };
  struct Publish {
    BroadcastSource source;
  };
  struct Timer {
    td::uint64 token = 0;
  };
  // Engine-fanned-out peer notifications. The engine has already updated PeerMap (and any
  // BroadcastShared::on_peer_upsert hooks) before pushing these. The session forwards them as
  // proto::evt::PeerUpsert / PeerRemove to its algorithm — a notification, not a command.
  struct PeerUpsert {
    proto::Peer peer;
  };
  struct PeerRemove {
    proto::PeerId peer = 0;
  };

  std::variant<Receive, Publish, Timer, PeerUpsert, PeerRemove> value;
};

class PieceTracker {
 public:
  void set_mode(const BroadcastMode &mode);
  bool has_duplicate(proto::PieceId seqno) const;
  void record(proto::PieceId seqno, adnl::AdnlNodeIdShort sender);
  size_t seen_piece_count() const;
  size_t unique_sender_count() const;

 private:
  std::unordered_set<proto::PieceId> seen_pieces_;
  std::vector<adnl::AdnlNodeIdShort> senders_;
  bool enabled_ = false;
};

class PayloadState {
 public:
  bool has_body() const;

  void set_body(td::BufferSlice body, std::unique_ptr<td::fec::Encoder> encoder);
  const td::BufferSlice &body() const;
  td::Result<td::fec::Symbol> make_piece(proto::PieceId piece_id);

  td::Status ensure_decoder(std::unique_ptr<td::fec::Decoder> decoder);
  td::Status accept_piece(td::fec::Symbol &symbol);
  td::Result<std::optional<td::BufferSlice>> try_decode_body(const Overlay::BroadcastDataHash &data_hash);

 private:
  std::optional<td::BufferSlice> body_;
  std::unique_ptr<td::fec::Encoder> encoder_;
  std::unique_ptr<td::fec::Decoder> decoder_;
};

class SourceState {
 public:
  struct Validated {
    BroadcastSource source;
    BroadcastCheckResult check_result = BroadcastCheckResult::Allowed;
    td::BufferSlice broadcast_signature;
  };

  Validated *validated();
  const Validated *validated() const;
  void adopt(BroadcastSource source, BroadcastCheckResult check);

 private:
  std::optional<Validated> validated_;
};

struct SessionContext {
  Env &env;
  BroadcastSessionOwner &owner;
  // Engine-owned stable AdnlNodeIdShort↔PeerId mapping. Shared by all sessions in the overlay.
  PeerMap &peer_map;
  // Algorithm family for this broadcast's mode. The session calls family->make_algorithm(init)
  // and reads wire/storage/session from it.
  AlgorithmFamily *family = nullptr;
};

class OverlayBroadcastSession : public std::enable_shared_from_this<OverlayBroadcastSession> {
 public:
  friend td::StringBuilder &operator<<(td::StringBuilder &sb, const OverlayBroadcastSession &session);

  // === Logic entry points (driven by OverlayBroadcasts router) ========================================

  // Build a fully attached session. The session constructs its own profile
  // (wire+storage+algorithm) from `mode + opts` plus the seed protocol state
  // (id, self, origin) derived from `meta`.
  static std::shared_ptr<OverlayBroadcastSession> make(SessionContext ctx, OverlayBroadcastOptions opts,
                                                       BroadcastMeta meta);

  // Enqueue externally driven session work. The drain loop processes one event at a time,
  // including async action emission, so state transitions cannot interleave across awaits.
  void push_event(SessionEvent event);
  void set_source_body(const BroadcastMode &mode, td::BufferSlice body);
  const Overlay::BroadcastHash &broadcast_id() const;
  bool is_expired(td::uint32 now) const;
  bool is_delivered() const;
  double elapsed_since_start() const;
  void mark_erased();

 private:
  // Pending: nothing delivered yet. Delivered: body handed to the host. Cancelled: erased
  // from the owner; coroutines holding a strong ref bail rather than emit further actions.
  enum class State { Pending, Delivered, Cancelled };

  SessionContext ctx_;
  // Per-broadcast algorithm instance. Constructed in the ctor via the engine's registered
  // AlgorithmFamily for this mode; mutates its state per event over the session's lifetime.
  std::unique_ptr<proto::BroadcastAlgorithm> algorithm_;
  BroadcastMeta meta_;

  // Per-session set of peers we've already emitted PeerUpsert for. The PeerMap itself lives
  // in OverlayBroadcasts (engine), so PeerId is stable across all sessions in this overlay.
  std::unordered_set<proto::PeerId> registered_peers_;
  SourceState source_state_;
  PayloadState payload_;
  PieceTracker pieces_;

  td::uint32 deadline_ = 0;  // unix ts; gc evicts when now > deadline
  td::Timestamp started_at_;
  std::deque<SessionEvent> pending_events_;
  State lifecycle_ = State::Pending;
  bool draining_ = false;
  // Deliver action is deferred to the end of run_actions so a single algorithm step yielding
  // multiple actions (e.g. Send + Deliver) doesn't interleave the body delivery with sends.
  bool pending_deliver_ = false;
  td::uint64 next_alarm_token_ = 1;
  std::optional<td::uint64> scheduled_alarm_token_;
  td::Timestamp scheduled_alarm_;

  OverlayBroadcastSession(SessionContext ctx, OverlayBroadcastOptions opts, BroadcastMeta meta);

  // Translate an algorithm-level message to wire bytes: resolve Piece via the encoder,
  // attach the appropriate signature, then serialize via the wire.
  td::actor::Task<td::BufferSlice> encode(broadcast::algorithm::Message message);

  // === Pure state predicates ======================================================================

  std::optional<proto::PieceId> duplicate_piece_id(const IncomingMessage &incoming) const;
  td::Result<WireMessage> to_wire_message(broadcast::algorithm::Message message);
  bool has_body() const;
  bool cancelled() const {
    return lifecycle_ == State::Cancelled;
  }

  // === Pure mutators (state-only) =================================================================

  td::Status accept_piece(td::fec::Symbol &incoming_symbol, adnl::AdnlNodeIdShort sender);
  // Single source-validation seat: meta is already on the session from construction, so all
  // adoption needs to do is install the validated source and finalize bookkeeping.
  void adopt_source(BroadcastSource source, BroadcastCheckResult check);
  td::Result<bool> try_finish_body_payload();

  void init_peers(std::vector<BroadcastPeerInfo> peers);
  // Look up the sender's `PeerId`, and on first encounter register it in the algorithm
  // with env's view (neighbour/persistent/score). Without this, late-arriving
  // senders that aren't in the initial snapshot would be permanently
  // misclassified as non-neighbour, non-persistent.
  std::optional<proto::PeerId> register_sender(adnl::AdnlNodeIdShort src,
                                               std::optional<BroadcastPeerInfo> info = std::nullopt);
  td::actor::Task<> drain(std::shared_ptr<OverlayBroadcastSession> self);
  td::actor::Task<> handle_event(SessionEvent &event);
  td::actor::Task<> handle(SessionEvent::Receive &receive);
  td::actor::Task<> handle(SessionEvent::Publish &publish);
  td::actor::Task<> handle(SessionEvent::Timer &);
  td::actor::Task<> handle(SessionEvent::PeerUpsert &);
  td::actor::Task<> handle(SessionEvent::PeerRemove &);
  void cancel();

  // Step the algorithm with `event`, then run resulting actions.
  td::actor::Task<> dispatch_event(proto::Event event);
  td::actor::Task<> run_actions(proto::Actions actions);
  void schedule_algorithm_alarm();

  // Action handlers. Deliver only flips pending_deliver_; the actual deliver runs once at
  // the end of run_actions (see field comment).
  td::actor::Task<> handle_action(const proto::act::Send &send);
  td::actor::Task<> handle_action(const proto::act::Deliver &deliver);
  td::actor::Task<> handle_action(const proto::act::PeerFeedback &feedback);
  td::actor::Task<> handle_deliver();

  // Receive sub-flows.
  td::Status check_fixed_source_match(const IncomingMessage &incoming) const;
  bool can_skip_broadcast_signature_check(const IncomingMessage &incoming) const;
  td::actor::Task<> ensure_validated(adnl::AdnlNodeIdShort src, IncomingMessage &incoming);
  td::Status mark_validated(IncomingMessage &incoming, BroadcastCheckResult check_result);
  td::actor::Task<> receive_control(IncomingMessage &incoming, adnl::AdnlNodeIdShort src, proto::PeerId peer_id);
  td::actor::Task<> receive_payload(IncomingMessage &incoming, adnl::AdnlNodeIdShort src, proto::PeerId peer_id);
  td::actor::Task<> receive_duplicate_payload(proto::PieceId piece_id, adnl::AdnlNodeIdShort src,
                                              proto::PeerId peer_id);
};

td::StringBuilder &operator<<(td::StringBuilder &sb, const OverlayBroadcastSession &session);

td::Status check_incoming_signature(const IncomingMessage &incoming, Env *env, adnl::AdnlNodeIdShort src_peer_id);

}  // namespace overlay
}  // namespace ton
