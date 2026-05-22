/*
    This file is part of TON Blockchain Library.

    TON Blockchain Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/

#include "common/checksum.h"
#include "crypto/common/bitstring.h"
#include "td/utils/ThreadSafeCounter.h"
#include "td/utils/overloaded.h"
#include "td/utils/port/Clocks.h"

#include "overlay-broadcast-session.h"

namespace ton {
namespace overlay {

namespace {

constexpr proto::PeerId kSelfPeerId = 1u << 31;
constexpr proto::PeerId kFirstPeerId = 2;

proto::BroadcastId protocol_broadcast_id(const Overlay::BroadcastHash &broadcast_id) {
  auto bytes = broadcast_id.as_slice().ubegin();
  td::uint64 result = 0;
  for (size_t i = 0; i < sizeof(result); i++) {
    result = (result << 8) | bytes[i];
  }
  return result;
}

bool has_fixed_source(const BroadcastInfo &info) {
  return !(info.common.flags & Overlays::BroadcastFlagAnySender());
}

td::uint32 deadline_for(const BroadcastMeta &meta, const broadcast::BroadcastSessionConfig &config) {
  return meta.info.common.date + config.ttl_seconds;
}

std::pair<proto::Message, const char *> control_dispatch(const WireMessage &content) {
  return std::visit(
      td::overloaded([](const Have &) { return std::pair{proto::Message{proto::msg::Have{}}, "RECV_HAVE"}; },
                     [](const Request &) { return std::pair{proto::Message{proto::msg::Request{}}, "RECV_REQUEST"}; },
                     [](const Cancel &) { return std::pair{proto::Message{proto::msg::Cancel{}}, "RECV_CANCEL"}; },
                     [](const td::fec::Symbol &) -> std::pair<proto::Message, const char *> { UNREACHABLE(); }),
      content);
}

}  // namespace

// === PeerMap ======================================================================================

std::optional<PeerMap::Entry> PeerMap::peer_id_for(const adnl::AdnlNodeIdShort &peer) {
  if (auto peer_id = known_peer_id(peer)) {
    return Entry{*peer_id, false};
  }
  if (peers_.size() >= kMaxPeers) {
    return std::nullopt;
  }
  auto next_id = static_cast<proto::PeerId>(peers_.size() + kFirstPeerId);
  CHECK(peer_to_id_.emplace(peer, next_id).second);
  peers_.push_back(peer);
  return Entry{next_id, true};
}

bool PeerMap::can_assign_peer_id(const adnl::AdnlNodeIdShort &peer) const {
  return known_peer_id(peer).has_value() || peers_.size() < kMaxPeers;
}

std::optional<proto::PeerId> PeerMap::known_peer_id(const adnl::AdnlNodeIdShort &peer) const {
  auto it = peer_to_id_.find(peer);
  if (it == peer_to_id_.end()) {
    return std::nullopt;
  }
  return it->second;
}

const adnl::AdnlNodeIdShort *PeerMap::peer_for_id(proto::PeerId peer) const {
  if (peer < kFirstPeerId) {
    return nullptr;
  }
  auto index = static_cast<size_t>(peer - kFirstPeerId);
  return index < peers_.size() ? &peers_[index] : nullptr;
}

// === PieceTracker =================================================================================

void PieceTracker::set_mode(const BroadcastMode &mode) {
  enabled_ = std::holds_alternative<mode::OptimumP2P>(mode) || std::holds_alternative<mode::TwostepFec>(mode);
}

bool PieceTracker::has_duplicate(proto::PieceId seqno) const {
  if (!enabled_) {
    return false;
  }
  return seen_pieces_.count(seqno) != 0;
}

void PieceTracker::record(proto::PieceId seqno, adnl::AdnlNodeIdShort sender) {
  if (!enabled_) {
    return;
  }
  seen_pieces_.insert(seqno);
  append_unique(senders_, sender);
}

size_t PieceTracker::seen_piece_count() const {
  return seen_pieces_.size();
}

size_t PieceTracker::unique_sender_count() const {
  return senders_.size();
}

// === PayloadState =================================================================================

bool PayloadState::has_body() const {
  return body_.has_value();
}

void PayloadState::set_body(td::BufferSlice body, std::unique_ptr<td::fec::Encoder> encoder) {
  CHECK(encoder != nullptr);
  body_ = std::move(body);
  encoder_ = std::move(encoder);
  decoder_.reset();
}

const td::BufferSlice &PayloadState::body() const {
  CHECK(body_.has_value());
  return *body_;
}

td::Result<td::fec::Symbol> PayloadState::make_piece(proto::PieceId piece_id) {
  if (encoder_ != nullptr) {
    return encoder_->gen_symbol(piece_id);
  }
  if (decoder_ != nullptr) {
    return decoder_->gen_symbol(piece_id);
  }
  return td::Status::Error(ErrorCode::notready, "broadcast payload store has no symbol source");
}

td::Status PayloadState::ensure_decoder(std::unique_ptr<td::fec::Decoder> decoder) {
  if (body_.has_value() || decoder_ != nullptr) {
    return td::Status::OK();
  }
  decoder_ = std::move(decoder);
  return td::Status::OK();
}

td::Status PayloadState::accept_piece(td::fec::Symbol &symbol) {
  if (decoder_ == nullptr) {
    return td::Status::Error(ErrorCode::protoviolation, "received piece without a payload store");
  }
  TRY_STATUS(decoder_->add_symbol({symbol.id, std::move(symbol.data)}));
  return td::Status::OK();
}

td::Result<std::optional<td::BufferSlice>> PayloadState::try_decode_body(const Overlay::BroadcastDataHash &data_hash) {
  if (body_.has_value() || decoder_ == nullptr) {
    return std::optional<td::BufferSlice>{};
  }
  TRY_RESULT(decode_result, decoder_->try_decode_v2(false));
  if (!decode_result.is_ready()) {
    return std::optional<td::BufferSlice>{};
  }
  auto decoded = std::move(decode_result.data.data);
  if (td::sha256_bits256(decoded.as_slice()) != data_hash) {
    return td::Status::Error(ErrorCode::protoviolation, "broadcast data hash mismatch");
  }
  return std::make_optional(std::move(decoded));
}

// === SourceState ==================================================================================

SourceState::Validated *SourceState::validated() {
  return validated_ ? &*validated_ : nullptr;
}

const SourceState::Validated *SourceState::validated() const {
  return validated_ ? &*validated_ : nullptr;
}

void SourceState::adopt(BroadcastSource source, BroadcastCheckResult check) {
  CHECK(!validated_);
  validated_ = Validated{std::move(source), check, td::BufferSlice{}};
}

// === Logic entry points ===========================================================================

OverlayBroadcastSession::OverlayBroadcastSession(SessionContext ctx, OverlayBroadcastOptions opts, BroadcastMeta meta)
    : ctx_(ctx), meta_(std::move(meta)), started_at_(td::Timestamp::now()) {
  pieces_.set_mode(meta_.info.mode);

  // Resolve origin peer id eagerly so the algorithm starts with `state.origin_peer`
  // already pointing at the publisher (Twostep relies on it).
  auto origin_peer_id = kSelfPeerId;
  if (meta_.info.common.src_adnl_id != ctx_.env.local_id()) {
    auto mapped_origin = ctx_.peer_map.peer_id_for(meta_.info.common.src_adnl_id);
    CHECK(mapped_origin);
    origin_peer_id = mapped_origin->id;
  }

  proto::BroadcastInit init{
      .id = protocol_broadcast_id(meta_.broadcast_id), .self = kSelfPeerId, .origin = origin_peer_id};
  if (auto *opt = std::get_if<mode::OptimumP2P>(&meta_.info.mode)) {
    init.required_pieces = opt->required_pieces;
  } else if (auto *fec = std::get_if<mode::TwostepFec>(&meta_.info.mode)) {
    init.required_pieces = fec->required_pieces;
  }
  // Every BroadcastMode must have a registered AlgorithmFamily — OverlayBroadcasts is wired by
  // OverlayImpl with one family per mode at startup. A missing family is a programming error.
  CHECK(ctx_.family != nullptr);
  CHECK(ctx_.family->wire != nullptr);
  CHECK(ctx_.family->storage != nullptr);
  algorithm_ = ctx_.family->make_algorithm(init);
  CHECK(algorithm_ != nullptr);
  deadline_ = deadline_for(meta_, ctx_.family->session);

  // Seed the initial peer set (the publisher is already in engine peer_map; if present in env's
  // peer list it'll be re-counted but PeerUpsert is idempotent by design — algorithm dedupes
  // via state).
  auto known = ctx_.env.peers();
  if (auto limit = ctx_.family->session.initial_peer_limit; limit > 0 && known.size() > limit) {
    known.resize(limit);
  }
  init_peers(std::move(known));
}

std::shared_ptr<OverlayBroadcastSession> OverlayBroadcastSession::make(SessionContext ctx, OverlayBroadcastOptions opts,
                                                                       BroadcastMeta meta) {
  return std::shared_ptr<OverlayBroadcastSession>(new OverlayBroadcastSession(ctx, opts, std::move(meta)));
}

void OverlayBroadcastSession::init_peers(std::vector<BroadcastPeerInfo> peers) {
  for (auto &peer : peers) {
    if (!register_sender(peer.id, std::move(peer))) {
      return;
    }
  }
}

std::optional<proto::PeerId> OverlayBroadcastSession::register_sender(adnl::AdnlNodeIdShort src,
                                                                      std::optional<BroadcastPeerInfo> info) {
  if (src == ctx_.env.local_id()) {
    return kSelfPeerId;
  }
  auto mapped_peer = ctx_.peer_map.peer_id_for(src);
  if (!mapped_peer) {
    return std::nullopt;
  }
  auto peer_id = mapped_peer->id;
  if (!registered_peers_.insert(peer_id).second) {
    return peer_id;
  }
  proto::Peer entry{.id = peer_id};
  if (!info) {
    info = ctx_.env.peer_info(src);
  }
  if (info) {
    entry.score = info->score;
    entry.neighbour = info->neighbour;
    entry.persistent = info->persistent;
  }
  algorithm_->set_now(td::Timestamp::now_cached());
  algorithm_->handle_event(proto::evt::PeerUpsert{entry});
  return peer_id;
}

void OverlayBroadcastSession::push_event(SessionEvent event) {
  if (cancelled()) {
    return;
  }
  pending_events_.push_back(std::move(event));
  if (draining_) {
    return;
  }
  draining_ = true;
  drain(shared_from_this()).start_immediate().detach();
}

void OverlayBroadcastSession::set_source_body(const BroadcastMode &mode, td::BufferSlice body) {
  CHECK(mode.index() == meta_.info.mode.index());
  payload_.set_body(body.clone(), ctx_.family->storage->for_sourcing(meta_.info, std::move(body)));
}

const Overlay::BroadcastHash &OverlayBroadcastSession::broadcast_id() const {
  return meta_.broadcast_id;
}

bool OverlayBroadcastSession::is_expired(td::uint32 now) const {
  return deadline_ <= now;
}

bool OverlayBroadcastSession::is_delivered() const {
  return lifecycle_ == State::Delivered;
}

double OverlayBroadcastSession::elapsed_since_start() const {
  return td::Timestamp::now().at() - started_at_.at();
}

void OverlayBroadcastSession::mark_erased() {
  lifecycle_ = State::Cancelled;
}

td::actor::Task<> OverlayBroadcastSession::drain(std::shared_ptr<OverlayBroadcastSession> self) {
  CHECK(self.get() == this);
  while (!cancelled() && !pending_events_.empty()) {
    auto event = std::move(pending_events_.front());
    pending_events_.pop_front();
    auto status = co_await handle_event(event).wrap();
    if (status.is_error() && status.error().code() != ErrorCode::notready) {
      VLOG(OVERLAY_BROADCAST_WARNING) << "failed to process new broadcast session event " << *this << ": "
                                      << status.error();
    }
  }
  // If nothing in this drain produced a validated source, the session has accumulated no
  // useful state; erase it so the next message for the same broadcast_id starts fresh.
  if (source_state_.validated() == nullptr) {
    cancel();
  }
  draining_ = false;
  co_return {};
}

td::actor::Task<> OverlayBroadcastSession::handle_event(SessionEvent &event) {
  co_return co_await std::visit([this](auto &e) { return handle(e); }, event.value);
}

td::actor::Task<> OverlayBroadcastSession::handle(SessionEvent::Receive &receive) {
  auto &incoming = receive.incoming;
  if (receive.src_peer_id != ctx_.env.local_id() && !ctx_.peer_map.can_assign_peer_id(receive.src_peer_id)) {
    co_return td::Status::Error(ErrorCode::notready, "broadcast peer limit reached");
  }
  if (auto duplicate = duplicate_piece_id(incoming)) {
    auto peer_id = register_sender(receive.src_peer_id);
    if (!peer_id) {
      co_return td::Status::Error(ErrorCode::notready, "broadcast peer limit reached");
    }
    co_return co_await receive_duplicate_payload(*duplicate, receive.src_peer_id, *peer_id);
  }
  co_await ensure_validated(receive.src_peer_id, incoming);
  auto peer_id = register_sender(receive.src_peer_id);
  if (!peer_id) {
    co_return td::Status::Error(ErrorCode::notready, "broadcast peer limit reached");
  }
  if (std::holds_alternative<td::fec::Symbol>(incoming.content)) {
    co_return co_await receive_payload(incoming, receive.src_peer_id, *peer_id);
  }
  co_return co_await receive_control(incoming, receive.src_peer_id, *peer_id);
}

td::actor::Task<> OverlayBroadcastSession::handle(SessionEvent::Publish &publish) {
  // No publish-time signing: encode() signs whatever message the algorithm actually emits
  // and caches the result if it's broadcast-level. source.public_key fills in on first sign.
  // Publish may also reach a session that already validated the same source via a prior
  // Receive (loopback race); in that case skip re-adoption.
  if (source_state_.validated() == nullptr) {
    adopt_source(std::move(publish.source), BroadcastCheckResult::Allowed);
  }
  VLOG(OVERLAY_BROADCAST_INFO) << "new broadcast START sender " << *this;
  co_return co_await dispatch_event(proto::evt::Publish{});
}

td::actor::Task<td::BufferSlice> OverlayBroadcastSession::encode(proto::Message message) {
  WireMessage wire_message = CO_TRY(to_wire_message(std::move(message)));
  const auto *wire = ctx_.family->wire.get();
  const auto &info = meta_.info;
  auto to_sign = CO_TRY(wire->to_sign(info, wire_message));
  auto *validated = source_state_.validated();
  CHECK(validated != nullptr);
  td::BufferSlice sig_bytes;
  if (to_sign.scope == SignatureScope::Broadcast && !validated->broadcast_signature.empty()) {
    sig_bytes = validated->broadcast_signature.clone();
  } else {
    auto [sig, pk] = co_await ctx_.env.sign(validated->source.key_hash, std::move(to_sign.bytes));
    sig_bytes = std::move(sig);
    validated->source.public_key = std::move(pk);
    if (to_sign.scope == SignatureScope::Broadcast) {
      validated->broadcast_signature = sig_bytes.clone();  // First broadcast-level sign primes the cache.
    }
  }
  co_return CO_TRY(wire->serialize(info, validated->source, std::move(wire_message), sig_bytes.as_slice()));
}

td::Result<WireMessage> OverlayBroadcastSession::to_wire_message(proto::Message message) {
  return std::visit(
      td::overloaded(
          [](const proto::msg::Have &) -> td::Result<WireMessage> { return WireMessage{Have{}}; },
          [](const proto::msg::Request &) -> td::Result<WireMessage> { return WireMessage{Request{}}; },
          [](const proto::msg::Cancel &) -> td::Result<WireMessage> { return WireMessage{Cancel{}}; },
          [&](const proto::msg::Piece &piece) -> td::Result<WireMessage> {
            TRY_RESULT(symbol, payload_.make_piece(piece.id));
            return WireMessage{std::move(symbol)};
          },
          [](const proto::msg::sim::HavePieces &) -> td::Result<WireMessage> {
            return td::Status::Error(ErrorCode::protoviolation, "simulator-only HavePieces cannot be serialized");
          },
          [](const proto::msg::sim::RequestPieces &) -> td::Result<WireMessage> {
            return td::Status::Error(ErrorCode::protoviolation, "simulator-only RequestPieces cannot be serialized");
          },
          [](const proto::msg::sim::RequestAnyPieces &) -> td::Result<WireMessage> {
            return td::Status::Error(ErrorCode::protoviolation, "simulator-only RequestAnyPieces cannot be serialized");
          },
          [](const proto::msg::sim::HaveBucket &) -> td::Result<WireMessage> {
            return td::Status::Error(ErrorCode::protoviolation, "simulator-only HaveBucket cannot be serialized");
          },
          [](const proto::msg::sim::SubscribeBucket &) -> td::Result<WireMessage> {
            return td::Status::Error(ErrorCode::protoviolation, "simulator-only SubscribeBucket cannot be serialized");
          },
          [](const proto::msg::sim::UnsubscribeBucket &) -> td::Result<WireMessage> {
            return td::Status::Error(ErrorCode::protoviolation,
                                     "simulator-only UnsubscribeBucket cannot be serialized");
          }),
      message);
}

td::actor::Task<> OverlayBroadcastSession::handle(SessionEvent::PeerUpsert &upsert) {
  // Engine already updated peer_map_ and any BroadcastShared. We just forward to the algorithm
  // as a behavioural notification; the algorithm decides whether to emit any actions in
  // response (e.g. announce Have to the fresh peer).
  registered_peers_.insert(upsert.peer.id);
  co_return co_await dispatch_event(proto::evt::PeerUpsert{upsert.peer});
}

td::actor::Task<> OverlayBroadcastSession::handle(SessionEvent::PeerRemove &remove) {
  registered_peers_.erase(remove.peer);
  co_return co_await dispatch_event(proto::evt::PeerRemove{remove.peer});
}

td::actor::Task<> OverlayBroadcastSession::handle(SessionEvent::Timer &timer) {
  if (!scheduled_alarm_token_ || *scheduled_alarm_token_ != timer.token) {
    co_return {};
  }
  auto now = td::Timestamp::now_cached();
  const auto &algorithm = *algorithm_;
  auto alarm = algorithm.alarm();
  if (!alarm || !(alarm == scheduled_alarm_) || !alarm.is_in_past(now)) {
    scheduled_alarm_token_.reset();
    scheduled_alarm_ = td::Timestamp::never();
    schedule_algorithm_alarm();
    co_return {};
  }
  scheduled_alarm_token_.reset();
  scheduled_alarm_ = td::Timestamp::never();
  VLOG(OVERLAY_BROADCAST_INFO) << "new broadcast TIMER " << *this;
  co_return co_await dispatch_event(proto::evt::Timer{});
}

// === Receive sub-flows ============================================================================

td::Status OverlayBroadcastSession::check_fixed_source_match(const IncomingMessage &incoming) const {
  auto *validated = source_state_.validated();
  if (has_fixed_source(meta_.info) && validated != nullptr && validated->source.key_hash != incoming.source.key_hash) {
    return td::Status::Error(ErrorCode::protoviolation, "broadcast source mismatch");
  }
  return td::Status::OK();
}

bool OverlayBroadcastSession::can_skip_broadcast_signature_check(const IncomingMessage &incoming) const {
  return source_state_.validated() != nullptr && incoming.signed_payload.scope == SignatureScope::Broadcast;
}

td::actor::Task<> OverlayBroadcastSession::ensure_validated(adnl::AdnlNodeIdShort src, IncomingMessage &incoming) {
  CO_TRY(check_fixed_source_match(incoming));
  if (can_skip_broadcast_signature_check(incoming)) {
    co_return {};
  }
  auto check = co_await ctx_.env.precheck_source(incoming.source, incoming.meta, src, false);
  if (check == BroadcastCheckResult::Forbidden) {
    co_return td::Status::Error(ErrorCode::error, "broadcast is forbidden");
  }
  CO_TRY(check_incoming_signature(incoming, &ctx_.env, src));
  check = co_await ctx_.env.precheck_source(incoming.source, incoming.meta, src, true);
  if (check == BroadcastCheckResult::Forbidden) {
    co_return td::Status::Error(ErrorCode::error, "broadcast is forbidden");
  }
  CO_TRY(mark_validated(incoming, check));
  co_return {};
}

td::Status OverlayBroadcastSession::mark_validated(IncomingMessage &incoming, BroadcastCheckResult check) {
  TRY_STATUS(check_fixed_source_match(incoming));
  if (source_state_.validated() == nullptr) {
    adopt_source(incoming.source.clone(), check);
    // Cache the broadcast-level signature for re-emit on every outgoing piece.
    if (incoming.signed_payload.scope == SignatureScope::Broadcast) {
      source_state_.validated()->broadcast_signature = incoming.signature.clone();
    }
  }
  TRY_RESULT(decoder, ctx_.family->storage->for_receiving(meta_.info));
  return payload_.ensure_decoder(std::move(decoder));
}

td::actor::Task<> OverlayBroadcastSession::receive_control(IncomingMessage &incoming, adnl::AdnlNodeIdShort src,
                                                           proto::PeerId peer_id) {
  auto [msg, tag] = control_dispatch(incoming.content);
  VLOG(OVERLAY_BROADCAST_INFO) << "broadcast " << tag << " " << *this << " from=" << src;
  co_return co_await dispatch_event(proto::evt::Receive{peer_id, std::move(msg)});
}

td::actor::Task<> OverlayBroadcastSession::receive_payload(IncomingMessage &incoming, adnl::AdnlNodeIdShort src,
                                                           proto::PeerId peer_id) {
  auto &symbol = std::get<td::fec::Symbol>(incoming.content);
  auto piece_id = symbol.id;
  CO_TRY(accept_piece(symbol, src));
  VLOG(OVERLAY_BROADCAST_INFO) << "new broadcast RECV_PIECE " << *this << " from=" << src;
  co_await dispatch_event(proto::evt::Receive{peer_id, proto::Message{proto::msg::Piece{piece_id}}});
  if (cancelled()) {
    co_return {};
  }
  bool newly_ready = CO_TRY(try_finish_body_payload());
  if (newly_ready && !cancelled()) {
    co_await dispatch_event(proto::evt::BodyReady{});
  }
  co_return {};
}

td::actor::Task<> OverlayBroadcastSession::receive_duplicate_payload(proto::PieceId piece_id, adnl::AdnlNodeIdShort src,
                                                                     proto::PeerId peer_id) {
  VLOG(OVERLAY_BROADCAST_INFO) << "new broadcast RECV_DUPLICATE_PIECE " << *this << " from=" << src;
  co_return co_await dispatch_event(
      proto::evt::Receive{peer_id, proto::Message{proto::msg::Piece{.id = piece_id, .duplicate = true}}});
}

// === Algorithm dispatch & action handlers =========================================================

td::actor::Task<> OverlayBroadcastSession::dispatch_event(proto::Event event) {
  proto::Actions actions;
  {
    TD_PERF_COUNTER(overlay_broadcast_algorithm_step);
    algorithm_->set_now(td::Timestamp::now_cached());
    actions = algorithm_->handle_event(std::move(event));
  }
  co_await run_actions(std::move(actions));
  if (!cancelled()) {
    schedule_algorithm_alarm();
  }
  co_return {};
}

td::actor::Task<> OverlayBroadcastSession::run_actions(proto::Actions actions) {
  pending_deliver_ = false;
  for (const auto &action : actions) {
    if (cancelled()) {
      co_return {};
    }
    co_await std::visit([this](const auto &a) { return handle_action(a); }, action);
  }
  if (std::exchange(pending_deliver_, false)) {
    co_await handle_deliver();
  }
  co_return {};
}

td::actor::Task<> OverlayBroadcastSession::handle_action(const proto::act::Send &send) {
  auto dst = ctx_.peer_map.peer_for_id(send.peer);
  if (dst == nullptr) {
    co_return {};
  }
  auto encoded = co_await encode(send.message).wrap();
  if (encoded.is_error()) {
    auto error = encoded.move_as_error();
    if (error.code() != ErrorCode::notready) {
      VLOG(OVERLAY_BROADCAST_WARNING) << "failed to encode new broadcast action: " << error;
    }
    co_return {};
  }
  ctx_.env.send(*dst, encoded.move_as_ok());
  co_return {};
}

td::actor::Task<> OverlayBroadcastSession::handle_action(const proto::act::Deliver &) {
  pending_deliver_ = true;
  co_return {};
}

td::actor::Task<> OverlayBroadcastSession::handle_action(const proto::act::PeerFeedback &feedback) {
  if (auto *peer = ctx_.peer_map.peer_for_id(feedback.peer); peer != nullptr) {
    ctx_.env.update_peer_score(*peer, feedback.delta);
  }
  co_return {};
}

void OverlayBroadcastSession::schedule_algorithm_alarm() {
  const auto &algorithm = *algorithm_;
  auto alarm = algorithm.alarm();
  if (!alarm) {
    scheduled_alarm_token_.reset();
    scheduled_alarm_ = td::Timestamp::never();
    return;
  }
  if (scheduled_alarm_ == alarm) {
    return;
  }
  auto token = next_alarm_token_++;
  scheduled_alarm_token_ = token;
  scheduled_alarm_ = alarm;
  ctx_.env.schedule_dispatch_timer(meta_.broadcast_id, alarm, token);
}

td::actor::Task<> OverlayBroadcastSession::handle_deliver() {
  if (cancelled() || !payload_.has_body() || lifecycle_ == State::Delivered) {
    co_return {};
  }
  auto *validated = source_state_.validated();
  CHECK(validated != nullptr);
  auto broadcast_id = meta_.broadcast_id;
  auto src = validated->source.key_hash;
  auto check = validated->check_result;
  auto data = payload_.body().clone();
  auto extra = std::move(meta_.info.common.extra);
  lifecycle_ = State::Delivered;
  ctx_.owner.mark_delivered(broadcast_id);
  VLOG(OVERLAY_BROADCAST_INFO) << "new broadcast FINISH " << *this;
  if (check != BroadcastCheckResult::Allowed) {
    co_await ctx_.env.verify_decoded_body(validated->source.clone(), data.clone());
  }
  ctx_.env.deliver(src, std::move(data), std::move(extra));
  co_return {};
}

void OverlayBroadcastSession::cancel() {
  if (cancelled()) {
    return;
  }
  lifecycle_ = State::Cancelled;
  pending_events_.clear();
  ctx_.owner.erase_session(shared_from_this());
}

void OverlayBroadcastSession::adopt_source(BroadcastSource source, BroadcastCheckResult check) {
  source_state_.adopt(std::move(source), check);
  // Origin's peer_id was set at construction; we still want the publisher
  // registered in the algorithm's peer set (with env-resolved info).
  register_sender(meta_.info.common.src_adnl_id);
}

std::optional<proto::PieceId> OverlayBroadcastSession::duplicate_piece_id(const IncomingMessage &incoming) const {
  auto *symbol = std::get_if<td::fec::Symbol>(&incoming.content);
  if (symbol == nullptr) {
    return std::nullopt;  // Control: not a session-level duplicate.
  }
  if (has_body()) {
    return symbol->id;  // Full body already collected; additional pieces add nothing.
  }
  // Multi-piece broadcasts dedupe by seqno; single-piece (Twostep simple,
  // whole-body V2 modes) have only one piece, so has_body above already covers them.
  return pieces_.has_duplicate(symbol->id) ? std::make_optional(symbol->id) : std::nullopt;
}

td::Status OverlayBroadcastSession::accept_piece(td::fec::Symbol &incoming_symbol, adnl::AdnlNodeIdShort sender) {
  auto piece_id = incoming_symbol.id;
  TRY_STATUS(payload_.accept_piece(incoming_symbol));
  pieces_.record(piece_id, sender);
  return td::Status::OK();
}

td::Result<bool> OverlayBroadcastSession::try_finish_body_payload() {
  TRY_RESULT(decoded, payload_.try_decode_body(meta_.info.common.data_hash));
  if (!decoded) {
    return false;
  }
  auto encoder = ctx_.family->storage->for_sourcing(meta_.info, decoded->clone());
  payload_.set_body(std::move(*decoded), std::move(encoder));
  return true;
}

bool OverlayBroadcastSession::has_body() const {
  return payload_.has_body();
}

td::StringBuilder &operator<<(td::StringBuilder &sb, const OverlayBroadcastSession &session) {
  const auto &common = session.meta_.info.common;
  sb << "broadcast_id=" << session.meta_.broadcast_id.to_hex() << " src=" << common.src_adnl_id
     << " data_hash=" << common.data_hash.to_hex() << " data_size=" << common.data_size;
  if (auto required = mode_traits(session.meta_.info.mode).required_pieces; required > 1) {
    sb << " symbols=" << session.pieces_.seen_piece_count() << "/" << required;
  }
  if (session.pieces_.unique_sender_count() != 0) {
    sb << " unique_senders=" << session.pieces_.unique_sender_count();
  }
  return sb;
}

}  // namespace overlay
}  // namespace ton
