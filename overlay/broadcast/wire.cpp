/*
    This file is part of TON Blockchain Library.

    TON Blockchain Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/

#include "common/checksum.h"
#include "td/utils/overloaded.h"

#include "overlay-broadcast-session.h"

namespace ton {
namespace overlay {

BroadcastModeTraits mode_traits(const BroadcastMode &mode) {
  return std::visit(
      td::overloaded([](const mode::EagerLazy &) { return BroadcastModeTraits{.uses_whole_body = true}; },
                     [](const mode::Plumtree &) { return BroadcastModeTraits{.uses_whole_body = true}; },
                     [](const mode::OptimumP2P &m) {
                       return BroadcastModeTraits{.signs_pieces = true, .required_pieces = m.required_pieces};
                     },
                     [](const mode::TwostepPush &) { return BroadcastModeTraits{.uses_whole_body = true}; },
                     [](const mode::TwostepFec &m) {
                       return BroadcastModeTraits{.signs_pieces = true, .required_pieces = m.required_pieces};
                     }),
      mode);
}

namespace {

td::BufferSlice v2_to_sign_broadcast(Overlay::BroadcastHash broadcast_id) {
  return create_serialize_tl_object<ton_api::overlay_broadcastV2_toSignBroadcast>(broadcast_id);
}

td::BufferSlice v2_to_sign_piece(Overlay::BroadcastHash broadcast_id, td::uint32 seqno, td::Slice data) {
  return create_serialize_tl_object<ton_api::overlay_broadcastV2_toSignPiece>(
      broadcast_id, static_cast<td::int32>(seqno), td::BufferSlice{data});
}

tl_object_ptr<ton_api::overlay_broadcastV2_Mode> v2_mode_tl(const BroadcastMode &mode) {
  return std::visit(td::overloaded(
                        [](const mode::EagerLazy &) -> tl_object_ptr<ton_api::overlay_broadcastV2_Mode> {
                          return create_tl_object<ton_api::overlay_broadcastV2_modeEagerLazy>();
                        },
                        [](const mode::Plumtree &) -> tl_object_ptr<ton_api::overlay_broadcastV2_Mode> {
                          return create_tl_object<ton_api::overlay_broadcastV2_modePlumtree>();
                        },
                        [](const mode::OptimumP2P &m) -> tl_object_ptr<ton_api::overlay_broadcastV2_Mode> {
                          return create_tl_object<ton_api::overlay_broadcastV2_modeOptimumP2P>(
                              static_cast<td::int32>(m.required_pieces));
                        },
                        [](const mode::TwostepPush &) -> tl_object_ptr<ton_api::overlay_broadcastV2_Mode> {
                          return create_tl_object<ton_api::overlay_broadcastV2_modeTwostepSimple>();
                        },
                        [](const mode::TwostepFec &m) -> tl_object_ptr<ton_api::overlay_broadcastV2_Mode> {
                          return create_tl_object<ton_api::overlay_broadcastV2_modeTwostepFec>(
                              static_cast<td::int32>(m.required_pieces));
                        }),
                    mode);
}

tl_object_ptr<ton_api::overlay_broadcastV2_id> v2_id_tl(const BroadcastInfo &info) {
  return create_tl_object<ton_api::overlay_broadcastV2_id>(
      static_cast<td::int32>(info.common.flags), static_cast<td::int32>(info.common.date),
      info.common.id_source.bits256_value(), info.common.src_adnl_id.bits256_value(), info.common.data_hash,
      static_cast<td::int32>(info.common.data_size), info.common.extra.clone(), v2_mode_tl(info.mode));
}

tl_object_ptr<ton_api::overlay_broadcastV2_source> v2_source_tl(const BroadcastSource &source, td::Slice signature) {
  return create_tl_object<ton_api::overlay_broadcastV2_source>(
      source.public_key.tl(), source.certificate ? source.certificate->tl() : Certificate::empty_tl(),
      td::BufferSlice{signature});
}

Overlay::BroadcastHash v2_broadcast_id(const BroadcastInfo &info) {
  return get_tl_object_sha_bits256(v2_id_tl(info));
}

td::Result<BroadcastMode> parse_v2_mode(ton_api::overlay_broadcastV2_modeEagerLazy &) {
  return BroadcastMode{mode::EagerLazy{}};
}

td::Result<BroadcastMode> parse_v2_mode(ton_api::overlay_broadcastV2_modePlumtree &) {
  return BroadcastMode{mode::Plumtree{}};
}

td::Result<BroadcastMode> parse_v2_mode(ton_api::overlay_broadcastV2_modeOptimumP2P &mode) {
  if (mode.required_pieces_ <= 0) {
    return td::Status::Error(ErrorCode::protoviolation, "non-positive v2 broadcast piece count");
  }
  return BroadcastMode{mode::OptimumP2P{.required_pieces = static_cast<td::uint32>(mode.required_pieces_)}};
}

td::Result<BroadcastMode> parse_v2_mode(ton_api::overlay_broadcastV2_modeTwostepSimple &) {
  return BroadcastMode{mode::TwostepPush{}};
}

td::Result<BroadcastMode> parse_v2_mode(ton_api::overlay_broadcastV2_modeTwostepFec &mode) {
  if (mode.required_pieces_ <= 0) {
    return td::Status::Error(ErrorCode::protoviolation, "non-positive v2 broadcast piece count");
  }
  return BroadcastMode{mode::TwostepFec{.required_pieces = static_cast<td::uint32>(mode.required_pieces_)}};
}

template <class T>
td::Result<BroadcastMode> parse_v2_mode(T &) {
  return td::Status::Error(ErrorCode::protoviolation, "unknown v2 broadcast mode");
}

td::Result<BroadcastMode> parse_v2_mode_ptr(const tl_object_ptr<ton_api::overlay_broadcastV2_Mode> &mode) {
  if (mode == nullptr) {
    return td::Status::Error(ErrorCode::protoviolation, "missing v2 broadcast mode");
  }

  td::Result<BroadcastMode> result;
  if (!ton_api::downcast_call(*mode, [&](auto &value) { result = parse_v2_mode(value); })) {
    return td::Status::Error(ErrorCode::protoviolation, "unknown v2 broadcast mode");
  }
  return result;
}

td::Result<BroadcastMeta> parse_v2_id(const tl_object_ptr<ton_api::overlay_broadcastV2_id> &id) {
  if (id == nullptr) {
    return td::Status::Error(ErrorCode::protoviolation, "missing v2 broadcast id");
  }
  TRY_RESULT(v2_mode, parse_v2_mode_ptr(id->mode_));
  if (id->data_size_ < 0) {
    return td::Status::Error(ErrorCode::protoviolation, "negative v2 broadcast size");
  }
  auto flags = static_cast<td::uint32>(id->flags_);
  auto data_size = static_cast<td::uint32>(id->data_size_);
  PublicKeyHash id_source(id->src_);
  adnl::AdnlNodeIdShort src_adnl_id{id->src_adnl_id_};
  if ((flags & Overlays::BroadcastFlagAnySender()) && (!id_source.is_zero() || !src_adnl_id.is_zero())) {
    return td::Status::Error(ErrorCode::protoviolation, "nonzero source in any-sender v2 broadcast");
  }
  BroadcastInfo info{.common = {.flags = flags,
                                .date = static_cast<td::uint32>(id->date_),
                                .id_source = id_source,
                                .src_adnl_id = src_adnl_id,
                                .data_hash = id->data_hash_,
                                .data_size = data_size,
                                .extra = id->extra_.clone()},
                     .mode = v2_mode};
  return BroadcastMeta{.broadcast_id = v2_broadcast_id(info), .info = std::move(info)};
}

td::Result<BroadcastSource> parse_broadcast_source(const tl_object_ptr<ton_api::PublicKey> &src,
                                                   const tl_object_ptr<ton_api::overlay_Certificate> &certificate) {
  PublicKey public_key(src);
  auto key_hash = PublicKeyHash(public_key.compute_short_id());
  TRY_RESULT(cert, Certificate::create(certificate));
  return BroadcastSource{std::move(public_key), key_hash, std::move(cert)};
}

struct V2Envelope {
  BroadcastMeta meta;
  BroadcastSource source;
  td::BufferSlice signature;
};

td::Result<V2Envelope> parse_v2_envelope(const tl_object_ptr<ton_api::overlay_broadcastV2_id> &id,
                                         const tl_object_ptr<ton_api::overlay_broadcastV2_source> &source) {
  if (source == nullptr) {
    return td::Status::Error(ErrorCode::protoviolation, "missing v2 broadcast source");
  }
  TRY_RESULT(meta, parse_v2_id(id));
  TRY_RESULT(parsed_source, parse_broadcast_source(source->src_, source->certificate_));
  auto id_source =
      (meta.info.common.flags & Overlays::BroadcastFlagAnySender()) ? PublicKeyHash::zero() : parsed_source.key_hash;
  if (meta.info.common.id_source != id_source) {
    return td::Status::Error(ErrorCode::protoviolation, "invalid v2 broadcast source id");
  }
  return V2Envelope{std::move(meta), std::move(parsed_source), source->signature_.clone()};
}

template <class Tl>
td::Result<IncomingMessage> parse_v2_control(const tl_object_ptr<Tl> &broadcast, WireMessage content) {
  TRY_RESULT(envelope, parse_v2_envelope(broadcast->id_, broadcast->source_));
  auto to_sign = v2_to_sign_broadcast(envelope.meta.broadcast_id);
  return IncomingMessage{.meta = std::move(envelope.meta),
                         .source = std::move(envelope.source),
                         .signature = std::move(envelope.signature),
                         .signed_payload = {.scope = SignatureScope::Broadcast, .bytes = std::move(to_sign)},
                         .content = std::move(content)};
}

}  // namespace

td::Status check_incoming_signature(const IncomingMessage &incoming, Env *env, adnl::AdnlNodeIdShort src_peer_id) {
  TD_PERF_COUNTER(check_signature_overlay_broadcast_v2);
  return env->verify_signature(incoming.source.public_key, incoming.signed_payload.bytes, incoming.signature,
                               src_peer_id);
}

td::Result<IncomingMessage> Wire::parse(tl_object_ptr<ton_api::overlay_broadcastV2Piece> piece) {
  if (piece->seqno_ < 0) {
    return td::Status::Error(ErrorCode::protoviolation, "invalid v2 broadcast piece id");
  }
  TRY_RESULT(envelope, parse_v2_envelope(piece->id_, piece->source_));
  auto traits = mode_traits(envelope.meta.info.mode);
  auto seqno = static_cast<td::uint32>(piece->seqno_);
  if (traits.uses_whole_body && seqno != 0) {
    return td::Status::Error(ErrorCode::protoviolation, "invalid whole-body v2 broadcast piece id");
  }
  if (traits.uses_whole_body) {
    if (td::sha256_bits256(piece->data_.as_slice()) != envelope.meta.info.common.data_hash) {
      return td::Status::Error(ErrorCode::protoviolation, "invalid v2 broadcast piece body hash");
    }
  }
  // Whole-body V2 modes are single-piece (broadcast_id+data_hash binds the body, sig over
  // broadcast_id is sufficient). Piece modes sign individually for per-piece authentication.
  auto to_sign = traits.uses_whole_body ? v2_to_sign_broadcast(envelope.meta.broadcast_id)
                                        : v2_to_sign_piece(envelope.meta.broadcast_id, seqno, piece->data_.as_slice());
  WireMessage content = td::fec::Symbol{seqno, std::move(piece->data_)};
  return IncomingMessage{
      .meta = std::move(envelope.meta),
      .source = std::move(envelope.source),
      .signature = std::move(envelope.signature),
      .signed_payload = {.scope = traits.uses_whole_body ? SignatureScope::Broadcast : SignatureScope::Message,
                         .bytes = std::move(to_sign)},
      .content = std::move(content)};
}

td::Result<IncomingMessage> Wire::parse(tl_object_ptr<ton_api::overlay_broadcastV2Have> broadcast) {
  return parse_v2_control(broadcast, Have{});
}

td::Result<IncomingMessage> Wire::parse(tl_object_ptr<ton_api::overlay_broadcastV2Request> broadcast) {
  return parse_v2_control(broadcast, Request{});
}

td::Result<IncomingMessage> Wire::parse(tl_object_ptr<ton_api::overlay_broadcastV2Cancel> broadcast) {
  return parse_v2_control(broadcast, Cancel{});
}

namespace {

class V2Wire final : public BroadcastWire {
 public:
  td::Bits256 compute_broadcast_id(const BroadcastInfo &info) const override {
    return v2_broadcast_id(info);
  }

  td::Result<SignaturePayload> to_sign(const BroadcastInfo &info, const WireMessage &message) const override {
    auto broadcast_id = compute_broadcast_id(info);
    auto traits = mode_traits(info.mode);
    // Whole-body V2 + control: broadcast-level sig (data_hash binds the body).
    // Piece modes: per-piece sig over (broadcast_id, seqno, data).
    if (traits.signs_pieces) {
      if (auto *symbol = std::get_if<td::fec::Symbol>(&message)) {
        return SignaturePayload{.scope = SignatureScope::Message,
                                .bytes = v2_to_sign_piece(broadcast_id, symbol->id, symbol->data.as_slice())};
      }
    }
    return SignaturePayload{.scope = SignatureScope::Broadcast, .bytes = v2_to_sign_broadcast(broadcast_id)};
  }

  td::Result<td::BufferSlice> serialize(const BroadcastInfo &info, const BroadcastSource &source, WireMessage message,
                                        td::Slice signature) const override {
    auto id = v2_id_tl(info);
    auto src = v2_source_tl(source, signature);
    return std::visit(
        td::overloaded(
            [&](const Have &) {
              return create_serialize_tl_object<ton_api::overlay_broadcastV2Have>(std::move(id), std::move(src));
            },
            [&](const Request &) {
              return create_serialize_tl_object<ton_api::overlay_broadcastV2Request>(std::move(id), std::move(src));
            },
            [&](const Cancel &) {
              return create_serialize_tl_object<ton_api::overlay_broadcastV2Cancel>(std::move(id), std::move(src));
            },
            [&](td::fec::Symbol &symbol) {
              return create_serialize_tl_object<ton_api::overlay_broadcastV2Piece>(
                  std::move(id), std::move(src), static_cast<td::int32>(symbol.id), std::move(symbol.data));
            }),
        message);
  }
};

}  // namespace

const std::shared_ptr<const BroadcastWire> &v2_wire() {
  static const auto instance = std::shared_ptr<const BroadcastWire>(std::make_shared<V2Wire>());
  return instance;
}

}  // namespace overlay
}  // namespace ton
