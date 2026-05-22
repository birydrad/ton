/*
    This file is part of TON Blockchain Library.

    TON Blockchain Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#pragma once

#include <memory>
#include <variant>

#include "adnl/adnl-node-id.hpp"
#include "keys/keys.hpp"
#include "overlay/overlay.h"
#include "overlay/overlays.h"
#include "td/fec/fec.h"
#include "td/utils/buffer.h"
#include "td/utils/int_types.h"

namespace ton {
namespace overlay {

struct Have {};
struct Request {};
struct Cancel {};

using WireMessage = std::variant<td::fec::Symbol, Have, Request, Cancel>;

enum class SignatureScope { Broadcast, Message };

struct SignaturePayload {
  SignatureScope scope = SignatureScope::Message;
  td::BufferSlice bytes;
};

struct BroadcastCommon {
  td::uint32 flags = 0;
  td::uint32 date = 0;
  PublicKeyHash id_source;
  adnl::AdnlNodeIdShort src_adnl_id;
  Overlay::BroadcastDataHash data_hash;
  td::uint32 data_size = 0;
  td::BufferSlice extra;

  BroadcastCommon clone() const {
    return {.flags = flags,
            .date = date,
            .id_source = id_source,
            .src_adnl_id = src_adnl_id,
            .data_hash = data_hash,
            .data_size = data_size,
            .extra = extra.clone()};
  }
};

// Mode = the protocol-specific parameters for a broadcast. Each TL mode has its own struct;
// the variant lets consumers std::visit instead of switching on a tag.
namespace mode {
struct EagerLazy {};
struct Plumtree {};
struct OptimumP2P {
  td::uint32 required_pieces = 1;
};
struct TwostepPush {};
struct TwostepFec {
  td::uint32 required_pieces = 1;
  size_t part_size = 0;
};
}  // namespace mode

using BroadcastMode =
    std::variant<mode::EagerLazy, mode::Plumtree, mode::OptimumP2P, mode::TwostepPush, mode::TwostepFec>;

struct BroadcastModeTraits {
  bool uses_whole_body = false;
  bool signs_pieces = false;
  td::uint32 required_pieces = 1;
};

BroadcastModeTraits mode_traits(const BroadcastMode &mode);

struct BroadcastInfo {
  BroadcastCommon common;
  BroadcastMode mode;

  BroadcastInfo clone() const {
    return {.common = common.clone(), .mode = mode};
  }
};

struct BroadcastMeta {
  Overlay::BroadcastHash broadcast_id;
  BroadcastInfo info;

  BroadcastMeta clone() const {
    return {.broadcast_id = broadcast_id, .info = info.clone()};
  }
};

struct BroadcastSource {
  PublicKey public_key;
  PublicKeyHash key_hash;
  std::shared_ptr<Certificate> certificate;

  BroadcastSource clone() const {
    return {.public_key = public_key, .key_hash = key_hash, .certificate = certificate};
  }
};

struct IncomingMessage {
  BroadcastMeta meta;
  BroadcastSource source;
  td::BufferSlice signature;
  SignaturePayload signed_payload;
  WireMessage content;
};

class BroadcastWire {
 public:
  virtual ~BroadcastWire() = default;

  virtual td::Bits256 compute_broadcast_id(const BroadcastInfo &info) const = 0;

  // Canonical keyring payload for `message`. Per-piece for piece modes (OptimumP2P,
  // TwostepFec); broadcast-level otherwise.
  virtual td::Result<SignaturePayload> to_sign(const BroadcastInfo &info, const WireMessage &message) const = 0;

  virtual td::Result<td::BufferSlice> serialize(const BroadcastInfo &info, const BroadcastSource &source,
                                                WireMessage message, td::Slice signature) const = 0;
};

const std::shared_ptr<const BroadcastWire> &v2_wire();

inline adnl::AdnlNodeIdShort v2_id_src_adnl_id(td::uint32 flags, const adnl::AdnlNodeIdShort &src_adnl_id) {
  return (flags & Overlays::BroadcastFlagAnySender()) ? adnl::AdnlNodeIdShort::zero() : src_adnl_id;
}

}  // namespace overlay
}  // namespace ton
