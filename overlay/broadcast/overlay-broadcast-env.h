/*
    This file is part of TON Blockchain Library.

    TON Blockchain Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#pragma once

#include <optional>
#include <utility>
#include <vector>

#include "adnl/adnl-node-id.hpp"
#include "keys/keys.hpp"
#include "overlay/broadcast/wire.h"
#include "overlay/overlays.h"
#include "td/actor/coro_task.h"
#include "td/utils/Time.h"
#include "td/utils/buffer.h"

namespace ton {
namespace overlay {

// Per design.md: PeerInfo is the engine's view of a peer — what `Env::peers()`
// returns. score / neighbour / persistent are read by the algorithm; id is how the engine
// addresses the peer when emitting Send actions.
struct BroadcastPeerInfo {
  adnl::AdnlNodeIdShort id;
  double score = 0.0;
  bool neighbour = false;
  bool persistent = false;
};

// The contract between the broadcast engine and the host (in TON: OverlayImpl).
// It groups the host-owned state and services the engine cannot own itself.
class Env {
 public:
  virtual ~Env() = default;

  // Identity.
  virtual adnl::AdnlNodeIdShort local_id() const = 0;

  // Peers — single source of truth. `BroadcastPeerInfo::neighbour` distinguishes
  // overlay neighbours; `persistent` flags persistent (twostep-eligible) nodes.
  virtual std::vector<BroadcastPeerInfo> peers() = 0;
  // Single-peer lookup. Returns nullopt if the peer is unknown to the host (or
  // not currently alive). Used by the session to enrich `state.peers` with the
  // host's view of `score`/`neighbour`/`persistent` when a sender first appears.
  virtual std::optional<BroadcastPeerInfo> peer_info(adnl::AdnlNodeIdShort peer) = 0;
  virtual void update_peer_score(adnl::AdnlNodeIdShort peer, double delta) = 0;

  // App validation. Source precheck runs before the body is available. It is
  // called before and after signature verification because app limits may change
  // while signature verification is running. Decoded-body verification runs only
  // when the signed precheck returned NeedCheck.
  virtual td::actor::Task<BroadcastCheckResult> precheck_source(const BroadcastSource &source,
                                                                const BroadcastMeta &meta, adnl::AdnlNodeIdShort from,
                                                                bool signature_checked) = 0;
  virtual td::actor::Task<> verify_decoded_body(BroadcastSource source, td::BufferSlice body) = 0;

  // Delivery to the application.
  virtual void deliver(PublicKeyHash sender, td::BufferSlice body, td::BufferSlice extra) = 0;

  // Network — forward a wire-level TL blob to a peer over ADNL.
  virtual void send(adnl::AdnlNodeIdShort dst, td::BufferSlice wire) = 0;

  // Crypto — keyring sign. Returns (signature, public_key) once the keyring resolves.
  virtual td::actor::StartedTask<std::pair<td::BufferSlice, PublicKey>> sign(PublicKeyHash key_hash,
                                                                             td::BufferSlice to_sign) = 0;

  // Crypto — verify a signature on a message claimed from `from`. The host owns the
  // encryptor cache and may apply peer-banning policy on failure. (One method beyond
  // design's strict 8: signature verify is fundamental but can't reduce to sign because
  // peer-banning is host policy.)
  virtual td::Status verify_signature(PublicKey public_key, td::Slice message, td::Slice signature,
                                      adnl::AdnlNodeIdShort from) = 0;

  // Schedule a Timer event to dispatch on this broadcast at `alarm`. `token` lets
  // the engine ignore stale host callbacks after the algorithm's alarm changes.
  virtual void schedule_dispatch_timer(td::Bits256 broadcast_id, td::Timestamp alarm, td::uint64 token) = 0;
};

}  // namespace overlay
}  // namespace ton
