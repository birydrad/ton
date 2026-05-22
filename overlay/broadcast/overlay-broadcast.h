/*
    This file is part of TON Blockchain Library.

    TON Blockchain Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/
#pragma once

#include <map>
#include <memory>
#include <queue>
#include <set>
#include <string>

#include "adnl/adnl-node-id.hpp"
#include "adnl/adnl.h"
#include "auto/tl/ton_api.h"
#include "keys/keys.hpp"
#include "overlay/broadcast/algorithm.h"
#include "overlay/broadcast/catalog.h"
#include "overlay/broadcast/overlay-broadcast-env.h"
#include "overlay/broadcast/wire.h"
#include "td/actor/coro_task.h"
#include "td/actor/coro_utils.h"
#include "td/utils/Status.h"
#include "td/utils/buffer.h"
#include "td/utils/int_types.h"

namespace ton {
namespace overlay {

namespace proto = broadcast::algorithm;
using AlgorithmFamily = broadcast::AlgorithmFamily;

class OverlayBroadcastSession;
struct Ctx;
struct IncomingMessage;

// Stable per-overlay AdnlNodeIdShort ↔ algorithm PeerId mapping. Owned by OverlayBroadcasts so
// every session of every algorithm family in this overlay sees the same PeerId for a given
// ADNL identity. PeerId remains valid for the lifetime of the engine.
//
// Insertion is lazy: the first time a session asks for `peer_id_for(adnl)` (either while
// seeding from Env::peers() or on first incoming message from a previously-unknown sender), a
// fresh PeerId is allocated. Reverse lookup `peer_for_id` is used by action handlers when
// sending out wire messages.
class PeerMap {
 public:
  struct Entry {
    proto::PeerId id = 0;
    bool inserted = false;
  };

  std::optional<Entry> peer_id_for(const adnl::AdnlNodeIdShort &peer);
  bool can_assign_peer_id(const adnl::AdnlNodeIdShort &peer) const;
  const adnl::AdnlNodeIdShort *peer_for_id(proto::PeerId peer) const;

 private:
  static constexpr size_t kMaxPeers = 2048;

  std::optional<proto::PeerId> known_peer_id(const adnl::AdnlNodeIdShort &peer) const;

  std::map<adnl::AdnlNodeIdShort, proto::PeerId> peer_to_id_;
  std::vector<adnl::AdnlNodeIdShort> peers_;
};

struct Wire {
  static td::Result<IncomingMessage> parse(tl_object_ptr<ton_api::overlay_broadcastV2Piece> broadcast);
  static td::Result<IncomingMessage> parse(tl_object_ptr<ton_api::overlay_broadcastV2Have> broadcast);
  static td::Result<IncomingMessage> parse(tl_object_ptr<ton_api::overlay_broadcastV2Request> broadcast);
  static td::Result<IncomingMessage> parse(tl_object_ptr<ton_api::overlay_broadcastV2Cancel> broadcast);

  template <class Tl>
  static td::Result<IncomingMessage> parse_message(tl_object_ptr<Tl> broadcast) {
    return parse(std::move(broadcast));
  }
};

class BroadcastSessionOwner {
 public:
  virtual ~BroadcastSessionOwner() = default;
  virtual void erase_session(const std::shared_ptr<OverlayBroadcastSession> &session) = 0;
  virtual void mark_delivered(const Overlay::BroadcastHash &broadcast_id) = 0;
};

struct BroadcastModeRequest {
  std::string algorithm;
  td::uint32 data_size = 0;
  size_t persistent_peer_count = 0;
};

td::Status check_overlay_broadcast_algorithm_name(const std::string &name);
td::Result<BroadcastMode> choose_overlay_broadcast_mode(const BroadcastModeRequest &request);

// Engine-private anti-replay cache. Per design.md: lives inside the engine,
// not on the host. LRU-bounded; entries are inserted when a broadcast is delivered or its
// session is gc'd, and queried before accepting new broadcasts with the same id.
class DeliveredCache {
 public:
  static constexpr size_t kCapacity = 4096;

  bool contains(const td::Bits256 &broadcast_id) const {
    return set_.count(broadcast_id) != 0;
  }

  void insert(const td::Bits256 &broadcast_id) {
    if (!set_.insert(broadcast_id).second) {
      return;
    }
    fifo_.push(broadcast_id);
    if (fifo_.size() > kCapacity) {
      set_.erase(fifo_.front());
      fifo_.pop();
    }
  }

 private:
  std::set<td::Bits256> set_;
  std::queue<td::Bits256> fifo_;
};

class OverlayBroadcasts : private BroadcastSessionOwner {
 public:
  OverlayBroadcasts() = default;
  ~OverlayBroadcasts() = default;

  void configure(OverlayBroadcastOptions opts) {
    opts_ = opts;
  }

  // One-time per-overlay setup. Each AlgorithmFamily binds one BroadcastMode to its
  // (BroadcastShared instance, make_algorithm) pair. The engine fans peer churn to every
  // registered family's shared and uses make_algorithm when a session of that mode is created.
  void configure_algorithms(std::vector<AlgorithmFamily> families);

  // Caller resolves the source and publish mode; the engine resolves the matching profile.
  void send(Env *env, BroadcastSource source, BroadcastMode mode, td::BufferSlice data, td::BufferSlice extra,
            td::uint32 flags);

  // Single entry for every broadcast TL the overlay can receive. Tl deduces
  // from the argument; parses the TL, resolves the session profile, then
  // dispatches the parsed message.
  template <class Tl>
  td::Status process_broadcast(Env *env, adnl::AdnlNodeIdShort src_peer_id, tl_object_ptr<Tl> broadcast) {
    TRY_RESULT(incoming, Wire::parse_message(std::move(broadcast)));
    return process(env, src_peer_id, std::move(incoming));
  }
  // Called by the host when a previously-scheduled timer (via Env::schedule_dispatch_timer)
  // fires; dispatches a Timer event into the matching session.
  void on_timer_fired(td::Bits256 broadcast_id, td::uint64 token);

  // Engine-side fanout for overlay peer churn. Called by OverlayImpl when its peer set changes
  // (add_peer / del_peer). The engine:
  //   1. Updates peer_map_ — single writer, stable PeerId across this overlay.
  //   2. Fans an evt::PeerUpsert / evt::PeerRemove out to every active session.
  // Sessions forward to their algorithm as a notification (Shared is already consistent).
  void on_overlay_peer_added(Env *env, adnl::AdnlNodeIdShort adnl_id);
  void on_overlay_peer_removed(adnl::AdnlNodeIdShort adnl_id);

  void gc();

 private:
  td::Status process(Env *env, adnl::AdnlNodeIdShort src_peer_id, IncomingMessage incoming);
  std::shared_ptr<OverlayBroadcastSession> make_session(Env *env, BroadcastMeta meta);
  void erase_session(const std::shared_ptr<OverlayBroadcastSession> &session) override;
  void mark_delivered(const Overlay::BroadcastHash &broadcast_id) override;

  AlgorithmFamily *find_family(std::string_view key);

  std::map<td::Bits256, std::shared_ptr<OverlayBroadcastSession>> sessions_;
  DeliveredCache delivered_;
  OverlayBroadcastOptions opts_;
  // Stable peer identity for all sessions of this overlay. Lazily populated on first
  // `peer_id_for(adnl)`. Outlives any individual session, so cross-broadcast peer-keyed state
  // (peer scoring, subscribe leases, plumtree mesh) can rely on stable PeerIds.
  PeerMap peer_map_;
  // Registered algorithm families. One per BroadcastMode (e.g. "twostep-push", "twostep-fec",
  // "eager-lazy"). Engine uses family.make_algorithm and fans peer churn through
  // family.shared->on_peer_upsert / on_peer_remove.
  std::vector<AlgorithmFamily> families_;
};

// Static mapping BroadcastMode → algorithm-family key. Update with every new mode.
std::string mode_to_family_key(const BroadcastMode &mode);

}  // namespace overlay
}  // namespace ton
