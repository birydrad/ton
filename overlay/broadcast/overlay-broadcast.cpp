/*
    This file is part of TON Blockchain Library.

    TON Blockchain Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.
*/

#include <algorithm>
#include <cctype>

#include "common/checksum.h"
#include "td/utils/ThreadSafeCounter.h"
#include "td/utils/port/Clocks.h"

#include "overlay-broadcast-session.h"

namespace ton {
namespace overlay {

namespace {

struct BroadcastPlanningDefaults {
  td::uint32 v2_rlnc_shard_count = 8;
  td::uint32 min_twostep_fec_data_size = 513;
  size_t min_twostep_fec_peers = 4;
};

constexpr BroadcastPlanningDefaults kPlanningDefaults;

enum class NamedBroadcastAlgorithm { Auto, EagerLazy, Plumtree, OptimumP2P, Twostep, TwostepPush, TwostepFec };

struct PublishSourceIdentity {
  PublicKeyHash id_source;
  adnl::AdnlNodeIdShort src_adnl_id;
};

std::string normalize_algorithm_name(const std::string &name) {
  std::string result;
  result.reserve(name.size());
  for (unsigned char c : name) {
    result.push_back(c == '_' ? '-' : static_cast<char>(std::tolower(c)));
  }
  return result;
}

td::Result<NamedBroadcastAlgorithm> parse_algorithm_name(const std::string &name) {
  auto normalized = normalize_algorithm_name(name);
  if (normalized.empty()) {
    return td::Status::Error("empty broadcast algorithm");
  }
  if (normalized == "auto" || normalized == "default") {
    return NamedBroadcastAlgorithm::Auto;
  }
  if (normalized == "eager-lazy" || normalized == "eagerlazy") {
    return NamedBroadcastAlgorithm::EagerLazy;
  }
  if (normalized == "plumtree" || normalized == "plum") {
    return NamedBroadcastAlgorithm::Plumtree;
  }
  if (normalized == "optimum-p2p" || normalized == "optimump2p" || normalized == "optimum") {
    return NamedBroadcastAlgorithm::OptimumP2P;
  }
  if (normalized == "twostep" || normalized == "twostep-auto") {
    return NamedBroadcastAlgorithm::Twostep;
  }
  if (normalized == "twostep-push") {
    return NamedBroadcastAlgorithm::TwostepPush;
  }
  if (normalized == "twostep-fec") {
    return NamedBroadcastAlgorithm::TwostepFec;
  }
  return td::Status::Error(PSTRING() << "unknown broadcast algorithm \"" << name
                                     << "\"; expected auto, eager-lazy, plumtree, optimum-p2p, twostep, "
                                        "twostep-push, or twostep-fec");
}

PublishSourceIdentity publish_source_identity(td::uint32 flags, const BroadcastMode &, const BroadcastSource &source,
                                              const adnl::AdnlNodeIdShort &local_id) {
  bool any_sender = flags & Overlays::BroadcastFlagAnySender();
  return PublishSourceIdentity{any_sender ? PublicKeyHash::zero() : source.key_hash,
                               v2_id_src_adnl_id(flags, local_id)};
}

td::uint32 v2_rlnc_shard_count(td::uint32 data_size) {
  auto size = std::max<td::uint32>(1, data_size);
  auto shard_payload_size = (size + kPlanningDefaults.v2_rlnc_shard_count - 1) / kPlanningDefaults.v2_rlnc_shard_count;
  return std::max<td::uint32>(1, (data_size + shard_payload_size - 1) / shard_payload_size);
}

BroadcastMode plan_twostep_fec_mode(td::uint32 data_size, size_t persistent_peers) {
  LOG_CHECK(persistent_peers > 2) << "persistent_nodes=" << persistent_peers;
  auto k = static_cast<td::uint32>((persistent_peers * 2 - 2) / 3);
  auto part_size = (data_size + k - 1) / k;
  CHECK(part_size < data_size);
  return mode::TwostepFec{.required_pieces = k, .part_size = part_size};
}

BroadcastMode plan_twostep_mode(const BroadcastModeRequest &request) {
  bool use_fec = request.data_size >= kPlanningDefaults.min_twostep_fec_data_size &&
                 request.persistent_peer_count >= kPlanningDefaults.min_twostep_fec_peers;
  return use_fec ? plan_twostep_fec_mode(request.data_size, request.persistent_peer_count)
                 : BroadcastMode{mode::TwostepPush{}};
}

BroadcastMode plan_for_algorithm(NamedBroadcastAlgorithm algorithm, const BroadcastModeRequest &request) {
  switch (algorithm) {
    case NamedBroadcastAlgorithm::Auto:
    case NamedBroadcastAlgorithm::EagerLazy:
      return mode::EagerLazy{};
    case NamedBroadcastAlgorithm::Plumtree:
      return mode::Plumtree{};
    case NamedBroadcastAlgorithm::OptimumP2P:
      return mode::OptimumP2P{.required_pieces = v2_rlnc_shard_count(request.data_size)};
    case NamedBroadcastAlgorithm::Twostep:
      return plan_twostep_mode(request);
    case NamedBroadcastAlgorithm::TwostepPush:
      return mode::TwostepPush{};
    case NamedBroadcastAlgorithm::TwostepFec:
      if (request.data_size >= kPlanningDefaults.min_twostep_fec_data_size &&
          request.persistent_peer_count >= kPlanningDefaults.min_twostep_fec_peers) {
        return plan_twostep_fec_mode(request.data_size, request.persistent_peer_count);
      }
      VLOG(OVERLAY_BROADCAST_WARNING) << "debug twostep-fec broadcast falls back to twostep-push, data_size="
                                      << request.data_size << ", persistent_peers=" << request.persistent_peer_count;
      return mode::TwostepPush{};
  }
  UNREACHABLE();
}

}  // namespace

td::Status check_overlay_broadcast_algorithm_name(const std::string &name) {
  return parse_algorithm_name(name).move_as_status();
}

td::Result<BroadcastMode> choose_overlay_broadcast_mode(const BroadcastModeRequest &request) {
  CHECK(!request.algorithm.empty());
  TRY_RESULT(algorithm, parse_algorithm_name(request.algorithm));
  return plan_for_algorithm(algorithm, request);
}

td::Status OverlayBroadcasts::process(Env *env, adnl::AdnlNodeIdShort src_peer_id, IncomingMessage incoming) {
  TD_PERF_COUNTER(overlay_broadcast_process);
  auto broadcast_id = incoming.meta.broadcast_id;
  auto it = sessions_.find(broadcast_id);
  if (it == sessions_.end()) {
    if (delivered_.contains(broadcast_id)) {
      VLOG(OVERLAY_BROADCAST_INFO) << "new broadcast DUPLICATE receiver broadcast_id=" << broadcast_id.to_hex();
      return td::Status::Error(ErrorCode::notready, "duplicate broadcast");
    }
    it = sessions_.emplace(broadcast_id, make_session(env, incoming.meta.clone())).first;
  }
  auto session = it->second;
  session->push_event({SessionEvent::Receive{std::move(incoming), src_peer_id}});
  return td::Status::OK();
}

std::shared_ptr<OverlayBroadcastSession> OverlayBroadcasts::make_session(Env *env, BroadcastMeta meta) {
  auto *family = find_family(mode_to_family_key(meta.info.mode));
  return OverlayBroadcastSession::make({.env = *env, .owner = *this, .peer_map = peer_map_, .family = family}, opts_,
                                       std::move(meta));
}

void OverlayBroadcasts::on_overlay_peer_added(Env *env, adnl::AdnlNodeIdShort adnl_id) {
  if (adnl_id == env->local_id()) {
    return;  // self never enters the algorithm peer list — it's always kSelfPeerId
  }
  auto mapped = peer_map_.peer_id_for(adnl_id);
  if (!mapped) {
    return;  // map full
  }
  // Pull current overlay flags. peer_info is best-effort; missing info means we still fan out a
  // bare Peer DTO (id only) so algorithms can at least know the identity exists.
  proto::Peer peer{.id = mapped->id};
  if (auto info = env->peer_info(adnl_id)) {
    peer.score = info->score;
    peer.neighbour = info->neighbour;
    peer.persistent = info->persistent;
  }
  // Each registered family updates its own Shared exactly once. Active sessions then receive
  // a behavioural notification.
  for (auto &family : families_) {
    family.shared->on_peer_upsert(peer);
  }
  for (auto &kv : sessions_) {
    kv.second->push_event({SessionEvent::PeerUpsert{peer}});
  }
}

void OverlayBroadcasts::on_overlay_peer_removed(adnl::AdnlNodeIdShort adnl_id) {
  // Look up but don't reclaim — keep the PeerId reserved so reinserted peers retain stable id.
  auto known = peer_map_.peer_id_for(adnl_id);
  if (!known) {
    return;
  }
  for (auto &family : families_) {
    family.shared->on_peer_remove(known->id);
  }
  for (auto &kv : sessions_) {
    kv.second->push_event({SessionEvent::PeerRemove{known->id}});
  }
}

void OverlayBroadcasts::configure_algorithms(std::vector<AlgorithmFamily> families) {
  families_ = std::move(families);
}

AlgorithmFamily *OverlayBroadcasts::find_family(std::string_view key) {
  for (auto &family : families_) {
    if (family.key == key) {
      return &family;
    }
  }
  return nullptr;
}

std::string mode_to_family_key(const BroadcastMode &mode) {
  return std::visit(td::overloaded([](const mode::TwostepPush &) { return std::string{"twostep-push"}; },
                                   [](const mode::TwostepFec &) { return std::string{"twostep-fec"}; },
                                   [](const mode::EagerLazy &) { return std::string{"eager-lazy"}; },
                                   [](const mode::Plumtree &) { return std::string{"plumtree"}; },
                                   [](const mode::OptimumP2P &) { return std::string{"optimum-p2p"}; }),
                    mode);
}

void OverlayBroadcasts::send(Env *env, BroadcastSource source, BroadcastMode mode, td::BufferSlice data,
                             td::BufferSlice extra, td::uint32 flags) {
  TD_PERF_COUNTER(overlay_broadcast_send);
  auto identity = publish_source_identity(flags, mode, source, env->local_id());
  BroadcastInfo info{.common = {.flags = flags,
                                .date = static_cast<td::uint32>(td::Clocks::system()),
                                .id_source = identity.id_source,
                                .src_adnl_id = identity.src_adnl_id,
                                .data_hash = sha256_bits256(data.as_slice()),
                                .data_size = static_cast<td::uint32>(data.size()),
                                .extra = std::move(extra)},
                     .mode = mode};
  auto broadcast_id = wire_for_mode(mode)->compute_broadcast_id(info);
  static auto dedup_delivered =
      td::NamedThreadSafeCounter::get_default().get_counter(td::Slice("overlay_broadcast_send_dedup_delivered"));
  static auto dedup_session =
      td::NamedThreadSafeCounter::get_default().get_counter(td::Slice("overlay_broadcast_send_dedup_session"));
  static auto fresh = td::NamedThreadSafeCounter::get_default().get_counter(td::Slice("overlay_broadcast_send_fresh"));
  if (delivered_.contains(broadcast_id)) {
    dedup_delivered.add(1);
    return;
  }
  std::shared_ptr<OverlayBroadcastSession> session;
  if (auto it = sessions_.find(broadcast_id); it != sessions_.end()) {
    dedup_session.add(1);
    session = it->second;
  } else {
    fresh.add(1);
    session = make_session(env, {.broadcast_id = broadcast_id, .info = std::move(info)});
    sessions_.emplace(broadcast_id, session);
  }
  session->set_source_body(mode, std::move(data));
  session->push_event({SessionEvent::Publish{std::move(source)}});
}

void OverlayBroadcasts::on_timer_fired(td::Bits256 broadcast_id, td::uint64 token) {
  auto it = sessions_.find(broadcast_id);
  if (it == sessions_.end()) {
    return;
  }
  it->second->push_event({SessionEvent::Timer{token}});
}

void OverlayBroadcasts::gc() {
  std::vector<std::shared_ptr<OverlayBroadcastSession>> expired;
  auto now = static_cast<td::uint32>(td::Clocks::system());
  for (const auto &[_, session] : sessions_) {
    if (session->is_expired(now)) {
      expired.push_back(session);
    }
  }
  for (const auto &session : expired) {
    auto broadcast_id = session->broadcast_id();
    if (!session->is_delivered()) {
      LOG(INFO) << "new broadcast GC_INCOMPLETE " << *session
                << " decoded=false elapsed=" << session->elapsed_since_start();
    }
    erase_session(session);
    delivered_.insert(broadcast_id);
  }
}

void OverlayBroadcasts::erase_session(const std::shared_ptr<OverlayBroadcastSession> &session) {
  auto it = sessions_.find(session->broadcast_id());
  CHECK(it != sessions_.end());
  CHECK(it->second == session);
  session->mark_erased();
  sessions_.erase(it);
}

void OverlayBroadcasts::mark_delivered(const Overlay::BroadcastHash &broadcast_id) {
  delivered_.insert(broadcast_id);
}

}  // namespace overlay
}  // namespace ton
