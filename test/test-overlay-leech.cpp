/*
    This file is part of TON Blockchain source code.

    TON Blockchain is free software; you can redistribute it and/or
    modify it under the terms of the GNU General Public License
    as published by the Free Software Foundation; either version 2
    of the License, or (at your option) any later version.

    TON Blockchain is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
*/

// Overlay sybil / broadcast-degradation experiment, with real (public)
// overlays and real DHT, optionally running under TestScheduler so virtual
// minutes cost milliseconds of wall clock.
//
// Phases (default mode):
//   1. bootstrap   — give DHT + overlay time to discover peers (quiesce)
//   2. attacked    — send broadcasts, measure reach with leeches alive-and-
//                    dropping
//   3. kill        — leech ADNL ids are muted at the network manager (all
//                    their traffic, both send and receive, is dropped)
//   4. rotation    — sample reach every `round_seconds` of virtual time; the
//                    overlay's own update_neighbours + TTL eviction organically
//                    replace dead leech neighbours

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "adnl/adnl-node-id.hpp"
#include "adnl/adnl-peer-table.h"
#include "adnl/adnl-test-loopback-implementation.h"
#include "adnl/adnl.h"
#include "auto/tl/ton_api.h"
#include "dht/dht.h"
#include "keyring/keyring.h"
#include "keys/encryptor.h"
#include "keys/keys.hpp"
#include "overlay/overlays.h"
#include "quic/quic-sender.h"
#include "quic/quic-server.h"
#include "td/actor/TestScheduler.h"
#include "td/actor/actor.h"
#include "td/actor/coro_task.h"
#include "td/actor/coro_utils.h"
#include "td/utils/OptionParser.h"
#include "td/utils/Random.h"
#include "td/utils/Status.h"
#include "td/utils/ThreadSafeCounter.h"
#include "td/utils/Time.h"
#include "td/utils/buffer.h"
#include "td/utils/misc.h"
#include "td/utils/port/IPAddress.h"
#include "td/utils/port/path.h"
#include "td/utils/port/signals.h"

namespace {

using ton::adnl::Adnl;
using ton::adnl::AdnlNodeIdFull;
using ton::adnl::AdnlNodeIdShort;
using ton::adnl::TestLoopbackNetworkManager;
using ton::dht::Dht;
using ton::keyring::Keyring;
using ton::overlay::ExperimentalBroadcastOverlay;
using ton::overlay::OverlayIdFull;
using ton::overlay::OverlayIdShort;
using ton::overlay::OverlayOptions;
using ton::overlay::OverlayPrivacyRules;
using ton::overlay::Overlays;
using ton::quic::QuicSender;

// =================================================================================================
// Options
// =================================================================================================

struct Options {
  td::uint32 honest_count = 8;
  td::uint32 leech_count = 32;
  td::uint32 broadcasts_per_measure = 30;
  td::uint32 chunks = 0;  // 0 = simple broadcast, else FEC chunks*768 B
  td::uint32 max_rounds = 30;
  td::uint32 churn_honest = 0;
  td::uint32 multi_publishers = 1;
  double tick_seconds = 0.2;
  double bootstrap = 600.0;
  double round_seconds = 75.0;
  double multi_delay = 1.0;  // total span across K publishers
  double tail_wait_override = -1.0;
  bool use_quic_broadcast = false;
  bool use_v2_broadcast = false;
  bool use_v2_plumtree = false;
  bool use_v2_optimum_p2p = false;
  bool use_v2_twostep = false;
  bool use_any_sender = false;

  size_t payload_bytes() const {
    return chunks == 0 ? 64u : static_cast<size_t>(chunks) * 768u;
  }
  bool use_fec() const {
    return chunks > 0;
  }
  bool use_fec_api() const {
    return use_fec() || use_quic_broadcast;
  }
  double tail_wait() const {
    if (tail_wait_override >= 0) {
      return tail_wait_override;
    }
    return use_fec() ? 120.0 : 5.0;
  }
};

td::Status parse_options(int argc, char *argv[], Options &out) {
  td::OptionParser p;
  p.set_description(
      "real-overlay sybil test: measures broadcast delivery while the public overlay's built-in "
      "neighbour rotation + TTL eviction organically replace dead leech neighbours");
  p.add_option('v', "verbosity", "verbosity (default INFO)", [](td::Slice arg) {
    int v = VERBOSITY_NAME(FATAL) + td::to_integer<int>(arg);
    SET_VERBOSITY_LEVEL(v);
  });
  p.add_option('h', "help", "print help", [&]() {
    char buf[10240];
    td::StringBuilder sb({buf, sizeof(buf) - 1});
    sb << p;
    std::cout << sb.as_cslice().c_str();
    std::cout.flush();
    _Exit(2);
  });
  p.add_checked_option('n', "honest", "honest count (default 8)", [&](td::Slice s) {
    TRY_RESULT_ASSIGN(out.honest_count, td::to_integer_safe<td::uint32>(s));
    if (out.honest_count < 2)
      return td::Status::Error("need >=2 honest");
    return td::Status::OK();
  });
  p.add_checked_option('k', "leech", "leech count (default 32)", [&](td::Slice s) {
    TRY_RESULT_ASSIGN(out.leech_count, td::to_integer_safe<td::uint32>(s));
    return td::Status::OK();
  });
  p.add_checked_option('b', "broadcasts", "broadcasts per measurement (default 30)", [&](td::Slice s) {
    TRY_RESULT_ASSIGN(out.broadcasts_per_measure, td::to_integer_safe<td::uint32>(s));
    return td::Status::OK();
  });
  p.add_checked_option('c', "chunks", "FEC chunks (0 = simple, default 0)", [&](td::Slice s) {
    TRY_RESULT_ASSIGN(out.chunks, td::to_integer_safe<td::uint32>(s));
    return td::Status::OK();
  });
  p.add_option('Q', "quic-broadcast", "use QUIC-backed broadcast path instead of old simple/FEC", [&]() {
    out.use_quic_broadcast = true;
    out.use_v2_broadcast = true;
  });
  p.add_option('\0', "v2-broadcast", "use OverlayBroadcasts V2 wire (eager-lazy)", [&]() {
    out.use_v2_broadcast = true;
    out.use_quic_broadcast = true;
  });
  p.add_option('\0', "v2-plumtree", "V2 wire with Plumtree algorithm", [&]() {
    out.use_v2_plumtree = true;
    out.use_v2_broadcast = true;
    out.use_quic_broadcast = true;
  });
  p.add_option('\0', "v2-optimum-p2p", "V2 metadata/shard broadcast path", [&]() {
    out.use_v2_optimum_p2p = true;
    out.use_v2_broadcast = true;
    out.use_quic_broadcast = true;
  });
  p.add_option('\0', "v2-twostep", "V2 wire with twostep algorithm (no AnySender; uses private overlay)", [&]() {
    out.use_v2_twostep = true;
    out.use_v2_broadcast = true;
    out.use_quic_broadcast = true;
  });
  p.add_option('A', "any-sender", "set BroadcastFlagAnySender on measured broadcasts",
               [&]() { out.use_any_sender = true; });
  p.add_checked_option('\0', "multi-publishers",
                       "publish each broadcast K times by K different senders (default 1; auto-enables "
                       "--any-sender unless using --v2-twostep)",
                       [&](td::Slice s) {
                         TRY_RESULT_ASSIGN(out.multi_publishers, td::to_integer_safe<td::uint32>(s));
                         if (out.multi_publishers < 1)
                           return td::Status::Error("multi-publishers must be >=1");
                         return td::Status::OK();
                       });
  p.add_checked_option('\0', "multi-delay",
                       "total span (seconds) over which all K publishers fire (default 1.0); per-pair gap is "
                       "multi-delay/(K-1)",
                       [&](td::Slice s) {
                         out.multi_delay = td::to_double(s);
                         if (out.multi_delay < 0)
                           return td::Status::Error("multi-delay must be >=0");
                         return td::Status::OK();
                       });
  p.add_checked_option('\0', "tail-wait", "override post-round virtual quiesce seconds (default 5 simple, 120 FEC)",
                       [&](td::Slice s) {
                         out.tail_wait_override = td::to_double(s);
                         if (out.tail_wait_override < 0)
                           return td::Status::Error("tail-wait must be >=0");
                         return td::Status::OK();
                       });
  p.add_checked_option('R', "rounds", "max rotation samples after kill (default 30)", [&](td::Slice s) {
    TRY_RESULT_ASSIGN(out.max_rounds, td::to_integer_safe<td::uint32>(s));
    return td::Status::OK();
  });
  p.add_checked_option('\0', "churn-honest", "honest nodes to rotate offline during post-kill rounds (default 0)",
                       [&](td::Slice s) {
                         TRY_RESULT_ASSIGN(out.churn_honest, td::to_integer_safe<td::uint32>(s));
                         return td::Status::OK();
                       });
  p.add_checked_option('S', "round-seconds", "virtual seconds between rotation samples (default 75)", [&](td::Slice s) {
    out.round_seconds = td::to_double(s);
    return td::Status::OK();
  });
  p.add_checked_option('B', "bootstrap", "bootstrap quiesce seconds (default 600)", [&](td::Slice s) {
    out.bootstrap = td::to_double(s);
    return td::Status::OK();
  });

  TRY_STATUS(p.run(argc, argv));

  if (out.churn_honest >= out.honest_count) {
    return td::Status::Error("churn-honest must be less than honest count");
  }
  if (out.multi_publishers > out.honest_count) {
    return td::Status::Error("multi-publishers must be <= honest count");
  }
  if (out.multi_publishers > 1 && !out.use_any_sender && !out.use_v2_twostep) {
    LOG(WARNING) << "multi-publishers > 1 implies --any-sender (otherwise broadcast_ids would differ); enabling";
    out.use_any_sender = true;
  }
  if (out.use_v2_twostep && out.use_any_sender) {
    LOG(WARNING) << "twostep does not support BroadcastFlagAnySender; disabling --any-sender";
    out.use_any_sender = false;
  }
  return td::Status::OK();
}

// =================================================================================================
// Members and delivery tracker
// =================================================================================================

struct Member {
  AdnlNodeIdFull adnl_full;
  AdnlNodeIdShort adnl_short;
  ton::adnl::AdnlAddressList addr_list;
  ton::PublicKey msg_pub;
  ton::PublicKeyHash msg_hash;
};

struct DeliveryTracker {
  std::unordered_map<td::uint32, std::unordered_set<td::uint32>> deliveries;
  std::unordered_map<td::uint32, td::uint32> sender_of;

  void record_sender(td::uint32 seqno, td::uint32 node_idx) {
    sender_of[seqno] = node_idx;
  }
  void record(td::uint32 seqno, td::uint32 node_idx) {
    deliveries[seqno].insert(node_idx);
  }

  // Mean per-broadcast delivery ratio over [lo, hi), counting only online honest receivers
  // and excluding the sender from the expected count.
  double mean_reach(td::uint32 lo, td::uint32 hi, const std::vector<td::uint8> &online) const {
    td::uint32 online_count = 0;
    for (auto v : online) {
      online_count += v != 0 ? 1 : 0;
    }
    td::uint64 expected = 0, delivered = 0;
    for (td::uint32 seqno = lo; seqno < hi; seqno++) {
      td::uint32 sender = sender_of.count(seqno) ? sender_of.at(seqno) : 0;
      if (online_count == 0 || sender >= online.size()) {
        continue;
      }
      expected += online_count - 1;
      auto it = deliveries.find(seqno);
      if (it == deliveries.end()) {
        continue;
      }
      for (auto idx : it->second) {
        if (idx != sender && idx < online.size() && online[idx] != 0) {
          delivered++;
        }
      }
    }
    return expected == 0 ? 0.0 : static_cast<double>(delivered) / static_cast<double>(expected);
  }
};

class HonestCallback : public Overlays::Callback {
 public:
  HonestCallback(td::uint32 idx, std::shared_ptr<DeliveryTracker> tracker) : idx_(idx), tracker_(std::move(tracker)) {
  }
  void receive_broadcast(ton::PublicKeyHash, OverlayIdShort, td::BufferSlice data) override {
    if (data.size() < sizeof(td::uint32)) {
      return;
    }
    td::uint32 seqno = 0;
    std::memcpy(&seqno, data.as_slice().data(), sizeof(seqno));
    tracker_->record(seqno, idx_);
  }

 private:
  td::uint32 idx_;
  std::shared_ptr<DeliveryTracker> tracker_;
};

class LeechCallback : public Overlays::Callback {
 public:
  void check_broadcast(ton::PublicKeyHash, OverlayIdShort, td::BufferSlice, td::Promise<td::Unit> promise) override {
    promise.set_error(td::Status::Error("leech drop"));
  }
  void precheck_broadcast(ton::PublicKeyHash, OverlayIdShort, td::Bits256, td::BufferSlice, bool,
                          td::Promise<td::Unit> promise) override {
    promise.set_error(td::Status::Error("leech drop"));
  }
};

// =================================================================================================
// Cluster: owns ADNL/DHT/Overlays infrastructure and the member list
// =================================================================================================

struct Cluster {
  static constexpr td::uint32 kDhtNodeCount = 8;

  Options opts;
  std::string db_root;

  td::actor::ActorOwn<Keyring> keyring;
  td::actor::ActorOwn<TestLoopbackNetworkManager> network_manager;
  td::actor::ActorOwn<Adnl> adnl;
  td::actor::ActorOwn<QuicSender> quic_sender;
  std::vector<td::actor::ActorOwn<Dht>> dhts;
  td::actor::ActorOwn<Overlays> overlays;
  std::shared_ptr<ton::dht::DhtGlobalConfig> dht_config;

  std::vector<Member> honest;
  std::vector<Member> leeches;
  std::vector<td::uint8> honest_online;

  OverlayIdFull overlay_id_full{
      ton::create_serialize_tl_object<ton::ton_api::pub_overlay>(td::BufferSlice("overlay-leech-real-rotation"))};
  OverlayIdShort overlay_id{overlay_id_full.compute_short_id()};
  std::shared_ptr<DeliveryTracker> tracker = std::make_shared<DeliveryTracker>();

  td::uint32 next_seqno = 1;
};

ton::adnl::AdnlAddressList make_addr_list(td::uint16 port) {
  td::IPAddress ip;
  ip.init_host_port(PSTRING() << "127.0.0.1:" << port).ensure();
  ton::adnl::AdnlAddressList list;
  list.add_udp_adnl_address(ip).ensure();
  list.set_version(static_cast<td::int32>(td::Time::system_now()));
  list.set_reinit_date(Adnl::adnl_start_time());
  return list;
}

ton::quic::QuicServer::Options quic_test_options() {
  ton::quic::QuicServer::Options o;
  o.enable_gso = false;
  o.enable_gro = false;
  o.enable_mmsg = false;
  o.flood_control.reset();
  o.new_connection_rate_limit_capacity = 0;
  return o;
}

// All the per-cluster setup — runs in scheduler context (real or TestScheduler).
void build_cluster(Cluster &c) {
  c.honest.resize(c.opts.honest_count);
  c.leeches.resize(c.opts.leech_count);
  c.honest_online.assign(c.opts.honest_count, 1);

  c.keyring = Keyring::create(c.db_root);
  c.network_manager = td::actor::create_actor<TestLoopbackNetworkManager>("tln");
  c.adnl = Adnl::create(c.db_root, c.keyring.get());
  td::actor::send_closure(c.adnl, &Adnl::register_network_manager, c.network_manager.get());
  if (c.opts.use_quic_broadcast) {
    c.quic_sender = td::actor::create_actor<QuicSender>(
        "overlay-leech-quic", td::actor::actor_dynamic_cast<ton::adnl::AdnlPeerTable>(c.adnl.get()), c.keyring.get(),
        quic_test_options());
    td::actor::send_closure(c.quic_sender, &QuicSender::set_default_mtu,
                            static_cast<td::uint64>(Overlays::max_fec_broadcast_size()) + 1024);
  }

  auto addr = TestLoopbackNetworkManager::generate_dummy_addr_list();
  auto addr0 = TestLoopbackNetworkManager::generate_dummy_addr_list(true);

  std::vector<AdnlNodeIdFull> dht_ids;
  for (td::uint32 i = 0; i < Cluster::kDhtNodeCount; i++) {
    auto pk = ton::PrivateKey{ton::privkeys::Ed25519::random()};
    auto pub = pk.compute_public_key();
    auto id_short = AdnlNodeIdShort{pub.compute_short_id()};
    if (i == 0) {
      auto obj = ton::create_tl_object<ton::ton_api::dht_node>(pub.tl(), addr0.tl(), -1, td::BufferSlice());
      obj->signature_ = pk.create_decryptor().move_as_ok()->sign(serialize_tl_object(obj, true)).move_as_ok();
      std::vector<ton::tl_object_ptr<ton::ton_api::dht_node>> seed;
      seed.push_back(std::move(obj));
      auto seeds = ton::create_tl_object<ton::ton_api::dht_nodes>(std::move(seed));
      auto global = ton::create_tl_object<ton::ton_api::dht_config_global>(std::move(seeds), 6, 3);
      c.dht_config = Dht::create_global_config(std::move(global)).move_as_ok();
    }
    td::actor::send_closure(c.keyring, &Keyring::add_key, std::move(pk), true, [](td::Result<>) {});
    td::actor::send_closure(c.adnl, &Adnl::add_id, AdnlNodeIdFull{pub}, addr, static_cast<td::uint8>(0));
    td::actor::send_closure(c.network_manager, &TestLoopbackNetworkManager::add_node_id, id_short, true, true);
    c.dhts.push_back(Dht::create(id_short, c.db_root, c.dht_config, c.keyring.get(), c.adnl.get()).move_as_ok());
    dht_ids.push_back(AdnlNodeIdFull{pub});
  }
  for (auto &n1 : dht_ids) {
    for (auto &n2 : dht_ids) {
      td::actor::send_closure(c.adnl, &Adnl::add_peer, n1.compute_short_id(), n2, addr);
    }
  }
  td::actor::send_closure(c.adnl, &Adnl::register_dht_node, c.dhts[0].get());
  c.overlays = Overlays::create(c.db_root, c.keyring.get(), c.adnl.get(), c.dhts[0].get());

  td::uint16 next_port = 21000;
  auto register_member = [&](Member &m) {
    auto pk = ton::PrivateKey{ton::privkeys::Ed25519::random()};
    auto pub = pk.compute_public_key();
    m.adnl_full = AdnlNodeIdFull{pub};
    m.adnl_short = AdnlNodeIdShort{pub.compute_short_id()};
    m.addr_list = c.opts.use_quic_broadcast ? make_addr_list(next_port++)
                                            : TestLoopbackNetworkManager::generate_dummy_addr_list();
    td::actor::send_closure(c.keyring, &Keyring::add_key, std::move(pk), true, [](td::Result<>) {});
    td::actor::send_closure(c.adnl, &Adnl::add_id, m.adnl_full, m.addr_list, static_cast<td::uint8>(0));
    td::actor::send_closure(c.network_manager, &TestLoopbackNetworkManager::add_node_id, m.adnl_short, true, true);

    auto msg_pk = ton::PrivateKey{ton::privkeys::Ed25519::random()};
    m.msg_pub = msg_pk.compute_public_key();
    m.msg_hash = m.msg_pub.compute_short_id();
    td::actor::send_closure(c.keyring, &Keyring::add_key, std::move(msg_pk), true, [](td::Result<>) {});
  };
  for (auto &m : c.honest) {
    register_member(m);
  }
  for (auto &m : c.leeches) {
    register_member(m);
  }

  // Cross-register: every member knows every other member and every dht node.
  std::vector<Member *> all;
  all.reserve(c.honest.size() + c.leeches.size());
  for (auto &m : c.honest) {
    all.push_back(&m);
  }
  for (auto &m : c.leeches) {
    all.push_back(&m);
  }
  for (auto *a : all) {
    for (auto &did : dht_ids) {
      td::actor::send_closure(c.adnl, &Adnl::add_peer, a->adnl_short, did, addr);
    }
    for (auto *b : all) {
      if (a == b)
        continue;
      td::actor::send_closure(c.adnl, &Adnl::add_peer, a->adnl_short, b->adnl_full, b->addr_list);
    }
  }

  // Pick algorithm name + overlay category. Twostep needs persistent peers, hence private overlay.
  auto algo_overlay =
      c.opts.use_v2_twostep ? ExperimentalBroadcastOverlay::Private : ExperimentalBroadcastOverlay::Public;
  ton::overlay::clear_experimental_broadcast_algorithm(algo_overlay);
  if (c.opts.use_v2_broadcast) {
    const char *algo = c.opts.use_v2_optimum_p2p ? "optimum-p2p"
                       : c.opts.use_v2_plumtree  ? "plumtree"
                       : c.opts.use_v2_twostep   ? "twostep"
                                                 : "eager-lazy";
    ton::overlay::set_experimental_broadcast_algorithm(algo_overlay, algo);
  }

  if (c.opts.use_v2_twostep) {
    std::vector<AdnlNodeIdShort> members;
    members.reserve(c.honest.size());
    for (const auto &m : c.honest) {
      members.push_back(m.adnl_short);
    }
    OverlayPrivacyRules rules(20 << 20,
                              ton::overlay::CertificateFlags::AllowFec | ton::overlay::CertificateFlags::Trusted, {});
    for (td::uint32 i = 0; i < c.honest.size(); i++) {
      OverlayOptions opts;
      opts.name_ = "leech-test-twostep";
      opts.max_peers_ = c.opts.honest_count;
      opts.max_neighbours_ = c.opts.honest_count;
      opts.propagate_broadcast_to_ = c.opts.honest_count;
      opts.experimental_broadcast_sender_ = c.quic_sender.get();
      td::actor::send_closure(c.overlays, &Overlays::create_private_overlay_ex, c.honest[i].adnl_short,
                              c.overlay_id_full.clone(), members, std::make_unique<HonestCallback>(i, c.tracker), rules,
                              "", std::move(opts));
    }
    return;
  }

  auto join_public = [&](const Member &m, std::unique_ptr<Overlays::Callback> cb) {
    OverlayPrivacyRules rules(20 << 20, ton::overlay::CertificateFlags::AllowFec, {});
    OverlayOptions opts;
    opts.name_ = "leech-test";
    opts.announce_self_ = true;
    if (c.opts.use_v2_broadcast) {
      opts.experimental_broadcast_sender_ = c.quic_sender.get();
    }
    td::actor::send_closure(c.overlays, &Overlays::create_public_overlay_ex, m.adnl_short, c.overlay_id_full.clone(),
                            std::move(cb), std::move(rules), std::string{R"({ "type": "shard" })"}, std::move(opts));
  };
  for (td::uint32 i = 0; i < c.honest.size(); i++) {
    join_public(c.honest[i], std::make_unique<HonestCallback>(i, c.tracker));
  }
  for (auto &m : c.leeches) {
    join_public(m, std::make_unique<LeechCallback>());
  }
}

// =================================================================================================
// Round helpers (scheduler-agnostic — they only build state and dispatch send_closure)
// =================================================================================================

void set_churn_window(Cluster &c, td::uint32 round) {
  if (c.opts.churn_honest == 0) {
    return;
  }
  td::uint32 first_offline = (round * c.opts.churn_honest) % c.opts.honest_count;
  for (td::uint32 i = 0; i < c.opts.honest_count; i++) {
    bool offline = false;
    for (td::uint32 j = 0; j < c.opts.churn_honest; j++) {
      if (i == (first_offline + j) % c.opts.honest_count) {
        offline = true;
        break;
      }
    }
    c.honest_online[i] = offline ? 0 : 1;
    td::actor::send_closure(c.network_manager, &TestLoopbackNetworkManager::add_node_id, c.honest[i].adnl_short,
                            !offline, !offline);
  }
}

td::uint32 choose_online_honest(const Cluster &c) {
  CHECK(c.opts.churn_honest < c.opts.honest_count);
  while (true) {
    auto idx = td::Random::fast_uint32() % c.opts.honest_count;
    if (c.honest_online[idx] != 0) {
      return idx;
    }
  }
}

// K distinct online publishers, with `primary` first.
std::vector<td::uint32> pick_publishers(const Cluster &c, td::uint32 primary, td::uint32 k) {
  std::vector<td::uint32> result{primary};
  if (k <= 1) {
    return result;
  }
  std::vector<td::uint32> pool;
  for (td::uint32 i = 0; i < c.opts.honest_count; i++) {
    if (i != primary && c.honest_online[i] != 0) {
      pool.push_back(i);
    }
  }
  std::shuffle(pool.begin(), pool.end(), std::mt19937_64(td::Random::fast_uint64()));
  for (auto idx : pool) {
    if (result.size() >= k) {
      break;
    }
    result.push_back(idx);
  }
  return result;
}

// Build a fresh body with the seqno embedded in the first 4 bytes; remaining bytes are random.
td::BufferSlice make_body(size_t size, td::uint32 seqno) {
  td::BufferSlice body(size);
  auto sl = body.as_slice();
  std::memcpy(sl.data(), &seqno, sizeof(seqno));
  td::Random::secure_bytes(sl.substr(sizeof(seqno)));
  return body;
}

// Dispatch a single publish. Asynchronous from the scheduler's POV.
void dispatch_publish(Cluster &c, const Member &m, td::BufferSlice body) {
  td::uint32 flags = c.opts.use_any_sender ? Overlays::BroadcastFlagAnySender() : 0;
  if (c.opts.use_fec_api()) {
    td::actor::send_closure(c.overlays, &Overlays::send_broadcast_fec_ex, m.adnl_short, c.overlay_id, m.msg_hash, flags,
                            std::move(body));
  } else {
    td::actor::send_closure(c.overlays, &Overlays::send_broadcast_ex, m.adnl_short, c.overlay_id, m.msg_hash, flags,
                            std::move(body));
  }
}

void kill_leeches(Cluster &c) {
  for (auto &m : c.leeches) {
    td::actor::send_closure(c.network_manager, &TestLoopbackNetworkManager::add_node_id, m.adnl_short, false, false);
  }
}

// =================================================================================================
// QUIC bytes_tx
// =================================================================================================

void log_round_traffic(td::uint32 publishes, td::uint32 unique, td::int64 tx_delta) {
  if (tx_delta <= 0) {
    return;
  }
  double mb = static_cast<double>(tx_delta) / (1024.0 * 1024.0);
  double per_unique = unique == 0 ? 0.0 : static_cast<double>(tx_delta) / unique / 1024.0;
  LOG(WARNING) << "round publishes=" << publishes << " unique=" << unique << " quic_tx_delta=" << mb << " MB ("
               << per_unique << " KB/unique)";
}

// =================================================================================================
// Perf counter dump (sorted by total CPU time)
// =================================================================================================

std::string format_perf_counters() {
  struct Row {
    std::string base;
    td::int64 count = 0;
    td::int64 duration = 0;
  };
  std::unordered_map<std::string, Row> by_base;
  td::NamedPerfCounter::get_default().for_each([&](td::Slice name, td::int64 value) {
    if (td::ends_with(name, ".count")) {
      auto base = name.substr(0, name.size() - 6).str();
      by_base[base].base = base;
      by_base[base].count = value;
    } else if (td::ends_with(name, ".duration")) {
      auto base = name.substr(0, name.size() - 9).str();
      by_base[base].base = base;
      by_base[base].duration = value;
    }
  });
  std::vector<Row> rows;
  rows.reserve(by_base.size());
  for (auto &[_, r] : by_base) {
    rows.push_back(std::move(r));
  }
  std::sort(rows.begin(), rows.end(), [](const Row &a, const Row &b) { return a.duration > b.duration; });
  td::int64 total = 0;
  for (auto &r : rows) {
    total += r.duration;
  }
  double tps = td::Clocks::ticks_per_second();
  td::StringBuilder sb;
  sb << "\nname\tcount\ttotal(ms)\tavg(us)\t%cpu\n";
  for (auto &r : rows) {
    double total_ms = r.duration / tps * 1000.0;
    double avg_us = r.count == 0 ? 0.0 : r.duration * 1e6 / tps / static_cast<double>(r.count);
    double pct = total == 0 ? 0.0 : 100.0 * static_cast<double>(r.duration) / static_cast<double>(total);
    sb << r.base << "\t" << r.count << "\t" << total_ms << "\t" << avg_us << "\t" << pct << "\n";
  }
  return sb.as_cslice().str();
}

void dump_counters() {
  LOG(WARNING) << "[events] " << td::NamedThreadSafeCounter::get_default();
  LOG(WARNING) << "[perf] " << format_perf_counters();
}

int as_pct(double x) {
  return static_cast<int>(x * 100.0 + 0.5);
}

// =================================================================================================
// Test driver (TestScheduler — virtual time)
// =================================================================================================

td::actor::Task<td::Unit> quiesce(td::actor::TestScheduler &ts, double max_delta) {
  double deadline = td::Time::now() + max_delta;
  co_await ts.wait_sync_work();
  while (true) {
    double next = ts.next_timeout_in();
    if (!std::isfinite(next))
      break;
    double step = std::max(next, 1e-6);
    if (td::Time::now() + step > deadline)
      break;
    ts.advance_time(step);
    co_await ts.wait_sync_work();
  }
  co_return td::Unit{};
}

td::actor::Task<td::int64> quic_bytes_tx(Cluster &c) {
  if (c.quic_sender.empty()) {
    co_return 0;
  }
  auto stats = co_await td::actor::ask(c.quic_sender.get(), &QuicSender::collect_stats);
  co_return stats.summary.server_stats.impl_stats.bytes_tx;
}

td::actor::Task<double> do_round(Cluster &c, td::actor::TestScheduler &ts) {
  td::int64 tx_before = co_await quic_bytes_tx(c);
  td::uint32 lo = c.next_seqno;
  for (td::uint32 i = 0; i < c.opts.broadcasts_per_measure; i++) {
    td::uint32 seqno = c.next_seqno++;
    td::uint32 primary = choose_online_honest(c);
    c.tracker->record_sender(seqno, primary);
    auto body = make_body(c.opts.payload_bytes(), seqno);
    auto senders = pick_publishers(c, primary, c.opts.multi_publishers);
    double gap = senders.size() <= 1 ? 0.0 : c.opts.multi_delay / static_cast<double>(senders.size() - 1);
    for (size_t k = 0; k < senders.size(); k++) {
      dispatch_publish(c, c.honest[senders[k]], td::BufferSlice(body.as_slice()));
      if (k + 1 < senders.size() && gap > 0) {
        co_await quiesce(ts, gap);
      }
    }
    co_await quiesce(ts, c.opts.tick_seconds);
  }
  co_await quiesce(ts, c.opts.tail_wait());
  td::uint32 hi = c.next_seqno;
  td::int64 tx_after = co_await quic_bytes_tx(c);
  td::uint32 unique = hi - lo;
  log_round_traffic(unique * c.opts.multi_publishers, unique, tx_after - tx_before);
  co_return c.tracker->mean_reach(lo, hi, c.honest_online);
}

void run_test(Cluster &c) {
  td::Time::allow_freezes();
  td::actor::TestScheduler ts;
  ts.run([&]() -> td::actor::Task<td::Unit> {
    build_cluster(c);
    co_await ts.wait_sync_work();

    LOG(WARNING) << "=== bootstrapping for " << c.opts.bootstrap << "s virtual ===";
    co_await quiesce(ts, c.opts.bootstrap);

    LOG(WARNING) << "=== phase: attacked ===";
    double attacked = co_await do_round(c, ts);
    LOG(WARNING) << "attacked mean reach = " << as_pct(attacked) << "%";

    LOG(WARNING) << "=== killing leeches ===";
    kill_leeches(c);
    co_await ts.wait_sync_work();

    std::printf("%6s  %6s  %s\n", "round", "t(min)", "reach");
    std::fflush(stdout);
    bool converged = false;
    for (td::uint32 r = 1; r <= c.opts.max_rounds; r++) {
      set_churn_window(c, r - 1);
      co_await ts.wait_sync_work();
      co_await quiesce(ts, c.opts.round_seconds);
      double reach = co_await do_round(c, ts);
      std::printf("%6u  %6.1f  mean=%5.1f%%\n", r, static_cast<double>(r) * c.opts.round_seconds / 60.0, reach * 100.0);
      std::fflush(stdout);
      if (reach >= 0.98) {
        LOG(WARNING) << "converged >= 98% reach at round " << r;
        converged = true;
        break;
      }
    }
    dump_counters();
    td::rmrf(c.db_root).ignore();
    if (!converged) {
      std::cerr << "error: did not converge to >=98% reach after " << c.opts.max_rounds << " rounds\n";
      std::_Exit(1);
    }
    std::_Exit(0);
    co_return td::Unit{};
  });
  td::rmrf(c.db_root).ignore();
  std::_Exit(0);
}

}  // namespace

int main(int argc, char *argv[]) {
  SET_VERBOSITY_LEVEL(verbosity_INFO);
  td::set_default_failure_signal_handler().ensure();

  Options opts;
  if (auto s = parse_options(argc, argv, opts); s.is_error()) {
    std::cerr << "error: " << s.to_string() << "\n";
    return 2;
  }

  Cluster cluster;
  cluster.opts = opts;
  cluster.db_root = "tmp-test-overlay-leech";
  td::rmrf(cluster.db_root).ignore();
  td::mkdir(cluster.db_root).ensure();

  run_test(cluster);
  return 0;
}
