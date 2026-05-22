#pragma once

// Clean, single-purpose harness for `BroadcastAlgorithm`. The sim does three things:
//
//   1. Stand up N nodes with a fixed peer graph and a per-pair one-way latency matrix.
//   2. Run a single broadcast: instantiate the algorithm at the source (with `has_body`),
//      dispatch a `Publish` event, drain the resulting Sends and algorithm alarms until quiescence
//      or `max_sim_time` is reached.
//   3. Account every wire message — body bytes vs control bytes, per-node fan-in/fan-out,
//      first-delivery times, duplicate counts.
//
// Everything algorithm-specific lives in `overlay/broadcast/algorithm.cpp`. The harness has no
// knowledge of FEC, plumtree, eager-lazy, etc; it just wires `algo::Action` outputs back into
// `algo::Event` inputs through a latency-aware queue.

#include <cstdint>
#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

#include "overlay/broadcast/algorithm.h"
#include "overlay/broadcast/catalog.h"
#include "overlay/broadcast/score.h"

namespace ton::bsim {

namespace algo = ::ton::overlay::broadcast::algorithm;
using NodeId = algo::PeerId;
using BodyId = algo::BroadcastId;

struct Topology {
  std::uint32_t node_count = 0;
  std::vector<std::vector<NodeId>> peers;    // peers[i] = i's known peers
  std::vector<std::vector<double>> latency;  // latency[i][j] = one-way seconds, i != j
  // Active view cap (HyParView-style). The first `active_view_size` peers in `peers[i]` are
  // registered with `neighbour=true`; the rest are known-only. 0 = treat all peers as neighbours.
  std::uint32_t active_view_size = 0;
  // Per-node upload bandwidth (bytes/second). Models a single FIFO pipe at each sender:
  // simultaneous Sends serialise, each takes `bytes / upload_bw` seconds to drain. Default
  // 100 MB/s ≈ 800 Mbps, a typical validator-grade cloud node. Captures the chunked-vs-
  // monolithic latency tradeoff: large messages block the pipe; small chunks pipeline.
  double upload_bw = 100.0 * 1024 * 1024;
  // Adversarial nodes — bsim instantiates `Spec::make_leech_algorithm` instead of the honest
  // factory at these positions. Empty vector = no leeches. Indexed by NodeId.
  std::vector<bool> leech_nodes;
};

// Pair of family factories: bsim calls these once per simulated node at simulate() setup. Each
// invocation produces a fresh AlgorithmFamily with its own typed Shared, mirroring production
// where each overlay/node has its own Shared instance. The family's make_algorithm closure is
// then used per-broadcast on that node.
struct AlgorithmFactory {
  using FamilyFactory = std::function<::ton::overlay::broadcast::AlgorithmFamily()>;
  // Legacy plain-algorithm factory signature. Stateless algorithms can be lifted into a
  // FamilyFactory via simple_family(...) below.
  using Make = std::function<std::unique_ptr<algo::BroadcastAlgorithm>(algo::BroadcastInit)>;
  FamilyFactory honest;
  FamilyFactory leech;

  static AlgorithmFactory just(FamilyFactory m) {
    return AlgorithmFactory{.honest = std::move(m)};
  }
};

// Lifts a plain algorithm factory into a FamilyFactory backed by the process-wide empty Shared.
// Use this for stateless algorithms that have nothing to remember between broadcasts.
inline AlgorithmFactory::FamilyFactory simple_family(std::string key, AlgorithmFactory::Make make) {
  return [key = std::move(key), make = std::move(make)]() -> ::ton::overlay::broadcast::AlgorithmFamily {
    return {.key = key, .shared = ::ton::overlay::broadcast::algorithm::empty_shared(), .make_algorithm = make};
  };
}

// One Publish event the engine should fire. Multiple publications with the same body_id model
// the AnySend pattern (race to deliver one body); different body_ids model concurrent unrelated
// broadcasts sharing the same overlay (e.g. testing whether a Plumtree mesh handles parallel
// traffic).
struct Publication {
  NodeId source = 0;
  BodyId body_id = 1;
  double at = 0.0;  // sim time at which to fire Publish (seconds)
};

struct Spec {
  BodyId body_id = 1;
  NodeId source = 0;
  // Extra publishers for the multi-sender AnySend test: every node in this list also receives
  // a Publish at t=0 with the same body_id. Receivers dedup on body_id naturally; per-source
  // overhead scales linearly but receiver-side load stays roughly constant.
  std::vector<NodeId> additional_sources;
  // Concurrent unrelated broadcasts. When non-empty, REPLACES the legacy `source` /
  // `additional_sources` / `body_id` triple — each entry here is a distinct (source, body_id,
  // time) publication. Bodies share the per-node algorithm Shared, so a Plumtree-style mesh
  // serves all bodies. piece_count / body_size apply uniformly to every body.
  std::vector<Publication> publications;
  std::uint64_t body_size = 0;    // bytes of a fully-decoded body
  std::uint32_t piece_count = 1;  // body is split into this many pieces; 1 = single-shot
  // Mark every peer as persistent (overlay member). Used by twostep algorithms that route the
  // first hop through persistent neighbours only. Real validator overlays have a persistent
  // subset; this flag asks bsim to treat the full honest set as if it were that subset.
  bool all_peers_persistent = false;
  // Force every peer in the topology to be marked as neighbour (overrides active_view_size cap).
  // Combined with all_peers_persistent, gives twostep algorithms a fully-meshed overlay so they
  // can deliver to everyone in one hop. Real overlays cap neighbour count; this flag opts out.
  bool all_peers_neighbours = false;
  // Per-message overhead derived from ton_api.tl `overlay.broadcastV2*` constructors:
  //   id (tag+flags+date+src+src_adnl_id+data_hash+data_size+extra+mode tag) ≈ 117 B
  //   source (tag+PublicKey+emptyCertificate+sig bytes) ≈ 109 B
  //   outer constructor tag ≈ 4 B
  //   ADNL/channel framing folded in ≈ 20 B
  // Total ≈ 250 B for Have/Request/Cancel; Piece has the same envelope (+ seqno) plus payload.
  std::uint32_t piece_header = 250;
  std::uint32_t control_bytes = 250;
  AlgorithmFactory factory;
};

struct PerNode {
  std::uint64_t body_bytes_in = 0;
  std::uint64_t body_bytes_out = 0;
  std::uint64_t control_bytes_out = 0;
  std::uint32_t body_messages_in = 0;
  std::uint32_t body_messages_out = 0;
  std::uint32_t duplicate_messages_in = 0;
  double delivery_time = -1.0;  // -1 if never delivered
};

// Per-broadcast stats — produced in streaming runs (Spec.publications) so callers can compute
// per-body latency distributions and per-broadcast cost. Single-body runs populate exactly one
// entry; the legacy globals (`body_bytes`, `body_messages`, etc.) stay valid in both modes.
struct PerBody {
  double publish_time = -1.0;            // simulation-time t when Publish fired
  std::vector<double> delivery_latency;  // (delivery_time - publish_time) per honest receiver
  std::uint64_t body_bytes = 0;
  std::uint64_t control_bytes = 0;
  std::uint64_t body_messages = 0;
  std::uint64_t control_messages = 0;
  std::uint64_t duplicate_messages = 0;

  double percentile_delivery(double q) const;  // q in [0, 1] across this body's deliveries
};

struct Metrics {
  std::uint32_t delivered = 0;  // honest receivers that emitted Deliver (excluding source), summed across all bodies
  // Per-body delivery count. Same key set as Spec.publications' body_ids; legacy single-body
  // runs end up with one entry equal to `delivered`.
  std::unordered_map<BodyId, std::uint32_t> delivered_per_body;
  std::unordered_map<BodyId, PerBody> per_body;
  std::uint64_t body_messages = 0;
  std::uint64_t control_messages = 0;
  std::uint64_t duplicate_messages = 0;
  std::uint64_t body_bytes = 0;
  std::uint64_t control_bytes = 0;
  std::vector<PerNode> per_node;
  double last_delivery = 0.0;

  std::uint64_t total_bytes() const {
    return body_bytes + control_bytes;
  }

  // q in [0, 1]. Percentile across honest receivers' first-delivery times.
  double percentile_delivery(double q) const;
  // q in [0, 1]. Percentile of per-node `body_bytes_out` across all nodes (peak upload load).
  std::uint64_t percentile_body_bytes_out(double q) const;
  std::uint64_t max_body_bytes_out() const;
  std::uint64_t max_body_bytes_in() const;
};

// Optional cross-broadcast persistent state. When passed in, bsim uses production's
// `BroadcastPeerScore` (with half-life decay) per (node, peer). Each call to simulate() advances
// `next_wall_time` by `round_interval` so decay actually fires between rounds.
struct PersistentScore {
  ::ton::overlay::broadcast::BroadcastPeerScore global;
};

struct SessionState {
  std::vector<std::unordered_map<algo::PeerId, PersistentScore>> peer_scores;
  ::ton::overlay::broadcast::BroadcastPeerScoreConfig score_config{};
  double next_wall_time = 0.0;
  double round_interval = 60.0;  // default: 1 half-life between rounds
  // Per-node AlgorithmFamily instances. Created on the first simulate() call (via the Spec
  // factory) and reused on subsequent calls — this is how cross-broadcast Shared state
  // (subscribe leases, peer scoring, mesh memberships) persists across rounds. Indexed by
  // NodeId; absent slots are populated lazily.
  std::vector<std::optional<::ton::overlay::broadcast::AlgorithmFamily>> families;
};

Metrics simulate(const Topology &topology, const Spec &spec, double max_sim_time = 60.0,
                 SessionState *session = nullptr);

// Build `count` publications evenly spaced over [0, span] seconds, rotating sources from `pool`.
// body_ids are sequential starting at `base_body_id`. Useful for steady-state bench scenarios
// where the question is "how does the mesh handle a stream of broadcasts" rather than a single
// hero broadcast.
std::vector<Publication> streaming_publications(std::uint32_t count, double span, const std::vector<NodeId> &pool,
                                                BodyId base_body_id = 1);

}  // namespace ton::bsim
