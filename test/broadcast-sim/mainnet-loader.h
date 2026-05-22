#pragma once

// Mainnet topology loader. Reads the crawler's overlay-graph JSON and the geolocation matrix,
// produces a `bsim::Topology` ready to feed `bsim::simulate`. Pure function — no I/O outside of
// the two file reads, no globals — so it's straightforward to unit-test against a synthetic
// JSON pair if needed.

#include <string>
#include <utility>
#include <vector>

#include "td/utils/Status.h"

#include "bsim.h"

namespace ton::bsim_runner {

struct LoadedNode {
  std::string hash;
  std::pair<double, double> latlon{0, 0};
  std::string country;
  bool is_validator = false;
};

struct Loaded {
  bsim::Topology topology;
  std::vector<LoadedNode> nodes;  // parallel to topology.peers, indexed by NodeId
};

// Build a `Loaded` from the JSON files. If `exclude_leeches` is true, nodes flagged
// `unresponsive` in the crawler dump are dropped entirely (and not counted in `node_count`).
// Otherwise they're kept and marked in `topology.leech_nodes`.
td::Result<Loaded> load(const std::string &graph_path, const std::string &latency_path, bool exclude_leeches);

// V2 loader: reads the live `/api/overlay-peers` dump format. Builds a *directed* graph using
// only the peers that have `recent_neighbours` populated (we need an outgoing-edge observation
// to make them a vertex). Within that set, `unresponsive=false` = honest, `=true` = leech.
// Edges = each peer's recent_neighbours, restricted to in-graph targets. Edge latency uses the
// same geo formula as the v1 loader; peers missing from `latency_path` get a 50 ms default.
td::Result<Loaded> load_v2(const std::string &peers_path, const std::string &latency_path);

// Promote randomly-chosen honest nodes into leech state until the global ratio reaches `target`.
// The behaviour itself is implemented by `LeechAlgorithm` — bsim swaps in `Spec::factory.leech`
// for any node where `Topology::leech_nodes[i]` is true. Idempotent across re-promotion.
void promote_leeches(Loaded &loaded, double target_ratio, std::uint32_t seed);

// Rebuild Loaded keeping only nodes where `keep[i]` is true. NodeIds are remapped contiguously
// (new_id == position in the filtered list). Edges pointing to dropped nodes are filtered out;
// latency rows/cols outside the subset are dropped.
Loaded restrict_to_subset(const Loaded &original, const std::vector<bool> &keep);

// Build a `Loaded` for a given leech ratio under the "bad pool" model:
//   * 0% leech: drop every natural-leech node from the graph (keep only the honest subgraph).
//   * Small ratios: keep all honest + random subset of natural-leech as leech up to the ratio.
//   * Large ratios: include the full natural-leech pool + convert random honest → leech to hit
//     the target ratio. Natural-leech nodes never become honest.
Loaded apply_leech_regime(const Loaded &original, double leech_ratio, std::uint32_t seed);

// Drop honest nodes that aren't in the largest strongly-connected component of the honest
// subgraph. The v2 mainnet dump contains a handful of honest peers with no honest in-edges
// (topology-blind sources); without this prune, every algorithm caps at ~99.7% reach because
// those vertices are unreachable by construction. Returns the number of honest nodes demoted
// to leech state. Leeches in the input are left alone.
std::uint32_t prune_to_largest_honest_scc(Loaded &loaded);

// Print "topology: nodes=N leech=L edges=E mean-degree=D max-degree=M" plus the top-5 countries.
void print_summary(const Loaded &loaded);

}  // namespace ton::bsim_runner
