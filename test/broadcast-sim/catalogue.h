#pragma once

// The fixed list of broadcast algorithms we benchmark. Catalogue rows are algorithm families:
// each factory receives the body size and returns the concrete split/config for that run.

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "overlay/broadcast/algorithm.h"
#include "overlay/broadcast/catalog.h"

namespace ton::bsim_runner {

namespace algo = ::ton::overlay::broadcast::algorithm;

struct AlgoEntry {
  std::string label;
  std::uint32_t piece_count = 1;  // 1 = single-piece body; >1 = multi-piece (FEC, swarm, optimum)
  // Simple algorithm factory for stateless algorithms. Wrapped into a family factory at the
  // bsim Spec site via simple_family(). Mutually exclusive with `family_factory` below.
  std::function<std::unique_ptr<algo::BroadcastAlgorithm>(algo::BroadcastInit)> make;
  // Stateful algorithm factory. Called once per simulated node to produce a fresh
  // AlgorithmFamily (each with its own private BroadcastShared). Used by algorithms with
  // cross-broadcast state (subscribe leases, peer scoring, mesh memberships).
  std::function<::ton::overlay::broadcast::AlgorithmFamily()> family_factory;
  // Twostep-style algorithms route the first hop through persistent overlay members. Asks bsim
  // to treat every honest peer as if it were a persistent member.
  bool require_persistent_peers = false;
  // Force a fully-meshed neighbour set (every peer is everyone's neighbour). Combined with
  // require_persistent_peers, lets twostep deliver in one or two hops without artificial caps.
  bool require_all_peers_neighbours = false;
  // For twostep-FEC: piece_count must equal `k = (2N - 2) / 3` where N = honest peer count
  // (matches broadcast-twostep.cpp). Runner overrides entry.piece_count and Spec.piece_count
  // accordingly at simulate time. Without this, twostep-FEC cannot decode for body sizes that
  // exceed N pieces.
  bool dynamic_piece_count_twostep = false;
  // Human-readable description, surfaced in the HTML report. Keep one line.
  std::string description;
};

// Body-size-aware catalogue. The returned entries contain concrete piece counts and factories
// for this body size.
std::vector<AlgoEntry> standard_catalogue(std::uint64_t body_size);

}  // namespace ton::bsim_runner
