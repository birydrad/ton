#include <utility>

#include "overlay/broadcast/algorithms/eager-lazy.h"
#include "overlay/broadcast/algorithms/fec.h"
#include "overlay/broadcast/algorithms/gossip-mask-fec.h"
#include "overlay/broadcast/algorithms/optimum-p2p.h"
#include "overlay/broadcast/algorithms/plumtree.h"
#include "overlay/broadcast/algorithms/pull-only.h"
#include "overlay/broadcast/algorithms/push.h"
#include "overlay/broadcast/algorithms/sub-direct.h"
#include "overlay/broadcast/algorithms/twostep.h"

#include "catalogue.h"

namespace ton::bsim_runner {

namespace {

using BodySizedAlgorithm = std::function<AlgoEntry(std::uint64_t)>;

// Production FEC settings — 768 B RaptorQ symbols, 2× redundancy, optional 5 MB/s pacing.
constexpr std::uint32_t kProdSymbolBytes = 768;
constexpr double kProdFecOverhead = 2.0;
constexpr double kProdPaceBytesPerSec = 5.0 * 1024.0 * 1024.0;

// Modern (QUIC-era) FEC: 4 KB symbols, no pacing.
constexpr std::uint32_t kModernSymbolBytes = 4096;

// Below this body size FEC degenerates to a single chunk: we double push fan-out to compensate.
constexpr std::uint64_t kChunkedBodySize = 1024;
constexpr std::uint32_t kSmallBodyKMultiplier = 2;

bool use_chunks(std::uint64_t body_size) {
  return body_size >= kChunkedBodySize;
}

std::uint32_t production_required_pieces(std::uint64_t body_size) {
  if (body_size == 0)
    return 1;
  return static_cast<std::uint32_t>((body_size + kProdSymbolBytes - 1) / kProdSymbolBytes);
}

algo::FecConfig fit_fec_to_body_size(algo::FecConfig fec, std::uint64_t body_size) {
  if (!use_chunks(body_size)) {
    fec.total_pieces = 1;
    fec.required_pieces = 1;
    fec.random_per_piece *= kSmallBodyKMultiplier;
    if (fec.push_per_piece > 0) {
      fec.push_per_piece *= kSmallBodyKMultiplier;
    }
  }
  if (fec.emit_batch_size > fec.total_pieces) {
    fec.emit_batch_size = fec.total_pieces;
  }
  return fec;
}

struct ModernFecLayout {
  td::uint32 total_pieces;
  td::uint32 required_pieces;
};

ModernFecLayout modern_fec_layout(std::uint64_t body_size, std::uint32_t overhead_pct) {
  if (!use_chunks(body_size)) {
    return {1, 1};
  }
  auto required =
      std::max<std::uint32_t>(1, static_cast<std::uint32_t>((body_size + kModernSymbolBytes - 1) / kModernSymbolBytes));
  auto total = required + (required * overhead_pct + 99) / 100;
  return {total, required};
}

BodySizedAlgorithm fixed_family(std::string label,
                                std::function<::ton::overlay::broadcast::AlgorithmFamily()> family_factory,
                                std::string description) {
  return [label = std::move(label), family_factory = std::move(family_factory),
          description = std::move(description)](std::uint64_t) {
    return AlgoEntry{.label = label, .piece_count = 1, .family_factory = family_factory, .description = description};
  };
}

// Production FEC family: 768 B symbols, 2× redundancy. Optionally paced at 5 MB/s.
BodySizedAlgorithm prod_fec(std::string label, std::uint32_t k, bool stable_per_piece, bool pacing_5mbs,
                            std::string description) {
  return [label = std::move(label), k, stable_per_piece, pacing_5mbs,
          description = std::move(description)](std::uint64_t body_size) {
    auto required = production_required_pieces(body_size);
    auto total = static_cast<std::uint32_t>(required * kProdFecOverhead);
    algo::FecConfig fec;
    fec.total_pieces = total;
    fec.required_pieces = required;
    fec.random_per_piece = k;
    fec.push_per_piece = 0;
    fec.stable_per_piece = stable_per_piece;
    if (pacing_5mbs) {
      fec.emit_batch_size = 4;
      fec.emit_interval = (kProdSymbolBytes * 4) / kProdPaceBytesPerSec;
    } else {
      fec.emit_batch_size = total;
      fec.emit_interval = 0.0;
    }
    fec.emit_after_decode = true;
    auto fitted = fit_fec_to_body_size(fec, body_size);
    return AlgoEntry{.label = label,
                     .piece_count = fitted.required_pieces,
                     .family_factory = [fitted] { return ton::overlay::broadcast::make_fec_family(fitted); },
                     .description = description};
  };
}

// Twostep: source delivers body (or one piece each in FEC mode) to all persistent peers; those
// re-emit to everyone. piece_count for fec_mode is computed at runtime as (2N-2)/3.
BodySizedAlgorithm twostep(std::string label, bool fec_mode, std::string description) {
  return [label = std::move(label), fec_mode, description = std::move(description)](std::uint64_t /*body_size*/) {
    return AlgoEntry{.label = label,
                     .piece_count = 1,
                     .family_factory =
                         [fec_mode] {
                           return fec_mode ? ton::overlay::broadcast::make_twostep_fec_family()
                                           : ton::overlay::broadcast::make_twostep_push_family();
                         },
                     .require_persistent_peers = true,
                     .require_all_peers_neighbours = true,
                     .dynamic_piece_count_twostep = fec_mode,
                     .description = description};
  };
}

// Gossip-mask FEC: K1 push-per-piece + batched HavePieces announces. Optional source partition
// distributes initial pieces in a "different-per-neighbour" round-robin pattern.
BodySizedAlgorithm gossip_mask(std::string label, td::uint32 k1, td::uint32 source_partition_fanout,
                               td::uint32 fixed_total_pieces, td::uint32 fixed_required_pieces,
                               std::string description) {
  return [label = std::move(label), k1, source_partition_fanout, fixed_total_pieces, fixed_required_pieces,
          description = std::move(description)](std::uint64_t body_size) {
    algo::FecConfig fec;
    if (fixed_total_pieces > 0) {
      fec.total_pieces = fixed_total_pieces;
      fec.required_pieces = fixed_required_pieces;
    } else {
      auto layout = modern_fec_layout(body_size, /*overhead_pct=*/20);
      fec.total_pieces = layout.total_pieces;
      fec.required_pieces = layout.required_pieces;
    }
    fec.emit_batch_size = fec.total_pieces;
    fec.emit_interval = 0.0;
    fec.emit_after_decode = false;
    ton::overlay::broadcast::GossipMaskFecConfig cfg{
        .fec = fec, .push_per_piece = k1, .announce_delay = 0.005, .source_partition_fanout = source_partition_fanout};
    return AlgoEntry{.label = label,
                     .piece_count = fec.required_pieces,
                     .family_factory = [cfg] { return ton::overlay::broadcast::make_gossip_mask_fec_family(cfg); },
                     .description = description};
  };
}

std::vector<BodySizedAlgorithm> standard_algorithm_factories() {
  std::vector<BodySizedAlgorithm> rows;

  // -- Production baselines (mainnet today) ---------------------------------------------------
  rows.push_back(fixed_family(
      "prod-simple",
      [] { return ton::overlay::broadcast::make_push_family(ton::overlay::broadcast::PushConfig{.k = 5}); },
      "Production short-body broadcast: send body as-is to 5 random neighbours once."));
  rows.push_back(prod_fec("prod-fec-fixed-K5-5MB", /*k=*/5, /*stable=*/true, /*pace=*/true,
                          "Production FEC: 768 B symbols, FEC ratio 2.0, K=5 stable peers per source, paced 5 MB/s."));
  rows.push_back(prod_fec("prod-fec-random-inf", /*k=*/5, /*stable=*/false, /*pace=*/false,
                          "Production-shaped FEC with random per-piece peer choice and no pacing."));
  rows.push_back(prod_fec("prod-fec-random-K10-inf", /*k=*/10, /*stable=*/false, /*pace=*/false,
                          "Same as prod-fec-random-inf but K=10 random peers per piece."));

  // -- Twostep private-overlay ----------------------------------------------------------------
  rows.push_back(
      twostep("twostep-simple", /*fec=*/false, "Two-step push to all persistent peers (private overlay assumption)."));
  rows.push_back(twostep("twostep-fec", /*fec=*/true,
                         "Two-step push with FEC chunking, persistent peers receive starter piece each."));

  // -- No-FEC whole-body family ---------------------------------------------------------------
  rows.push_back(fixed_family(
      "flood-K=5",
      [] { return ton::overlay::broadcast::make_push_family(ton::overlay::broadcast::PushConfig{.k = 5}); },
      "Whole-body eager push to 5 random neighbours (no PRUNE)."));
  rows.push_back(fixed_family(
      "pull-K=10", [] { return ton::overlay::broadcast::make_pull_only_family({.k = 10}); },
      "Pure-pull whole-body broadcast. Have→Request→Piece per hop, no state."));
  rows.push_back(fixed_family(
      "eager-lazy",
      [] { return ton::overlay::broadcast::make_eager_lazy_family({.active_peer_limit = 5, .lazy_peer_limit = 16}); },
      "Per-broadcast plumtree-style: eager push K=5 + lazy Have 16. No persistent mesh."));
  rows.push_back(fixed_family(
      "plumtree", [] { return ton::overlay::broadcast::make_plumtree_family({}); },
      "Plumtree with persistent eager/lazy mesh (iroh-gossip style). PRUNE-on-duplicate / GRAFT-on-timeout."));
  rows.push_back(fixed_family(
      "plumtree-cap=5",
      [] { return ton::overlay::broadcast::make_plumtree_family({.eager_cap_min = 5, .eager_cap_max = 5}); },
      "Plumtree with eager set pinned to exactly 5."));
  rows.push_back(fixed_family(
      "plumtree-cap=10",
      [] { return ton::overlay::broadcast::make_plumtree_family({.eager_cap_min = 10, .eager_cap_max = 10}); },
      "Plumtree with eager set pinned to exactly 10."));
  rows.push_back(fixed_family(
      "subscribe-prune", [] { return ton::overlay::broadcast::make_sub_direct_family(/*prune=*/true); },
      "Subscribe/push with PRUNE-on-duplicate: subscriber set converges over multiple broadcasts."));

  // -- Gossip-mask FEC ------------------------------------------------------------------------
  rows.push_back(gossip_mask("gossip-mask-K1=2-d5ms", 2, 0, 0, 0, "K1=2 + 5ms announce batch."));
  rows.push_back(gossip_mask("gossip-mask-K1=3-d5ms", 3, 0, 0, 0, "K1=3 + 5ms announce batch."));
  rows.push_back(gossip_mask("gossip-mask-K1=4-d5ms", 4, 0, 0, 0, "K1=4 + 5ms announce batch."));
  rows.push_back(gossip_mask("gossip-mask-coarse30-K1=3-part=8", 3, 8, /*total=*/90, /*required=*/30,
                             "30 coarse pieces × ~35KB, K1=3, source partition=8 (packed-sim equivalent)."));

  // -- Optimum-P2P (RLNC, theoretical reference) ----------------------------------------------
  rows.push_back([](std::uint64_t body_size) {
    auto required = use_chunks(body_size) ? modern_fec_layout(body_size, /*overhead_pct=*/0).required_pieces : 1;
    ton::overlay::broadcast::OptimumP2PConfig config{
        .required_pieces = required,
        .initial_pieces = required,
        .forward_threshold = required,
        .piece_peer_limit = 1,
        .metadata_peer_limit = 32,
        .piece_request_peer_limit = 1,
    };
    return AlgoEntry{.label = "optimum-p2p",
                     .piece_count = required,
                     .family_factory = [config] { return ton::overlay::broadcast::make_optimum_p2p_family(config); },
                     .description = "RLNC broadcast: full decode before forward, piece_peer_limit=1. ≈1.0× ideal."};
  });

  return rows;
}

}  // namespace

std::vector<AlgoEntry> standard_catalogue(std::uint64_t body_size) {
  std::vector<AlgoEntry> rows;
  for (const auto &make : standard_algorithm_factories()) {
    rows.push_back(make(body_size));
  }
  return rows;
}

}  // namespace ton::bsim_runner
