#pragma once

#include "overlay/broadcast/catalog.h"
#include "td/utils/int_types.h"

namespace ton::overlay::broadcast {

struct OptimumP2PConfig {
  // `required_pieces` and `initial_pieces` are normally written per-broadcast from
  // BroadcastInit::required_pieces; the family closure patches them at make_algorithm time.
  td::uint32 required_pieces = 1;
  td::uint32 initial_pieces = 1;
  td::uint32 forward_threshold = 1;
  td::uint32 piece_peer_limit = 1;
  td::uint32 metadata_peer_limit = 32;
  td::uint32 piece_request_peer_limit = 1;
  double peer_success_delta = -0.25;
};

// Optimum-P2P with Random Linear Network Coding. The family closure reads
// `mode::OptimumP2P::required_pieces` from BroadcastInit and patches the base config
// per-broadcast.
AlgorithmFamily make_optimum_p2p_family(OptimumP2PConfig base);

}  // namespace ton::overlay::broadcast
