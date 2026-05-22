#pragma once

#include "overlay/broadcast/algorithm.h"
#include "overlay/broadcast/catalog.h"
#include "td/utils/int_types.h"

namespace ton::overlay::broadcast {

struct GossipMaskFecConfig {
  algorithm::FecConfig fec;
  // K1: per piece, push to this many neighbours.
  td::uint32 push_per_piece = 4;
  // Coalesce IHAVE announcements within this window. 0 = announce on every piece receive.
  double announce_delay = 0.0;
  // When >0, source distributes its pieces in a round-robin "different-per-neighbour" mode:
  // piece i goes to neighbour (i % source_partition_fanout). Each neighbour gets a disjoint subset.
  td::uint32 source_partition_fanout = 0;
};

// Gossip with persistent per-broadcast peer-have masks. Each Piece/HavePieces piggybacks
// sender's have-mask; receivers update their `peer_mask[from]` view; future push decisions
// rank candidates by (their missing ∩ our have) and skip known-have peers.
AlgorithmFamily make_gossip_mask_fec_family(GossipMaskFecConfig config);

}  // namespace ton::overlay::broadcast
