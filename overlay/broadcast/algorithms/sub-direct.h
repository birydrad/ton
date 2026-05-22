#pragma once

#include "overlay/broadcast/catalog.h"

namespace ton::overlay::broadcast {

// Whole-body subscribe/push: source pushes Piece to subscribers and Have to everyone else;
// receivers Subscribe (= Request) to the first announcer. Per-edge "this peer is subscribed
// to me" leases live in the family's private Shared and survive across broadcasts so the
// subscription tree converges over multiple rounds.
//
// prune_on_duplicate: receiver Cancels on a duplicate Piece; sender clears that subscription
// lease so future broadcasts skip the redundant push. Same idea as plumtree's PRUNE.
AlgorithmFamily make_sub_direct_family(bool prune_on_duplicate);

}  // namespace ton::overlay::broadcast
