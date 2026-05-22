#pragma once

#include "overlay/broadcast/catalog.h"
#include "td/utils/int_types.h"

namespace ton::overlay::broadcast {

// Pure-pull whole-body broadcast. When we have the body, send Have to K random neighbours.
// On receipt of Have, request from the first announcer (FIFO). No FEC, no scoring, no PRUNE.
struct PullOnlyConfig {
  td::uint32 k = 10;  // Number of neighbours to announce Have to (was: lazy_peer_limit).
};

AlgorithmFamily make_pull_only_family(PullOnlyConfig config);

}  // namespace ton::overlay::broadcast
