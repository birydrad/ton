#pragma once

#include "overlay/broadcast/catalog.h"
#include "td/utils/int_types.h"

namespace ton::overlay::broadcast {

// Push-K config. Send to K uniformly-random neighbours; no scoring, no feedback. The intuition:
// peers we usually receive from likely have the message already, so score-ranked eager push to
// "the best peers" is wasted bandwidth. Uniform random spreads the eager fan-out evenly.
struct PushConfig {
  // 0 = no cap (forward to every neighbour). >0 = K random neighbours, seeded by broadcast id.
  td::uint32 k = 0;
};

AlgorithmFamily make_push_family(PushConfig config);

}  // namespace ton::overlay::broadcast
