#pragma once

#include "overlay/broadcast/catalog.h"

namespace ton::overlay::broadcast {

// Adversarial: never delivers, never serves. `attract_haves` spams Have to all known peers,
// `reply_with_cancel` returns Cancel on Request (vs silent swallow), `propagate_haves` relays
// received Have onward, `proactive_request` spams Request itself.
AlgorithmFamily make_leech_family(algorithm::LeechConfig config);

}  // namespace ton::overlay::broadcast
