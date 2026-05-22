#pragma once

#include "overlay/broadcast/catalog.h"
#include "td/utils/int_types.h"

namespace ton::overlay::broadcast {

struct EagerLazyConfig {
  td::uint32 active_peer_limit = 6;  // eager push fan-out
  td::uint32 lazy_peer_limit = 10;   // lazy Have fan-out
  double request_delay = 0.050;
  double request_retry_delay = 0.075;
  double peer_success_delta = -0.25;  // on Piece arrival
  double peer_timeout_delta = 1.0;    // on Request timeout
};

// Plumtree on the whole body: eager push to top-K peers + lazy Have to the rest; receivers
// Cancel on duplicate. No FEC.
AlgorithmFamily make_eager_lazy_family(EagerLazyConfig config);

}  // namespace ton::overlay::broadcast
