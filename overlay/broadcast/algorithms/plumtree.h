#pragma once

#include "overlay/broadcast/catalog.h"

namespace ton::overlay::broadcast {

struct PlumtreeConfig {
  double graft_timeout = 0.050;
  double success_delta = -0.25;
  // Lower bound on the eager set. PRUNE-on-duplicate skips demote when |eager| <= this.
  std::size_t eager_cap_min = 0;
  // Upper bound. New neighbours go to lazy when |eager| >= this; GRAFT promotions blocked.
  // 0 = no cap.
  std::size_t eager_cap_max = 0;
};

// Plumtree (Leitão et al. 2007) in the iroh-gossip style: eager/lazy peer split lives in a
// per-overlay Shared and persists across broadcasts. PRUNE-on-duplicate demotes a peer; missing
// message GRAFTs promote. Over multiple rounds the eager set converges to a spanning tree.
//
// Wire mapping: Piece = GOSSIP, Have = IHAVE, Request = GRAFT, Cancel = PRUNE.
AlgorithmFamily make_plumtree_family(PlumtreeConfig config);

}  // namespace ton::overlay::broadcast
