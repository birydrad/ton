#pragma once

namespace ton::overlay::broadcast {

// BroadcastPeerScore is treated as a cost: lower is better. A successful
// delivery from a peer decreases its score; a request timeout increases it.
struct BroadcastPeerScoreConfig {
  double half_life = 60.0;
  double min_score = -1.0;
  double max_score = 8.0;
};

struct BroadcastPeerScore {
  double score = 0.0;
  double updated_at = 0.0;

  double value_at(double now, BroadcastPeerScoreConfig config = {}) const;
  void add(double delta, double now, BroadcastPeerScoreConfig config = {});
};

}  // namespace ton::overlay::broadcast
