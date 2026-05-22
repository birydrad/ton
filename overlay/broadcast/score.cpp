#include <algorithm>
#include <cmath>

#include "score.h"

namespace ton::overlay::broadcast {

double BroadcastPeerScore::value_at(double now, BroadcastPeerScoreConfig config) const {
  if (score == 0.0 || config.half_life <= 0.0 || now <= updated_at) {
    return score;
  }
  return score * std::pow(0.5, (now - updated_at) / config.half_life);
}

void BroadcastPeerScore::add(double delta, double now, BroadcastPeerScoreConfig config) {
  score = std::clamp(value_at(now, config) + delta, config.min_score, config.max_score);
  updated_at = now;
}

}  // namespace ton::overlay::broadcast
