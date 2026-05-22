#pragma once

// Internal header shared by every algorithm in this directory. Holds the per-broadcast peer
// table plus its ranking options. Not exported outside `overlay/broadcast/algorithms/`.

#include <algorithm>
#include <cmath>
#include <limits>
#include <optional>
#include <random>
#include <unordered_map>
#include <vector>

#include "overlay/broadcast/algorithm.h"
#include "td/utils/check.h"

namespace ton::overlay::broadcast::algorithm::internal {

struct PeerRankOptions {
  std::optional<PeerId> except = {};
  std::optional<double> score_granularity = {};
  bool shuffle_equal_scores = false;
  // When false, `ranked()` returns peers in table-insertion order (no stable_sort by score).
  // Use this when the caller only wants a filtered subset and doesn't care about ordering.
  bool sort_by_score = true;
  td::uint32 salt = 0;
  size_t limit = std::numeric_limits<size_t>::max();
};

struct PeerOnlyState {
  Peer peer;
};

template <class State>
class PeerTable {
 public:
  // Default ctor: no self awareness. Used by per-overlay Shared instances which only ever store
  // OTHER peers (engine never fans out self) and don't need is_self()/self_state_.
  PeerTable() = default;
  PeerTable(PeerId self, BroadcastId broadcast_id)
      : self_(self), broadcast_id_(broadcast_id), has_self_(true), self_state_{.peer = Peer{.id = self}} {
  }

  size_t size() const {
    return peers_.size();
  }

  bool is_self(PeerId id) const {
    return has_self_ && id == self_;
  }

  State *find(PeerId id) {
    if (is_self(id)) {
      return &self_state_;
    }
    auto index = find_index(id);
    if (!index) {
      return nullptr;
    }
    return &peers_[*index];
  }

  const State *find(PeerId id) const {
    if (is_self(id)) {
      return &self_state_;
    }
    auto index = find_index(id);
    if (!index) {
      return nullptr;
    }
    return &peers_[*index];
  }

  State &require(PeerId id) {
    auto *peer = find(id);
    CHECK(peer != nullptr);
    return *peer;
  }

  const State &require(PeerId id) const {
    auto *peer = find(id);
    CHECK(peer != nullptr);
    return *peer;
  }

  State &register_peer(Peer peer) {
    CHECK(!has_self_ || peer.id != self_);
    CHECK(std::isfinite(peer.score));
    auto id = peer.id;
    if (auto state = find(id)) {
      state->peer = std::move(peer);
      return *state;
    }
    auto index = peers_.size();
    peers_.push_back(State{.peer = std::move(peer)});
    CHECK(peer_index_.emplace(id, index).second);
    return peers_.back();
  }

  // Swap-with-last + index update. O(1) amortised. No-op if peer is unknown or is_self(id).
  void erase(PeerId id) {
    auto it = peer_index_.find(id);
    if (it == peer_index_.end()) {
      return;
    }
    auto index = it->second;
    peer_index_.erase(it);
    auto last = peers_.size() - 1;
    if (index != last) {
      peers_[index] = std::move(peers_[last]);
      peer_index_[peers_[index].peer.id] = index;
    }
    peers_.pop_back();
  }

  std::vector<State *> ranked(PeerRankOptions options) {
    return ranked(options, [](const State &) { return true; });
  }

  template <class Filter>
  std::vector<State *> filtered(PeerRankOptions options, Filter filter) {
    return filtered_impl<State *>(peers_, has_self_, self_, options, filter);
  }

  template <class Filter>
  std::vector<const State *> filtered(PeerRankOptions options, Filter filter) const {
    return filtered_impl<const State *>(peers_, has_self_, self_, options, filter);
  }

  template <class Filter>
  State *find_unique(Filter filter) {
    auto result = filtered(PeerRankOptions{.limit = 2}, filter);
    CHECK(result.size() <= 1);
    return result.empty() ? nullptr : result[0];
  }

  template <class Filter>
  const State *find_unique(Filter filter) const {
    auto result = filtered(PeerRankOptions{.limit = 2}, filter);
    CHECK(result.size() <= 1);
    return result.empty() ? nullptr : result[0];
  }

  template <class Filter>
  bool any(Filter filter) const {
    return !filtered(PeerRankOptions{.limit = 1}, filter).empty();
  }

  template <class Filter>
  size_t count(Filter filter) const {
    return filtered(PeerRankOptions{}, filter).size();
  }

  // Tiny xorshift32 — drop-in for std::shuffle's RNG concept. Constructor is one write; next()
  // is three xors. ~10x faster than re-seeding mt19937 for every per-chunk shuffle (which the
  // sample profile flagged as the dominant cost in FecAlgorithm).
  struct XorShift32 {
    using result_type = std::uint32_t;
    static constexpr result_type min() {
      return 0;
    }
    static constexpr result_type max() {
      return UINT32_MAX;
    }
    result_type s;
    explicit XorShift32(std::uint64_t seed) : s(static_cast<std::uint32_t>(seed ^ (seed >> 32))) {
      if (s == 0) {
        s = 0x9E3779B9u;  // golden ratio — anything non-zero
      }
    }
    result_type operator()() {
      s ^= s << 13;
      s ^= s >> 17;
      s ^= s << 5;
      return s;
    }
  };

  XorShift32 make_rng(std::uint32_t salt) const {
    // splitmix-style mix of (broadcast_id, self, salt) into one 64-bit seed.
    std::uint64_t z = broadcast_id_ ^ (static_cast<std::uint64_t>(self_) << 32) ^ salt;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return XorShift32(z ^ (z >> 31));
  }

  template <class Filter>
  std::vector<State *> ranked(PeerRankOptions options, Filter filter) {
    CHECK(!options.score_granularity ||
          (std::isfinite(*options.score_granularity) && *options.score_granularity > 0.0));
    auto result = filtered_impl<State *>(peers_, has_self_, self_, PeerRankOptions{.except = options.except}, filter);
    // Hot path for FEC etc.: shuffle_equal_scores + no granularity is just "pick K random of N".
    // Skip the full shuffle+stable_sort, do partial Fisher-Yates on the first `limit` slots.
    if (options.shuffle_equal_scores && !options.score_granularity && options.limit < result.size()) {
      auto rng = make_rng(options.salt);
      for (size_t i = 0; i < options.limit; i++) {
        std::uniform_int_distribution<size_t> dist(i, result.size() - 1);
        std::swap(result[i], result[dist(rng)]);
      }
      result.resize(options.limit);
      return result;
    }
    if (options.shuffle_equal_scores) {
      auto rng = make_rng(options.salt);
      std::shuffle(result.begin(), result.end(), rng);
    }
    if (options.sort_by_score) {
      std::stable_sort(result.begin(), result.end(), [&](const State *a, const State *b) {
        return score_bucket(a->peer.score, options.score_granularity) <
               score_bucket(b->peer.score, options.score_granularity);
      });
    }
    if (result.size() > options.limit) {
      result.resize(options.limit);
    }
    return result;
  }

 private:
  template <class PeerPtr, class Peers, class Filter>
  static std::vector<PeerPtr> filtered_impl(Peers &peers, bool has_self, PeerId self, PeerRankOptions options,
                                            Filter filter) {
    std::vector<PeerPtr> result;
    result.reserve(peers.size());
    for (auto &peer : peers) {
      if ((has_self && peer.peer.id == self) || (options.except && peer.peer.id == *options.except) || !filter(peer)) {
        continue;
      }
      result.push_back(&peer);
      if (result.size() == options.limit) {
        break;
      }
    }
    return result;
  }

  PeerId self_ = 0;
  BroadcastId broadcast_id_ = 0;
  bool has_self_ = false;
  State self_state_;
  std::vector<State> peers_;
  std::unordered_map<PeerId, size_t> peer_index_;

  std::optional<size_t> find_index(PeerId id) const {
    auto it = peer_index_.find(id);
    if (it == peer_index_.end()) {
      return std::nullopt;
    }
    CHECK(it->second < peers_.size());
    CHECK(peers_[it->second].peer.id == id);
    return it->second;
  }

  double score_bucket(double score, std::optional<double> granularity) const {
    CHECK(std::isfinite(score));
    if (!granularity) {
      return score;
    }
    auto scaled = score / *granularity;
    auto bucket = scaled < 0.0 ? std::floor(scaled) : std::ceil(scaled);
    CHECK(std::isfinite(bucket));
    return bucket;
  }
};

}  // namespace ton::overlay::broadcast::algorithm::internal
