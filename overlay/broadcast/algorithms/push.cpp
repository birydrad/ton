// Push-K: forward the body to K uniformly-random neighbours (or all neighbours when k=0).
// Receivers coalesce for 1 ms after BodyReady, collecting every Piece sender that races them in,
// then fan out to neighbours minus those senders. Cuts pointless echoes to peers who clearly
// already had the body. No Have/Request, no scoring.

#include <unordered_set>

#include "overlay/broadcast/algorithms/peer-table.h"
#include "overlay/broadcast/algorithms/push.h"

namespace ton::overlay::broadcast::algorithm {
namespace {

using namespace internal;

constexpr double kCoalesceDelay = 0.001;  // 1 ms

class PushShared final : public BroadcastShared {
 public:
  void on_peer_upsert(const Peer &peer) override {
    peers.register_peer(peer);
  }
  void on_peer_remove(PeerId id) override {
    peers.erase(id);
  }

  PeerTable<PeerOnlyState> peers;
};

class PushAlgorithm final : public BroadcastAlgorithm {
 public:
  PushAlgorithm(BroadcastInit init, PushConfig config, std::shared_ptr<PushShared> shared)
      : BroadcastAlgorithm(init.has_body)
      , config_(config)
      , shared_(std::move(shared))
      , broadcast_id_(init.id)
      , has_body_(init.has_body) {
  }

  BroadcastStats stats() const override {
    return BroadcastStats{.has_body = has_body_, .peer_count = shared_->peers.size()};
  }

 private:
  PushConfig config_;
  std::shared_ptr<PushShared> shared_;
  BroadcastId broadcast_id_;
  bool has_body_ = false;
  bool forwarded_ = false;
  std::unordered_set<PeerId> received_from_;

  Actions forward() {
    forwarded_ = true;
    Actions out;
    auto eligible = [this](const PeerOnlyState &peer) {
      return peer.peer.neighbour && received_from_.count(peer.peer.id) == 0;
    };
    std::vector<PeerOnlyState *> selected;
    if (config_.k == 0) {
      selected = shared_->peers.filtered({}, eligible);
    } else {
      selected = shared_->peers.ranked(
          {.shuffle_equal_scores = true, .salt = static_cast<td::uint32>(broadcast_id_), .limit = config_.k}, eligible);
    }
    for (auto *peer : selected) {
      out.send(peer->peer.id, msg::Piece{0});
    }
    return out;
  }

  Actions on_event(const evt::Publish &) override {
    CHECK(!has_body_);
    has_body_ = true;
    auto out = forward();
    out.deliver();
    return out;
  }

  Actions on_event(const evt::BodyReady &) override {
    if (has_body_) {
      return {};  // already published (race with multi-source); BodyReady is a no-op for us.
    }
    has_body_ = true;
    Actions out;
    out.deliver();
    // Hold off fanning out for 1 ms — racing forwarders deliver duplicate Pieces during this
    // window, and we exclude all of them from our own fan-out.
    alarm_ = td::Timestamp::in(kCoalesceDelay, now());
    return out;
  }

  Actions on_event(const evt::Timer &) override {
    return forward();
  }

  Actions on_message(PeerId from, const msg::Piece &) override {
    if (!forwarded_) {
      received_from_.insert(from);
    }
    return {};
  }
};

}  // namespace

}  // namespace ton::overlay::broadcast::algorithm

namespace ton::overlay::broadcast {

AlgorithmFamily make_push_family(PushConfig config) {
  auto shared = std::make_shared<algorithm::PushShared>();
  return {.key = "push", .shared = shared, .make_algorithm = [config, shared](algorithm::BroadcastInit init) {
            return std::make_unique<algorithm::PushAlgorithm>(init, config, shared);
          }};
}

}  // namespace ton::overlay::broadcast
