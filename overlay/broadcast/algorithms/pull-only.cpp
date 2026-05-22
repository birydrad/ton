// PullOnly — pure-pull whole-body broadcast. No FEC chunking, no eager push.
//   1. When we have the body, send Have to K random neighbours (lazy advertisement).
//   2. When we receive Have from peer P (and don't have body yet), Request body from P.
//      Pull from the FIRST announcer (FIFO order of arrival); ignore later Haves.
//   3. When we receive Request, serve Piece if we have body.
//   4. When we receive Piece, mark delivered, then start step 1.
//
// No scoring, no PRUNE/Cancel, no retry logic — minimal control surface.

#include "overlay/broadcast/algorithms/peer-table.h"
#include "overlay/broadcast/algorithms/pull-only.h"

namespace ton::overlay::broadcast::algorithm {
namespace {

using namespace internal;

class PullOnlyShared final : public BroadcastShared {
 public:
  void on_peer_upsert(const Peer &peer) override {
    peers.register_peer(peer);
  }
  void on_peer_remove(PeerId id) override {
    peers.erase(id);
  }

  PeerTable<PeerOnlyState> peers;
};

class PullOnlyAlgorithm final : public BroadcastAlgorithm {
 public:
  PullOnlyAlgorithm(BroadcastInit init, PullOnlyConfig config, std::shared_ptr<PullOnlyShared> shared)
      : BroadcastAlgorithm(init.has_body)
      , config_(config)
      , shared_(std::move(shared))
      , broadcast_id_(init.id)
      , has_body_(init.has_body) {
  }

  BroadcastStats stats() const override {
    return BroadcastStats{
        .has_body = has_body_, .peer_count = shared_->peers.size(), .last_piece_sender = last_piece_sender_};
  }

 private:
  PullOnlyConfig config_;
  std::shared_ptr<PullOnlyShared> shared_;
  BroadcastId broadcast_id_;
  bool has_body_ = false;
  std::optional<PeerId> last_piece_sender_;
  bool requested_ = false;  // we've already issued a Request — ignore subsequent Haves.
  td::uint32 rand_salt_ = 1;

  // Send a Have to K random neighbours.
  void announce_have(Actions &out, std::optional<PeerId> except) {
    auto salt = static_cast<td::uint32>(broadcast_id_) + (rand_salt_++);
    auto targets = shared_->peers.ranked(
        {.except = except, .shuffle_equal_scores = true, .sort_by_score = false, .salt = salt, .limit = config_.k},
        [](const PeerOnlyState &p) { return p.peer.neighbour; });
    for (auto *peer : targets) {
      out.send(peer->peer.id, msg::Have{});
    }
  }

  Actions on_event(const evt::Publish &) override {
    Actions out;
    if (!has_body_) {
      has_body_ = true;
      out.deliver();
      announce_have(out, std::nullopt);
    }
    return out;
  }

  Actions on_event(const evt::BodyReady &) override {
    Actions out;
    if (!has_body_) {
      has_body_ = true;
      out.deliver();
      announce_have(out, std::nullopt);
    }
    return out;
  }

  Actions on_message(PeerId from, const msg::Have &) override {
    Actions out;
    if (has_body_ || requested_)
      return out;
    requested_ = true;
    out.send(from, msg::Request{});
    return out;
  }

  Actions on_message(PeerId from, const msg::Request &) override {
    Actions out;
    if (has_body_) {
      out.send(from, msg::Piece{});  // Whole body as a single Piece. Piece id = 0.
    }
    return out;
  }

  Actions on_message(PeerId from, const msg::Piece &piece) override {
    Actions out;
    if (has_body_ || piece.duplicate)
      return out;
    has_body_ = true;
    last_piece_sender_ = from;
    out.deliver();
    announce_have(out, from);
    return out;
  }

  Actions on_message(PeerId, const msg::Cancel &) override {
    return {};
  }
};

}  // namespace

}  // namespace ton::overlay::broadcast::algorithm

namespace ton::overlay::broadcast {

AlgorithmFamily make_pull_only_family(PullOnlyConfig config) {
  auto shared = std::make_shared<algorithm::PullOnlyShared>();
  return {.key = "pull-only", .shared = shared, .make_algorithm = [config, shared](algorithm::BroadcastInit init) {
            return std::make_unique<algorithm::PullOnlyAlgorithm>(init, config, shared);
          }};
}

}  // namespace ton::overlay::broadcast
