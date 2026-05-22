// OptimumP2P — theoretical lower-bound reference. The publisher emits one piece per active edge;
// receivers re-emit derived pieces to a small score-ranked subset. Not deployable in production
// because it requires per-piece signatures the rest of the pipeline doesn't provide.

#include "overlay/broadcast/algorithms/optimum-p2p.h"
#include "overlay/broadcast/algorithms/peer-table.h"

namespace ton::overlay::broadcast::algorithm {
namespace {

using namespace internal;

class OptimumP2PAlgorithm final : public BroadcastAlgorithm {
 public:
  OptimumP2PAlgorithm(BroadcastInit init, OptimumP2PConfig config)
      : BroadcastAlgorithm(init.has_body), config_(config), peers_(init.self, init.id), has_body_(init.has_body) {
  }

 private:
  // Locally-generated piece IDs live in the high half of the PieceId space so they don't collide
  // with source-emitted piece IDs (which start at 0). Capped at 65k local pieces per node.
  static constexpr td::uint32 kLocalPieceIdBase = 1u << 31;
  static constexpr td::uint32 kLocalPieceIdMaxOffset = 1u << 16;

  static PieceId local_piece_id(td::uint32 offset) {
    CHECK(offset < kLocalPieceIdMaxOffset);
    return kLocalPieceIdBase + offset;
  }

 public:
  BroadcastStats stats() const override {
    auto pull_peers = pull_statuses();
    auto piece_sender_count = peers_.count([](const PeerState &peer) { return peer.piece_sender; });
    auto requested_peer_count = peers_.count([](const PeerState &peer) { return peer.piece_requested; });
    return BroadcastStats{.has_body = has_body_,
                          .peer_count = peers_.size(),
                          .last_piece_sender = last_piece_sender_,
                          .pull_tracked_count = pull_peers.size(),
                          .pull_peers = std::move(pull_peers),
                          .received_pieces = received_pieces_,
                          .forwarded_pieces = forwarded_pieces_,
                          .piece_sender_count = piece_sender_count,
                          .requested_peer_count = requested_peer_count};
  }

 private:
  struct PeerState {
    Peer peer;
    PullStatus pull = PullStatus::Unknown;
    bool piece_sender = false;
    bool piece_requested = false;

    bool served() const {
      return pull == PullStatus::Served;
    }

    bool can_request_piece() const {
      return !piece_requested;
    }

    void mark_piece_sender() {
      piece_sender = true;
    }

    void mark_piece_requested() {
      CHECK(!piece_requested);
      piece_requested = true;
    }

    void mark_served() {
      CHECK(pull != PullStatus::Served);
      pull = PullStatus::Served;
    }
  };

  std::vector<PullPeerStatus> pull_statuses() const {
    auto peers =
        peers_.filtered(PeerRankOptions{}, [](const PeerState &peer) { return peer.pull != PullStatus::Unknown; });
    std::vector<PullPeerStatus> result;
    result.reserve(peers.size());
    for (const auto *peer : peers) {
      result.push_back({peer->peer.id, peer->pull});
    }
    return result;
  }

  OptimumP2PConfig config_;
  PeerTable<PeerState> peers_;
  bool has_body_ = false;
  std::optional<PeerId> last_piece_sender_;
  td::uint32 received_pieces_ = 0;
  td::uint32 forwarded_pieces_ = 0;

  bool mark_body_ready(Actions &out) {
    if (has_body_) {
      return false;
    }
    has_body_ = true;
    out.deliver();
    return true;
  }

  size_t requested_peer_count() const {
    return peers_.count([](const PeerState &peer) { return peer.piece_requested; });
  }

  std::vector<PeerState *> metadata_peers() {
    return peers_.ranked({.limit = config_.metadata_peer_limit});
  }

  std::vector<PeerState *> piece_peers(td::uint32 salt, std::optional<PeerId> except) {
    return peers_.ranked({.except = except,
                          .score_granularity = config_.required_pieces > 1
                                                   ? std::optional<double>{std::abs(config_.peer_success_delta) / 2.0}
                                                   : std::nullopt,
                          .shuffle_equal_scores = config_.required_pieces > 1,
                          .salt = salt,
                          .limit = config_.piece_peer_limit},
                         [](const PeerState &peer) { return peer.peer.neighbour; });
  }

  void send_to(Actions &out, const std::vector<PeerState *> &peers, const Message &message) {
    for (auto *peer : peers) {
      out.send(peer->peer.id, message);
    }
  }

  void announce_pieces(Actions &out) {
    send_to(out, metadata_peers(), msg::Have{});
  }

  void send_initial_pieces(Actions &out) {
    for (td::uint32 piece_id = 0; piece_id < config_.initial_pieces; piece_id++) {
      send_to(out, piece_peers(piece_id, std::nullopt), msg::Piece{piece_id});
    }
  }

  Actions on_event(const evt::Publish &) override {
    Actions out;
    // Source's body is set via BroadcastInit, so `mark_body_ready` returns false here — but we
    // still need to kick off the announce + initial-pieces fan-out on the first Publish.
    if (!has_body_) {
      has_body_ = true;
      out.deliver();
    }
    announce_pieces(out);
    send_initial_pieces(out);
    return out;
  }

  Actions on_event(const evt::BodyReady &) override {
    Actions out;
    if (mark_body_ready(out)) {
      announce_pieces(out);
    }
    return out;
  }

  Actions on_event(const evt::PeerUpsert &event) override {
    peers_.register_peer(event.peer);
    return {};
  }

  Actions on_message(PeerId from, const msg::Have &) override {
    Actions out;
    if (has_body_ || requested_peer_count() >= config_.piece_request_peer_limit) {
      return out;
    }
    auto &peer = peers_.require(from);
    if (peer.can_request_piece()) {
      peer.mark_piece_requested();
      out.send(peer.peer.id, msg::Request{});
    }
    return out;
  }

  Actions on_message(PeerId from, const msg::Request &) override {
    Actions out;
    auto &state = peers_.require(from);
    if (state.served()) {
      return out;
    }
    if (!has_body_ && received_pieces_ < config_.forward_threshold) {
      return out;
    }
    state.mark_served();
    auto count = has_body_ ? config_.required_pieces : td::uint32{1};
    auto first_piece = local_piece_id(received_pieces_);
    for (td::uint32 i = 0; i < count; i++) {
      out.send(state.peer.id, msg::Piece{first_piece + i});
    }
    return out;
  }

  Actions on_message(PeerId from, const msg::Piece &piece) override {
    Actions out;
    if (has_body_ || piece.duplicate) {
      return out;
    }
    received_pieces_++;
    last_piece_sender_ = from;
    if (!peers_.is_self(from)) {
      auto &peer = peers_.require(from);
      if (!peer.piece_sender) {
        peer.mark_piece_sender();
        out.feedback(peer.peer.id, config_.peer_success_delta);
      }
    }
    if (received_pieces_ < config_.forward_threshold || forwarded_pieces_ >= config_.initial_pieces) {
      return out;
    }
    auto out_piece = local_piece_id(forwarded_pieces_++);
    send_to(out, piece_peers(out_piece, from), msg::Piece{.id = out_piece});
    return out;
  }
};

}  // namespace

}  // namespace ton::overlay::broadcast::algorithm

namespace ton::overlay::broadcast {

AlgorithmFamily make_optimum_p2p_family(OptimumP2PConfig base) {
  return {.key = "optimum-p2p",
          .shared = algorithm::empty_shared(),
          .make_algorithm = [base](algorithm::BroadcastInit init) {
            // Per-broadcast required_pieces from BroadcastInit (filled by session from
            // mode::OptimumP2P::required_pieces).
            auto config = base;
            config.required_pieces = init.required_pieces;
            config.initial_pieces = init.required_pieces;
            return std::make_unique<algorithm::OptimumP2PAlgorithm>(init, config);
          }};
}

}  // namespace ton::overlay::broadcast
