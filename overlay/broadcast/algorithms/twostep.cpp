// Two-step push. The origin sends the body to its persistent peers (or, in piece mode, one
// piece per persistent peer). Receivers that got it from the origin re-emit to everyone else.
// No pull; no Have/Request. Used as a structural baseline against the more elaborate algorithms.

#include "overlay/broadcast/algorithms/peer-table.h"
#include "overlay/broadcast/algorithms/twostep.h"

namespace ton::overlay::broadcast::algorithm {
namespace {

using namespace internal;

class TwostepShared final : public BroadcastShared {
 public:
  void on_peer_upsert(const Peer &peer) override {
    peers.register_peer(peer);
  }
  void on_peer_remove(PeerId id) override {
    peers.erase(id);
  }

  PeerTable<PeerOnlyState> peers;
};

class TwostepAlgorithm final : public BroadcastAlgorithm {
 public:
  TwostepAlgorithm(BroadcastInit init, bool use_fec, std::shared_ptr<TwostepShared> shared)
      : BroadcastAlgorithm(init.has_body)
      , shared_(std::move(shared))
      , origin_peer_(init.origin)
      , use_fec_(use_fec)
      , has_body_(init.has_body) {
  }

  BroadcastStats stats() const override {
    return BroadcastStats{.has_body = has_body_,
                          .peer_count = shared_->peers.size(),
                          .last_piece_sender = last_piece_sender_,
                          .received_pieces = received_pieces_,
                          .forwarded_pieces = forwarded_pieces_};
  }

 private:
  std::shared_ptr<TwostepShared> shared_;
  PeerId origin_peer_;
  bool use_fec_;
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

  std::vector<PeerId> peer_ids(const std::vector<PeerOnlyState *> &peers) const {
    std::vector<PeerId> result;
    result.reserve(peers.size());
    for (const auto *peer : peers) {
      result.push_back(peer->peer.id);
    }
    return result;
  }

  std::vector<PeerId> all_peers(std::optional<PeerId> except) {
    return peer_ids(shared_->peers.filtered({.except = except}, [](const PeerOnlyState &) { return true; }));
  }

  std::vector<PeerId> persistent_peers(std::optional<PeerId> except = std::nullopt) {
    return peer_ids(
        shared_->peers.filtered({.except = except}, [](const PeerOnlyState &peer) { return peer.peer.persistent; }));
  }

  void send_to(Actions &out, const std::vector<PeerId> &peers, const Message &message) {
    for (auto peer : peers) {
      out.send(peer, message);
    }
  }

  void send_body(Actions &out, bool persistent_only, std::optional<PeerId> except) {
    send_to(out, persistent_only ? persistent_peers(except) : all_peers(except), msg::Piece{0});
  }

  void send_initial_pieces(Actions &out) {
    td::uint32 piece_id = 0;
    for (auto peer : persistent_peers()) {
      out.send(peer, msg::Piece{piece_id++});
    }
  }

  void forward_piece(Actions &out, PeerId from, PieceId piece_id) {
    if (forwarded_pieces_ == 0) {
      forwarded_pieces_ = 1;
      send_to(out, all_peers(from), msg::Piece{piece_id});
    }
  }

  Actions on_event(const evt::Publish &) override {
    Actions out;
    if (mark_body_ready(out)) {
      if (use_fec_) {
        send_initial_pieces(out);
      } else {
        send_body(out, true, std::nullopt);
      }
    }
    return out;
  }

  Actions on_event(const evt::BodyReady &) override {
    Actions out;
    mark_body_ready(out);
    return out;
  }

  Actions on_message(PeerId from, const msg::Piece &piece) override {
    Actions out;
    if (has_body_ || piece.duplicate) {
      return out;
    }
    received_pieces_++;
    last_piece_sender_ = from;
    if (from == origin_peer_) {
      if (use_fec_) {
        forward_piece(out, from, piece.id);
      } else {
        send_body(out, false, from);
      }
    }
    return out;
  }
};

}  // namespace

}  // namespace ton::overlay::broadcast::algorithm

namespace ton::overlay::broadcast {

namespace {

AlgorithmFamily make_family(std::string key, bool use_fec) {
  auto shared = std::make_shared<algorithm::TwostepShared>();
  return {.key = std::move(key), .shared = shared, .make_algorithm = [use_fec, shared](algorithm::BroadcastInit init) {
            return std::make_unique<algorithm::TwostepAlgorithm>(init, use_fec, shared);
          }};
}

}  // namespace

AlgorithmFamily make_twostep_push_family() {
  return make_family("twostep-push", /*use_fec=*/false);
}

AlgorithmFamily make_twostep_fec_family() {
  return make_family("twostep-fec", /*use_fec=*/true);
}

}  // namespace ton::overlay::broadcast
