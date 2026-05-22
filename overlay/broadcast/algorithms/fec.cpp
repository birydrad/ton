// Multi-piece flood with RaptorQ semantics. The publisher schedules emission of
// `total_pieces` pieces; receivers deliver after `required_pieces` distinct seqnos arrive,
// then take over emission of the chunks they haven't yet seen — paced one batch per
// `emit_interval`. Receiving a piece counts as "we've sent it" (forward immediately, skip in
// the schedule) so we never re-emit something already in flight.

#include <algorithm>

#include "overlay/broadcast/algorithms/fec.h"
#include "overlay/broadcast/algorithms/peer-table.h"

namespace ton::overlay::broadcast::algorithm {
namespace {

using namespace internal;

class FecShared final : public BroadcastShared {
 public:
  void on_peer_upsert(const Peer &peer) override {
    peers.register_peer(peer);
  }
  void on_peer_remove(PeerId id) override {
    peers.erase(id);
  }

  PeerTable<PeerOnlyState> peers;
};

class FecAlgorithm final : public BroadcastAlgorithm {
 public:
  FecAlgorithm(BroadcastInit init, FecConfig fec, std::shared_ptr<FecShared> shared)
      : BroadcastAlgorithm(init.has_body)
      , shared_(std::move(shared))
      , broadcast_id_(init.id)
      , has_body_(init.has_body)
      , fec_(fec)
      , emitted_piece_ids_(fec.total_pieces, false)
      , received_piece_ids_(fec.total_pieces, false)
      , received_piece_count_(0) {
    CHECK(fec_.total_pieces >= fec_.required_pieces && fec_.required_pieces >= 1);
  }

  BroadcastStats stats() const override {
    return BroadcastStats{.has_body = has_body_, .peer_count = shared_->peers.size()};
  }

 private:
  std::shared_ptr<FecShared> shared_;
  BroadcastId broadcast_id_;
  bool has_body_ = false;
  bool is_publisher_ = false;
  FecConfig fec_;
  td::uint32 rand_state_ = 0;
  td::uint32 next_to_emit_ = 0;
  std::vector<bool> emitted_piece_ids_;
  std::vector<bool> received_piece_ids_;
  td::uint32 received_piece_count_;

  std::vector<PeerId> peers_for_piece(std::optional<PeerId> except, td::uint32 seqno) {
    // random_per_piece == 0 → all neighbours in table order. >0 → K uniformly-random neighbours
    // (per-chunk salt mixed with broadcast_id_ so concurrent broadcasts pick independent K).
    // stable_per_piece holds the salt fixed across all chunks of a broadcast (old-FEC behaviour).
    PeerRankOptions options{.except = except, .sort_by_score = false};
    if (fec_.random_per_piece > 0) {
      auto broadcast_salt = static_cast<td::uint32>(broadcast_id_);
      options.salt = fec_.stable_per_piece ? broadcast_salt : (seqno * 2654435761u + (rand_state_++) + broadcast_salt);
      options.shuffle_equal_scores = true;
      options.limit = fec_.random_per_piece;
    }
    auto selected = shared_->peers.ranked(options, [](const PeerOnlyState &p) { return p.peer.neighbour; });
    std::vector<PeerId> out;
    out.reserve(selected.size());
    for (auto *p : selected)
      out.push_back(p->peer.id);
    return out;
  }

  void emit_piece(Actions &out, td::uint32 piece_id, std::optional<PeerId> except) {
    if (emitted_piece_ids_[piece_id]) {
      return;
    }
    emitted_piece_ids_[piece_id] = true;
    for (auto peer : peers_for_piece(except, piece_id)) {
      out.send(peer, msg::Piece{.id = piece_id});
    }
  }

  // Emit up to `emit_batch_size` not-yet-emitted pieces; arm the alarm if more remain.
  void emit_batch(Actions &out) {
    if (!has_body_ || (!is_publisher_ && !fec_.emit_after_decode)) {
      return;
    }
    auto remaining = std::max<td::uint32>(1, fec_.emit_batch_size);
    while (next_to_emit_ < fec_.total_pieces && remaining > 0) {
      auto piece_id = next_to_emit_++;
      if (!emitted_piece_ids_[piece_id]) {
        emit_piece(out, piece_id, std::nullopt);
        remaining--;
      }
    }
    if (next_to_emit_ < fec_.total_pieces) {
      alarm_ = td::Timestamp::in(fec_.emit_interval, now());
    }
  }

  Actions on_event(const evt::Publish &) override {
    Actions out;
    if (!has_body_) {
      is_publisher_ = true;
      has_body_ = true;
      out.deliver();
      emit_batch(out);
    }
    return out;
  }

  Actions on_event(const evt::BodyReady &) override {
    Actions out;
    if (!has_body_) {
      has_body_ = true;
      out.deliver();
      if (fec_.emit_after_decode) {
        emit_batch(out);
      }
    }
    return out;
  }

  Actions on_event(const evt::Timer &) override {
    Actions out;
    emit_batch(out);
    return out;
  }

  Actions on_message(PeerId from, const msg::Piece &piece) override {
    Actions out;
    out.feedback(from, fec_.success_delta);
    if (piece.id >= fec_.total_pieces) {
      return out;
    }
    if (!received_piece_ids_[piece.id]) {
      received_piece_ids_[piece.id] = true;
      received_piece_count_++;
    }
    if (piece.duplicate)
      return out;
    emit_piece(out, piece.id, from);
    return out;
  }
};

}  // namespace

}  // namespace ton::overlay::broadcast::algorithm

namespace ton::overlay::broadcast {

AlgorithmFamily make_fec_family(algorithm::FecConfig fec) {
  auto shared = std::make_shared<algorithm::FecShared>();
  return {.key = "fec", .shared = shared, .make_algorithm = [fec, shared](algorithm::BroadcastInit init) {
            return std::make_unique<algorithm::FecAlgorithm>(init, fec, shared);
          }};
}

}  // namespace ton::overlay::broadcast
