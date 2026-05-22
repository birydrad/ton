// GossipMaskFec — stateless-across-broadcasts gossip with piggyback have-masks. Each Piece /
// HavePieces carries the sender's current have-mask (packed uint64 words, bit-per-piece).
// Receivers update their per-broadcast `peer_mask[from]` view. Outgoing K1 selection ranks
// candidates by how many fresh pieces we'd give them and skips peers whose mask already shows
// they have the piece.

#include "overlay/broadcast/algorithms/gossip-mask-fec.h"
#include "overlay/broadcast/algorithms/peer-table.h"

namespace ton::overlay::broadcast::algorithm {
namespace {

using namespace internal;

using Mask = std::vector<td::uint64>;

inline std::size_t mask_words(td::uint32 piece_count) {
  return (piece_count + 63) / 64;
}

inline bool mask_get(const Mask &m, td::uint32 piece_id) {
  auto w = piece_id / 64, b = piece_id % 64;
  if (w >= m.size())
    return false;
  return (m[w] >> b) & 1u;
}

inline void mask_set(Mask &m, td::uint32 piece_id) {
  auto w = piece_id / 64, b = piece_id % 64;
  if (w >= m.size())
    m.resize(w + 1, 0);
  m[w] |= (td::uint64{1} << b);
}

inline void mask_or(Mask &dst, const Mask &src) {
  if (dst.size() < src.size())
    dst.resize(src.size(), 0);
  for (size_t i = 0; i < src.size(); i++)
    dst[i] |= src[i];
}

inline td::uint32 mask_popcount_and_not(const Mask &a, const Mask &b) {
  td::uint32 c = 0;
  auto n = std::max(a.size(), b.size());
  for (size_t i = 0; i < n; i++) {
    auto av = i < a.size() ? a[i] : 0;
    auto bv = i < b.size() ? b[i] : 0;
    c += static_cast<td::uint32>(__builtin_popcountll(av & ~bv));
  }
  return c;
}

class GossipMaskFecShared final : public BroadcastShared {
 public:
  void on_peer_upsert(const Peer &peer) override {
    peers.register_peer(peer);
  }
  void on_peer_remove(PeerId id) override {
    peers.erase(id);
  }

  PeerTable<PeerOnlyState> peers;
};

class GossipMaskFecAlgorithm final : public BroadcastAlgorithm {
 public:
  GossipMaskFecAlgorithm(BroadcastInit init, GossipMaskFecConfig config, std::shared_ptr<GossipMaskFecShared> shared)
      : BroadcastAlgorithm(init.has_body)
      , config_(config)
      , shared_(std::move(shared))
      , self_(init.self)
      , has_body_(init.has_body)
      , mask_words_count_(mask_words(config.fec.total_pieces))
      , pieces_(config.fec.total_pieces)
      , received_piece_hops_(config.fec.total_pieces, 0) {
    CHECK(config_.fec.total_pieces >= config_.fec.required_pieces && config_.fec.required_pieces >= 1);
    my_mask_.assign(mask_words_count_, 0);
  }

  BroadcastStats stats() const override {
    return BroadcastStats{
        .has_body = has_body_, .peer_count = shared_->peers.size(), .last_piece_sender = last_piece_sender_};
  }

 private:
  struct PieceState {
    bool received = false;
    bool emitted = false;
    bool requested = false;  // we've already sent Request to some announcer.
  };

  GossipMaskFecConfig config_;
  std::shared_ptr<GossipMaskFecShared> shared_;
  PeerId self_ = 0;
  bool has_body_ = false;
  bool is_publisher_ = false;
  size_t mask_words_count_;
  std::vector<PieceState> pieces_;
  std::vector<td::uint8> received_piece_hops_;
  Mask my_mask_;
  std::optional<PeerId> last_piece_sender_;
  std::unordered_map<PeerId, Mask> peer_mask_;
  // Deferred announcements: piece-ids freshly received since the last batched IHAVE flush.
  std::vector<PieceId> pending_announce_;
  td::Timestamp announce_deadline_ = td::Timestamp::never();
  // Tail-repair: after decode, periodically broadcast current sender_mask to neighbours.
  td::Timestamp tail_repair_deadline_ = td::Timestamp::never();
  td::uint32 tail_repair_rounds_done_ = 0;
  bool tail_repair_started_ = false;

  void mark_have(td::uint32 piece_id) {
    mask_set(my_mask_, piece_id);
    pieces_[piece_id].received = true;
  }

  void mark_have(td::uint32 piece_id, td::uint8 received_hop) {
    mark_have(piece_id);
    received_piece_hops_[piece_id] = received_hop;
  }

  void update_peer_mask(PeerId peer, const Mask &received_mask) {
    auto &m = peer_mask_[peer];
    if (m.empty())
      m.assign(mask_words_count_, 0);
    mask_or(m, received_mask);
  }

  void update_peer_mask_bit(PeerId peer, td::uint32 piece_id) {
    auto &m = peer_mask_[peer];
    if (m.empty())
      m.assign(mask_words_count_, 0);
    mask_set(m, piece_id);
  }

  void emit_piece_push_only(Actions &out, td::uint32 piece_id, std::optional<PeerId> except, td::uint8 received_hop) {
    // K1 push portion only — IHAVE is batched separately via emit_have_batch.
    auto &state = pieces_[piece_id];
    if (state.emitted)
      return;
    state.emitted = true;
    auto candidates = shared_->peers.ranked({.except = except, .limit = std::numeric_limits<size_t>::max()},
                                            [](const PeerOnlyState &p) { return p.peer.neighbour; });
    if (candidates.empty())
      return;
    struct Scored {
      PeerId id;
      td::uint32 missing_count;
    };
    std::vector<Scored> scored;
    scored.reserve(candidates.size());
    for (auto *p : candidates) {
      auto pid = p->peer.id;
      auto it = peer_mask_.find(pid);
      bool known_has = (it != peer_mask_.end()) && mask_get(it->second, piece_id);
      if (known_has)
        continue;
      td::uint32 missing = 0;
      if (it != peer_mask_.end()) {
        missing = mask_popcount_and_not(my_mask_, it->second);
      } else {
        for (auto w : my_mask_)
          missing += static_cast<td::uint32>(__builtin_popcountll(w));
      }
      scored.push_back({pid, missing});
    }
    std::stable_sort(scored.begin(), scored.end(),
                     [](const Scored &a, const Scored &b) { return a.missing_count > b.missing_count; });
    auto k1 = std::min<size_t>(config_.push_per_piece, scored.size());
    auto hop_out = received_hop < 255 ? static_cast<td::uint8>(received_hop + 1) : static_cast<td::uint8>(255);
    for (size_t i = 0; i < k1; i++) {
      out.send(scored[i].id, msg::Piece{.id = piece_id, .hop = hop_out, .sender_mask = my_mask_});
      update_peer_mask_bit(scored[i].id, piece_id);
    }
  }

  // After processing all fresh pieces, send ONE HavePieces per peer batching every piece in
  // our have-mask that we believe the peer doesn't already have. Cuts control overhead 25×.
  void emit_have_batch(Actions &out, std::optional<PeerId> except) {
    auto candidates = shared_->peers.ranked({.except = except, .limit = std::numeric_limits<size_t>::max()},
                                            [](const PeerOnlyState &p) { return p.peer.neighbour; });
    for (auto *p : candidates) {
      auto pid = p->peer.id;
      auto it = peer_mask_.find(pid);
      std::vector<PieceId> need;
      need.reserve(pieces_.size());
      for (td::uint32 i = 0; i < pieces_.size(); i++) {
        if (!pieces_[i].received)
          continue;
        if (it != peer_mask_.end() && mask_get(it->second, i))
          continue;
        need.push_back(i);
      }
      if (need.empty())
        continue;
      out.send(pid, msg::sim::HavePieces{.pieces = std::move(need), .sender_mask = my_mask_});
    }
  }

  void emit_all(Actions &out) {
    if (is_publisher_ && config_.source_partition_fanout > 0) {
      emit_all_partitioned(out);
      return;
    }
    for (td::uint32 i = 0; i < pieces_.size(); i++)
      emit_piece_push_only(out, i, std::nullopt, /*received_hop=*/0);
    emit_have_batch(out, std::nullopt);
  }

  // Source mode: piece i → neighbour (i % source_partition_fanout). Each direct neighbour gets
  // a disjoint subset of pieces. No source-side duplication.
  void emit_all_partitioned(Actions &out) {
    auto fanout = config_.source_partition_fanout;
    auto candidates = shared_->peers.ranked({.limit = std::numeric_limits<size_t>::max()},
                                            [](const PeerOnlyState &p) { return p.peer.neighbour; });
    if (candidates.empty())
      return;
    std::vector<PeerId> partition(std::min<std::size_t>(fanout, candidates.size()));
    for (std::size_t i = 0; i < partition.size(); i++) {
      partition[i] = candidates[i]->peer.id;
    }
    for (td::uint32 i = 0; i < pieces_.size(); i++) {
      auto &state = pieces_[i];
      if (state.emitted)
        continue;
      state.emitted = true;
      auto dst = partition[i % partition.size()];
      out.send(dst, msg::Piece{.id = i, .hop = 1, .sender_mask = my_mask_});
      update_peer_mask_bit(dst, i);
    }
    emit_have_batch(out, std::nullopt);
  }

  // Called when a single fresh piece arrives — push the new piece via K1 and announce
  // ONLY the new piece (compact HavePieces) to non-pushed neighbours. If announce_delay > 0,
  // accumulate the piece-id into pending_announce_ and let the alarm flush them as a batch.
  void emit_piece(Actions &out, td::uint32 piece_id, std::optional<PeerId> except, td::uint8 received_hop) {
    emit_piece_push_only(out, piece_id, except, received_hop);
    if (config_.announce_delay > 0.0) {
      pending_announce_.push_back(piece_id);
      schedule_announce_alarm();
      return;
    }
    flush_announce(out, piece_id, except);
  }

  void flush_announce(Actions &out, td::uint32 piece_id, std::optional<PeerId> except) {
    auto candidates = shared_->peers.ranked({.except = except, .limit = std::numeric_limits<size_t>::max()},
                                            [](const PeerOnlyState &p) { return p.peer.neighbour; });
    for (auto *p : candidates) {
      auto pid = p->peer.id;
      auto it = peer_mask_.find(pid);
      if (it != peer_mask_.end() && mask_get(it->second, piece_id))
        continue;
      out.send(pid, msg::sim::HavePieces{.pieces = {piece_id}, .sender_mask = my_mask_});
    }
  }

  // Flush all pending IHAVE-batches to neighbours: one HavePieces per neighbour containing the
  // subset of pending pieces they don't yet have (per our mask view).
  void flush_pending_announce(Actions &out) {
    if (pending_announce_.empty())
      return;
    auto pending = std::move(pending_announce_);
    pending_announce_.clear();
    announce_deadline_ = td::Timestamp::never();
    auto candidates = shared_->peers.ranked({.limit = std::numeric_limits<size_t>::max()},
                                            [](const PeerOnlyState &p) { return p.peer.neighbour; });
    for (auto *p : candidates) {
      auto pid = p->peer.id;
      auto it = peer_mask_.find(pid);
      std::vector<PieceId> need;
      need.reserve(pending.size());
      for (auto piece_id : pending) {
        if (it != peer_mask_.end() && mask_get(it->second, piece_id))
          continue;
        need.push_back(piece_id);
      }
      if (need.empty())
        continue;
      out.send(pid, msg::sim::HavePieces{.pieces = std::move(need), .sender_mask = my_mask_});
    }
  }

  void schedule_announce_alarm() {
    auto target = td::Timestamp::in(config_.announce_delay, now());
    if (!announce_deadline_ || target.at() < announce_deadline_.at()) {
      announce_deadline_ = target;
      if (!alarm_ || announce_deadline_.at() < alarm_.at()) {
        alarm_ = announce_deadline_;
      }
    }
  }

  Actions on_event(const evt::Publish &) override {
    Actions out;
    if (!has_body_) {
      has_body_ = true;
      is_publisher_ = true;
      for (td::uint32 i = 0; i < pieces_.size(); i++)
        mark_have(i, /*received_hop=*/0);
      out.deliver();
      emit_all(out);
    }
    return out;
  }

  Actions on_event(const evt::BodyReady &) override {
    Actions out;
    if (!has_body_) {
      has_body_ = true;
      for (td::uint32 i = 0; i < pieces_.size(); i++)
        mark_have(i, /*received_hop=*/0);
      out.deliver();
      if (config_.fec.emit_after_decode)
        emit_all(out);
    }
    return out;
  }

  Actions on_message(PeerId from, const msg::Piece &piece) override {
    Actions out;
    if (piece.id >= pieces_.size())
      return out;
    update_peer_mask(from, piece.sender_mask);
    update_peer_mask_bit(from, piece.id);
    if (piece.duplicate || pieces_[piece.id].received)
      return out;
    mark_have(piece.id, piece.hop);
    last_piece_sender_ = from;
    emit_piece(out, piece.id, from, piece.hop);
    return out;
  }

  Actions on_message(PeerId from, const msg::sim::HavePieces &have) override {
    Actions out;
    update_peer_mask(from, have.sender_mask);
    std::vector<PieceId> immediate_request;
    for (auto piece_id : have.pieces) {
      if (piece_id >= pieces_.size())
        continue;
      update_peer_mask_bit(from, piece_id);
      auto &state = pieces_[piece_id];
      if (state.received || state.requested)
        continue;
      state.requested = true;
      immediate_request.push_back(piece_id);
    }
    if (!immediate_request.empty()) {
      out.send(from, msg::sim::RequestPieces{.needs = std::move(immediate_request)});
    }
    return out;
  }

  Actions on_message(PeerId from, const msg::sim::RequestPieces &request) override {
    Actions out;
    for (auto piece_id : request.needs) {
      if (piece_id >= pieces_.size())
        continue;
      if (pieces_[piece_id].received) {
        auto hop = piece_id < received_piece_hops_.size() ? received_piece_hops_[piece_id] : 0;
        auto hop_out = hop < 255 ? static_cast<td::uint8>(hop + 1) : static_cast<td::uint8>(255);
        out.send(from, msg::Piece{.id = piece_id, .hop = hop_out, .sender_mask = my_mask_});
        update_peer_mask_bit(from, piece_id);
      }
    }
    return out;
  }

  Actions on_message(PeerId, const msg::Have &) override {
    return {};
  }
  Actions on_message(PeerId, const msg::Request &) override {
    return {};
  }
  Actions on_message(PeerId, const msg::Cancel &) override {
    return {};
  }

  Actions on_event(const evt::Timer &) override {
    Actions out;
    flush_pending_announce(out);
    if (announce_deadline_ && now().at() >= announce_deadline_.at()) {
      announce_deadline_ = td::Timestamp::never();
    }
    alarm_ = announce_deadline_ ? announce_deadline_ : td::Timestamp::never();
    return out;
  }
};

}  // namespace

}  // namespace ton::overlay::broadcast::algorithm

namespace ton::overlay::broadcast {

AlgorithmFamily make_gossip_mask_fec_family(GossipMaskFecConfig config) {
  auto shared = std::make_shared<algorithm::GossipMaskFecShared>();
  return {
      .key = "gossip-mask-fec", .shared = shared, .make_algorithm = [config, shared](algorithm::BroadcastInit init) {
        return std::make_unique<algorithm::GossipMaskFecAlgorithm>(init, config, shared);
      }};
}

}  // namespace ton::overlay::broadcast
