#pragma once

#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "td/utils/Time.h"
#include "td/utils/int_types.h"

namespace ton::overlay::broadcast::algorithm {

using PeerId = td::uint32;
using BroadcastId = td::uint64;
using PieceId = td::uint32;

struct Peer {
  PeerId id = 0;
  double score = 0.0;
  bool neighbour = false;
  bool persistent = false;
};

namespace msg {
struct Have {};
struct Request {};
struct Cancel {
  PieceId id = 0;
  td::uint32 context = 0;
};
struct Piece {
  PieceId id = 0;
  // Set by the transport when the receiver already had this piece.
  bool duplicate = false;
  td::uint8 hop = 0;
  // Hint from piece-coordination algorithms: this body chunk is a repair/assist push, not a
  // normal eager-tree push. Algorithms that do not care ignore it.
  bool repair = false;
  // Sender's local coverage estimate when it forwarded this piece: how many peers it has
  // already informed (push or IHAVE) about this piece. Lets receivers prune redundant emits
  // even when they haven't personally heard those IHAVEs.
  td::uint16 coverage_hint = 0;
  // Sender-local forwarding context. Receivers echo it in PRUNE/GRAFT-style simulator messages
  // so a sender can update the forwarding tree that produced this packet.
  td::uint32 context = 0;
  // Piggyback: sender's have-mask packed in 64-bit words (bit per piece_id). Receivers update
  // their peer_mask[sender] view → know what sender already has → skip redundant offers.
  // Empty vector = no piggyback (treated as zero mask).
  std::vector<td::uint64> sender_mask;
};

// Simulator-only coordination messages. Production wire carries plain Have/Request.
namespace sim {
struct HavePieces {
  std::vector<PieceId> pieces;
  td::uint32 context = 0;
  // Piggyback full mask (same semantics as Piece::sender_mask). Receivers update their
  // peer_mask view from this. Used by mask-gossip algorithms for proactive de-duplication.
  std::vector<td::uint64> sender_mask;
};
struct RequestPieces {
  std::vector<PieceId> needs;
  td::uint32 context = 0;
};
struct RequestAnyPieces {
  td::uint32 count = 0;
};
// Bucket-subscribe protocol. Source advertises availability per-bucket; receiver subscribes to
// at most K_sub sources per bucket; the subscribed source pushes future pieces in that bucket.
struct HaveBucket {
  td::uint8 bucket = 0;
  std::vector<td::uint64> sender_mask;  // piggyback have-mask for cross-edge mask propagation.
};
struct SubscribeBucket {
  td::uint8 bucket = 0;
  std::vector<td::uint64> sender_mask;
};
struct UnsubscribeBucket {
  td::uint8 bucket = 0;
};
}  // namespace sim
}  // namespace msg

using Message = std::variant<msg::Have, msg::Request, msg::Cancel, msg::Piece, msg::sim::HavePieces,
                             msg::sim::RequestPieces, msg::sim::RequestAnyPieces, msg::sim::HaveBucket,
                             msg::sim::SubscribeBucket, msg::sim::UnsubscribeBucket>;

namespace evt {
struct Publish {};
struct Receive {
  PeerId from = 0;
  Message message;
};
// Storage finished decoding. Fired by the orchestrator after a successful `Decoder::try_decode`.
struct BodyReady {};
struct Timer {};
// Peer entered the overlay or its descriptor changed. Engine has already updated
// BroadcastShared::on_peer_upsert before fanning this out.
struct PeerUpsert {
  Peer peer;
};
struct PeerRemove {
  PeerId peer = 0;
};
}  // namespace evt

using Event = std::variant<evt::Publish, evt::Receive, evt::BodyReady, evt::Timer, evt::PeerUpsert, evt::PeerRemove>;

namespace act {
struct Deliver {};
struct Send {
  PeerId peer = 0;
  Message message;
};
struct PeerFeedback {
  PeerId peer = 0;
  double delta = 0.0;
};
}  // namespace act

using Action = std::variant<act::Deliver, act::Send, act::PeerFeedback>;

struct Actions {
  std::vector<Action> items;

  Actions &operator+=(Action action) {
    items.push_back(std::move(action));
    return *this;
  }
  Actions &operator+=(Actions &&other) {
    items.insert(items.end(), std::make_move_iterator(other.items.begin()), std::make_move_iterator(other.items.end()));
    return *this;
  }

  void deliver() {
    *this += act::Deliver{};
  }
  void send(PeerId peer, Message message) {
    *this += act::Send{peer, std::move(message)};
  }
  void feedback(PeerId peer, double delta) {
    *this += act::PeerFeedback{peer, delta};
  }

  bool empty() const {
    return items.empty();
  }
  size_t size() const {
    return items.size();
  }
  const Action &operator[](size_t i) const {
    return items[i];
  }
  auto begin() const {
    return items.begin();
  }
  auto end() const {
    return items.end();
  }
};

enum class PullStatus { Unknown, Announced, Requested, Cancelled, Served };

struct PullPeerStatus {
  PeerId peer = 0;
  PullStatus status = PullStatus::Unknown;
};

// Snapshot of observable per-broadcast state. Fields that don't apply to a given algorithm stay
// at their default.
struct BroadcastStats {
  bool has_body = false;
  size_t peer_count = 0;
  std::optional<PeerId> last_piece_sender;
  // Pull-protocol fields (EagerLazy, Plumtree, OptimumP2P).
  size_t pull_tracked_count = 0;
  std::optional<PeerId> pull_current_request;
  std::vector<PullPeerStatus> pull_peers;
  bool pull_timer_scheduled = false;
  // Piece-protocol fields (OptimumP2P, Twostep).
  td::uint32 received_pieces = 0;
  td::uint32 forwarded_pieces = 0;
  size_t piece_sender_count = 0;
  size_t requested_peer_count = 0;

  PullStatus pull_status(PeerId peer) const {
    for (const auto &status : pull_peers) {
      if (status.peer == peer) {
        return status.status;
      }
    }
    return PullStatus::Unknown;
  }
};

// Pure reducer: each algorithm owns its opaque state and transitions it via `handle_event`. The
// host updates the clock via `set_now` before dispatching. Observation goes through `stats()`
// and `alarm()`.
class BroadcastAlgorithm {
 public:
  explicit BroadcastAlgorithm(bool has_body = false);
  virtual ~BroadcastAlgorithm() = default;

  void set_now(td::Timestamp now);

  const td::Timestamp &alarm() const;

  Actions handle_event(const Event &event);

  virtual BroadcastStats stats() const = 0;

 protected:
  virtual Actions step(const Event &event);

  virtual Actions on_event(const evt::Publish &event);
  virtual Actions on_event(const evt::Receive &event);
  virtual Actions on_event(const evt::BodyReady &event);
  virtual Actions on_event(const evt::Timer &event);
  // PeerUpsert / PeerRemove are notifications: Shared has already been updated by the engine.
  // Algorithms override to react with side effects or to maintain a per-broadcast peer table.
  virtual Actions on_event(const evt::PeerUpsert &event);
  virtual Actions on_event(const evt::PeerRemove &event);

  virtual Actions on_message(PeerId from, const msg::Have &message);
  virtual Actions on_message(PeerId from, const msg::Request &message);
  virtual Actions on_message(PeerId from, const msg::Cancel &message);
  virtual Actions on_message(PeerId from, const msg::Piece &message);
  virtual Actions on_message(PeerId from, const msg::sim::HavePieces &message);
  virtual Actions on_message(PeerId from, const msg::sim::RequestPieces &message);
  virtual Actions on_message(PeerId from, const msg::sim::RequestAnyPieces &message);
  virtual Actions on_message(PeerId from, const msg::sim::HaveBucket &message);
  virtual Actions on_message(PeerId from, const msg::sim::SubscribeBucket &message);
  virtual Actions on_message(PeerId from, const msg::sim::UnsubscribeBucket &message);

  td::Timestamp now() const;

  td::Timestamp alarm_ = td::Timestamp::never();

 private:
  void check_duplicate_flag(const evt::Receive &receive);

  td::Timestamp now_ = td::Timestamp::never();
  bool body_ready_ = false;
  std::unordered_set<PieceId> seen_pieces_;
};

// One-shot construction parameters, fixed for the broadcast's lifetime. `has_body=true` is a
// test seeding hook; production never sets it.
struct BroadcastInit {
  BroadcastId id = 0;
  PeerId self = 0;
  PeerId origin = 0;
  bool has_body = false;
  // Set by the engine for FEC modes that need the runtime piece count (e.g. optimum-p2p).
  td::uint32 required_pieces = 0;
};

// Convenience: emit `PeerUpsert` for each peer.
void register_peers(BroadcastAlgorithm &algorithm, std::vector<Peer> peers,
                    td::Timestamp now = td::Timestamp::now_cached());

class BroadcastShared;  // full definition below

// Adversarial: never delivers, never serves. Badness knobs:
//   attract_haves     spam Have to drag honest Requests into a black hole.
//   reply_with_cancel reply to Requests with Cancel vs silent swallow.
//   propagate_haves   relay received Have onward like a hub.
//   proactive_request spam Request — burns honest serve_budget and upload.
struct LeechConfig {
  bool attract_haves = true;
  bool reply_with_cancel = false;
  bool propagate_haves = false;
  bool proactive_request = false;
  td::uint32 metadata_peer_limit = 32;
  td::uint32 request_peer_limit = 32;
  double rebroadcast_interval = 0.500;
};
// FEC broadcast: emit `total_pieces` (= required_pieces × redundancy), forward each fresh piece
// to neighbours; deliver once `required_pieces` distinct seqnos arrive. Source emission is paced
// (`emit_batch_size` pieces per `emit_interval` seconds).
struct FecConfig {
  td::uint32 total_pieces = 1;
  td::uint32 required_pieces = 1;
  // 0 = all neighbours; >0 = pick that many random known peers per piece (K).
  td::uint32 random_per_piece = 0;
  // Of `random_per_piece`, push to the top `push_per_piece` by score; 0 = pure-random FEC.
  td::uint32 push_per_piece = 0;
  // true → same salt every chunk so all chunks go to the SAME K neighbours (legacy mainnet).
  // false → each chunk's K is independently random for path diversity.
  bool stable_per_piece = false;
  td::uint32 emit_batch_size = 4;
  double emit_interval = 0.010;
  // RaptorQ semantics: once decoded we can generate any chunk locally. true (default) →
  // BodyReady kicks off paced emission of unsent chunks.
  bool emit_after_decode = true;
  // Feedback delta emitted on each received Piece.
  double success_delta = -0.25;
};

// Per-overlay shared state for an algorithm family. The engine owns one instance per registered
// family and fans peer churn out via the hooks (once per Shared, not per active broadcast).
// Concrete subclasses live in the family's .cpp and are captured in the make_algorithm closure;
// the engine sees only this base type.
class BroadcastShared {
 public:
  virtual ~BroadcastShared() = default;
  virtual void on_peer_upsert(const Peer &) {
  }
  virtual void on_peer_remove(PeerId) {
  }
};

// Process-wide no-op Shared for stateless algorithm families.
std::shared_ptr<BroadcastShared> empty_shared();

}  // namespace ton::overlay::broadcast::algorithm
