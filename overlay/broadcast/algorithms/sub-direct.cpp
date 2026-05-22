// SubDirect — whole-body subscribe/push broadcast with persistent per-edge subscriber state.
//
// Protocol:
//   1. When we have the body, send Piece to every peer currently subscribed to us; send Have
//      to every other neighbour.
//   2. When we receive Have from N and don't have the body, send Request (= subscribe) to N.
//   3. When we receive Request from N, mark N as subscribed-to-us until now + lease_seconds.
//      If we have the body, send Piece to N immediately.
//   4. When we receive Piece from N (fresh): deliver, run step 1, AND refresh our subscription
//      to N (send Request again) IF this is the first sender that delivered to us this round.
//      Earliest-deliverer = the peer that wins our subscription for future broadcasts.
//
// Cross-broadcast state ("which peers are subscribed to me, until when") lives in
// SubDirectShared::edges, owned by the family and shared by every broadcast on this node.

#include "overlay/broadcast/algorithms/peer-table.h"
#include "overlay/broadcast/algorithms/sub-direct.h"

namespace ton::overlay::broadcast {

namespace {

using namespace algorithm;
using namespace algorithm::internal;

constexpr double kSubscriptionLease = 10.0;  // peer is subscribed for this many seconds

// Family-private Shared. Key = peer id (the owner Shared belongs to one node, so self is
// implicit). Value = wall-clock instant at which the peer stops being subscribed to us.
struct SubDirectShared : BroadcastShared {
  std::unordered_map<PeerId, double> subscribers_until;

  void on_peer_remove(PeerId pid) override {
    subscribers_until.erase(pid);
  }
};

class SubDirectAlgorithm final : public BroadcastAlgorithm {
 public:
  SubDirectAlgorithm(BroadcastInit init, SubDirectShared &shared, bool prune_on_duplicate)
      : BroadcastAlgorithm(init.has_body)
      , peers_(init.self, init.id)
      , shared_(shared)
      , prune_on_duplicate_(prune_on_duplicate)
      , has_body_(init.has_body) {
  }

  BroadcastStats stats() const override {
    return BroadcastStats{.has_body = has_body_, .peer_count = peers_.size(), .last_piece_sender = last_piece_sender_};
  }

 private:
  PeerTable<PeerOnlyState> peers_;
  SubDirectShared &shared_;
  bool prune_on_duplicate_ = false;
  bool has_body_ = false;
  std::optional<PeerId> last_piece_sender_;
  std::optional<PeerId> first_sender_;                    // who delivered fresh body to us this round
  std::unordered_set<PeerId> subscribe_sent_this_round_;  // dedup of outgoing Subscribe this round
  bool subscribed_for_this_round_ = false;                // we've already chosen one peer to subscribe to this round

  bool is_subscribed_to_us(PeerId peer) const {
    auto it = shared_.subscribers_until.find(peer);
    if (it == shared_.subscribers_until.end()) {
      return false;
    }
    return it->second > now().at();
  }

  void mark_subscriber(PeerId peer) {
    shared_.subscribers_until[peer] = now().at() + kSubscriptionLease;
  }

  // Step 1 of the protocol: push body to subscribed peers, Have to the rest. Skips `except`.
  void broadcast(Actions &out, std::optional<PeerId> except) {
    auto neighbours = peers_.ranked(
        {.except = except, .salt = 0, .shuffle_equal_scores = false, .limit = std::numeric_limits<size_t>::max()},
        [](const PeerOnlyState &p) { return p.peer.neighbour; });
    for (auto *peer : neighbours) {
      auto pid = peer->peer.id;
      if (is_subscribed_to_us(pid)) {
        out.send(pid, msg::Piece{});  // active push (body)
      } else {
        out.send(pid, msg::Have{});  // lazy advertise
      }
    }
  }

  void send_subscribe(Actions &out, PeerId to) {
    if (!subscribe_sent_this_round_.insert(to).second) {
      return;
    }
    out.send(to, msg::Request{});
  }

  Actions on_event(const evt::Publish &) override {
    Actions out;
    if (!has_body_) {
      has_body_ = true;
      out.deliver();
      broadcast(out, std::nullopt);
    }
    return out;
  }

  Actions on_event(const evt::BodyReady &) override {
    Actions out;
    if (!has_body_) {
      has_body_ = true;
      out.deliver();
      broadcast(out, std::nullopt);
    }
    return out;
  }

  Actions on_event(const evt::PeerUpsert &event) override {
    peers_.register_peer(event.peer);
    return {};
  }

  Actions on_event(const evt::Timer &) override {
    return {};
  }

  Actions on_message(PeerId from, const msg::Have &) override {
    Actions out;
    if (has_body_) {
      return out;
    }
    // Only subscribe to the FIRST Have sender this round — same first-announcer rule as
    // pull-K10. Subscribing to every Have sender would have every upstream peer push body
    // to us, with all but one landing as duplicate (round-0 overhead bug).
    if (subscribed_for_this_round_) {
      return out;
    }
    subscribed_for_this_round_ = true;
    send_subscribe(out, from);
    return out;
  }

  Actions on_message(PeerId from, const msg::Request &) override {
    Actions out;
    mark_subscriber(from);
    if (has_body_) {
      out.send(from, msg::Piece{});
    }
    return out;
  }

  Actions on_message(PeerId from, const msg::Piece &piece) override {
    Actions out;
    if (piece.duplicate || has_body_) {
      // PRUNE: tell the sender we already had this body. They'll unsubscribe us.
      if (prune_on_duplicate_) {
        out.send(from, msg::Cancel{});
      }
      return out;
    }
    has_body_ = true;
    last_piece_sender_ = from;
    out.deliver();
    // First fresh sender wins our future-broadcast subscription. Refresh by sending Subscribe.
    if (!first_sender_.has_value()) {
      first_sender_ = from;
      send_subscribe(out, from);
    }
    broadcast(out, from);
    return out;
  }

  Actions on_message(PeerId from, const msg::Cancel &) override {
    // Peer told us our push was redundant. Drop them from our subscriber set so we stop pushing
    // body to them on future broadcasts.
    if (prune_on_duplicate_) {
      shared_.subscribers_until.erase(from);
    }
    return {};
  }
};

}  // namespace

AlgorithmFamily make_sub_direct_family(bool prune_on_duplicate) {
  auto shared = std::make_shared<SubDirectShared>();
  std::string key = prune_on_duplicate ? "subscribe-prune" : "subscribe";
  return {.key = std::move(key),
          .shared = shared,
          .make_algorithm = [shared,
                             prune_on_duplicate](algorithm::BroadcastInit init) -> std::unique_ptr<BroadcastAlgorithm> {
            return std::make_unique<SubDirectAlgorithm>(init, *shared, prune_on_duplicate);
          }};
}

}  // namespace ton::overlay::broadcast
