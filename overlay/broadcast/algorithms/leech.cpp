// Adversarial algorithm. A leech node never delivers and never serves Pieces. Different levels
// of badness via LeechConfig (defined in algorithm.h):
//   * `attract_haves`   — proactively spams Have to all known peers on Publish/Have receipt and
//                         on a periodic timer. Drags honest peers' Requests into a black hole.
//   * `reply_with_cancel` — when a Request arrives, reply with Cancel (faster signal). Default
//                           false: silently swallow the Request, requester waits for retry.
//   * `propagate_haves` — gossip received Have onward (act like a honest hub). Default false.

#include "overlay/broadcast/algorithms/leech.h"
#include "overlay/broadcast/algorithms/peer-table.h"

namespace ton::overlay::broadcast::algorithm {
namespace {

using namespace internal;

class LeechShared final : public BroadcastShared {
 public:
  void on_peer_upsert(const Peer &peer) override {
    peers.register_peer(peer);
  }
  void on_peer_remove(PeerId id) override {
    peers.erase(id);
  }

  PeerTable<PeerOnlyState> peers;
};

class LeechAlgorithm final : public BroadcastAlgorithm {
 public:
  LeechAlgorithm(BroadcastInit init, LeechConfig cfg, std::shared_ptr<LeechShared> shared)
      : BroadcastAlgorithm(init.has_body), shared_(std::move(shared)), cfg_(cfg) {
  }

  BroadcastStats stats() const override {
    return BroadcastStats{.has_body = false, .peer_count = shared_->peers.size()};
  }

 private:
  std::shared_ptr<LeechShared> shared_;
  LeechConfig cfg_;

  void spam_have(Actions &out) {
    auto announce_to = shared_->peers.ranked({.sort_by_score = false, .limit = cfg_.metadata_peer_limit});
    for (auto *p : announce_to) {
      out.send(p->peer.id, msg::Have{});
    }
  }

  void spam_request(Actions &out) {
    auto request_to = shared_->peers.ranked({.sort_by_score = false, .limit = cfg_.request_peer_limit});
    for (auto *p : request_to) {
      out.send(p->peer.id, msg::Request{});
    }
  }

  void schedule_rebroadcast() {
    bool needs_timer = (cfg_.attract_haves || cfg_.proactive_request);
    if (needs_timer && cfg_.rebroadcast_interval > 0.0) {
      alarm_.relax(td::Timestamp::in(cfg_.rebroadcast_interval, now()));
    }
  }

  Actions on_event(const evt::Publish &) override {
    Actions out;
    if (cfg_.attract_haves) {
      spam_have(out);
    }
    if (cfg_.proactive_request) {
      spam_request(out);
    }
    schedule_rebroadcast();
    return out;
  }

  Actions on_event(const evt::BodyReady &) override {
    return {};  // never deliver
  }

  Actions on_event(const evt::Timer &) override {
    Actions out;
    if (cfg_.attract_haves) {
      spam_have(out);
    }
    if (cfg_.proactive_request) {
      spam_request(out);
    }
    schedule_rebroadcast();
    return out;
  }

  Actions on_message(PeerId from, const msg::Have &) override {
    Actions out;
    if (cfg_.propagate_haves) {
      auto announce_to =
          shared_->peers.ranked({.except = from, .sort_by_score = false, .limit = cfg_.metadata_peer_limit});
      for (auto *p : announce_to) {
        out.send(p->peer.id, msg::Have{});
      }
    }
    if (cfg_.proactive_request) {
      // The peer just told us they have content — bait them into serving us a Piece.
      out.send(from, msg::Request{});
    }
    schedule_rebroadcast();
    return out;
  }

  Actions on_message(PeerId from, const msg::Request &) override {
    Actions out;
    if (cfg_.reply_with_cancel) {
      out.send(from, msg::Cancel{});
    }
    // Otherwise: silently swallow the Request. Requester waits for a timer.
    return out;
  }

  Actions on_message(PeerId, const msg::Piece &) override {
    return {};  // accept silently — no forwarding, no delivery
  }
};

}  // namespace

}  // namespace ton::overlay::broadcast::algorithm

namespace ton::overlay::broadcast {

AlgorithmFamily make_leech_family(algorithm::LeechConfig config) {
  auto shared = std::make_shared<algorithm::LeechShared>();
  return {.key = "leech", .shared = shared, .make_algorithm = [config, shared](algorithm::BroadcastInit init) {
            return std::make_unique<algorithm::LeechAlgorithm>(init, config, shared);
          }};
}

}  // namespace ton::overlay::broadcast
