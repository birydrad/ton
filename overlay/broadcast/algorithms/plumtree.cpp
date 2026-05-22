// Plumtree (Leitão et al. 2007) with persistent eager/lazy state — iroh-gossip style. The mesh
// adapts across broadcasts: PRUNE-on-duplicate demotes a peer to lazy, missing-message GRAFT
// promotes it back. The eager set converges to a spanning tree over the active peer set.

#include <unordered_set>

#include "overlay/broadcast/algorithms/plumtree.h"

namespace ton::overlay::broadcast::algorithm {
namespace {

class PlumtreeShared final : public BroadcastShared {
 public:
  explicit PlumtreeShared(PlumtreeConfig config) : config_(config) {
  }

  void on_peer_upsert(const Peer &peer) override {
    if (!peer.neighbour) {
      eager.erase(peer.id);
      lazy.erase(peer.id);
      return;
    }
    if (eager.count(peer.id) || lazy.count(peer.id)) {
      return;
    }
    if (config_.eager_cap_max > 0 && eager.size() >= config_.eager_cap_max) {
      lazy.insert(peer.id);
    } else {
      eager.insert(peer.id);  // Plumtree paper §3.1 default.
    }
  }
  void on_peer_remove(PeerId id) override {
    eager.erase(id);
    lazy.erase(id);
  }
  void refresh_existing_peer(const Peer &) {
  }

  // Returns true iff the demote actually happened (respects eager_cap_min).
  bool demote(PeerId p) {
    if (eager.count(p) == 0) {
      no_op_demotes++;
      g_no_op_demotes++;
      return false;
    }
    if (eager.size() <= config_.eager_cap_min) {
      return false;
    }
    eager.erase(p);
    lazy.insert(p);
    real_demotes++;
    g_real_demotes++;
    return true;
  }
  // Returns true iff the promote happened (respects eager_cap_max).
  bool promote(PeerId p) {
    if (lazy.count(p) == 0) {
      return false;
    }
    if (config_.eager_cap_max > 0 && eager.size() >= config_.eager_cap_max) {
      return false;
    }
    lazy.erase(p);
    eager.insert(p);
    real_promotes++;
    g_real_promotes++;
    return true;
  }

  mutable std::uint64_t real_demotes = 0;
  mutable std::uint64_t no_op_demotes = 0;
  mutable std::uint64_t real_promotes = 0;

  static std::uint64_t g_real_demotes;
  static std::uint64_t g_real_promotes;
  static std::uint64_t g_no_op_demotes;

  std::unordered_set<PeerId> eager;
  std::unordered_set<PeerId> lazy;

 private:
  PlumtreeConfig config_;
};

class PlumtreeAlgorithm final : public BroadcastAlgorithm {
 public:
  PlumtreeAlgorithm(BroadcastInit init, PlumtreeConfig config, std::shared_ptr<PlumtreeShared> shared)
      : BroadcastAlgorithm(init.has_body), config_(config), shared_(std::move(shared)), has_body_(init.has_body) {
  }

  BroadcastStats stats() const override {
    return BroadcastStats{.has_body = has_body_, .peer_count = shared_->eager.size() + shared_->lazy.size()};
  }

 private:
  PlumtreeConfig config_;
  std::shared_ptr<PlumtreeShared> shared_;
  bool has_body_ = false;
  // Per-broadcast: peers who advertised this message via IHAVE. First entry is the GRAFT target
  // if the eager-push window expires without payload arrival.
  std::vector<PeerId> ihave_senders_;
  bool grafted_ = false;

  // Forward the body: eager-push Piece to eager peers, IHAVE to lazy peers. `except` skips the
  // sender we received this body from (they obviously have it).
  void forward(Actions &out, std::optional<PeerId> except) {
    for (auto p : shared_->eager) {
      if (except && p == *except)
        continue;
      out.send(p, msg::Piece{0});
    }
    for (auto p : shared_->lazy) {
      if (except && p == *except)
        continue;
      out.send(p, msg::Have{});
    }
  }

  Actions on_event(const evt::Publish &) override {
    Actions out;
    CHECK(!has_body_);
    has_body_ = true;
    out.deliver();
    forward(out, std::nullopt);
    return out;
  }

  Actions on_event(const evt::BodyReady &) override {
    Actions out;
    if (!has_body_) {
      has_body_ = true;
      out.deliver();
    }
    return out;
  }

  Actions on_event(const evt::PeerUpsert &event) override {
    shared_->refresh_existing_peer(event.peer);
    return {};
  }

  Actions on_event(const evt::Timer &) override {
    // GRAFT timeout: we got IHAVE but no Piece. Promote the first announcer to eager and pull.
    Actions out;
    if (has_body_ || grafted_ || ihave_senders_.empty()) {
      return out;
    }
    auto target = ihave_senders_.front();
    shared_->promote(target);
    out.send(target, msg::Request{});  // GRAFT
    grafted_ = true;
    return out;
  }

  Actions on_message(PeerId from, const msg::Piece &) override {
    Actions out;
    if (has_body_) {
      out.send(from, msg::Cancel{});
      shared_->demote(from);
      return out;
    }
    has_body_ = true;
    out.deliver();
    out.feedback(from, config_.success_delta);
    alarm_ = td::Timestamp::never();
    forward(out, from);
    return out;
  }

  Actions on_message(PeerId from, const msg::Have &) override {
    if (has_body_) {
      Actions out;
      out.send(from, msg::Cancel{});
      shared_->demote(from);
      return out;
    }
    ihave_senders_.push_back(from);
    // Arm the GRAFT timer on first IHAVE.
    if (!alarm_) {
      alarm_ = td::Timestamp::in(config_.graft_timeout, now());
    }
    return {};
  }

  Actions on_message(PeerId from, const msg::Request &) override {
    // GRAFT received: promote them to eager and serve if we have the body.
    shared_->promote(from);
    Actions out;
    if (has_body_) {
      out.send(from, msg::Piece{0});
    }
    return out;
  }

  Actions on_message(PeerId from, const msg::Cancel &) override {
    // PRUNE received: demote them to lazy.
    shared_->demote(from);
    return {};
  }
};

std::uint64_t PlumtreeShared::g_real_demotes = 0;
std::uint64_t PlumtreeShared::g_real_promotes = 0;
std::uint64_t PlumtreeShared::g_no_op_demotes = 0;

}  // namespace

}  // namespace ton::overlay::broadcast::algorithm

namespace ton::overlay::broadcast {

AlgorithmFamily make_plumtree_family(PlumtreeConfig config) {
  auto shared = std::make_shared<algorithm::PlumtreeShared>(config);
  return {.key = "plumtree", .shared = shared, .make_algorithm = [config, shared](algorithm::BroadcastInit init) {
            return std::make_unique<algorithm::PlumtreeAlgorithm>(init, config, shared);
          }};
}

}  // namespace ton::overlay::broadcast
