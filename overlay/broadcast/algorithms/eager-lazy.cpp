// Eager-lazy push-pull. We score peers and push the body eagerly to the top `active_peer_limit`,
// announce via Have to the next `lazy_peer_limit`, and pull on receipt of Have. Production
// default for the overlay's main broadcast path.

#include "overlay/broadcast/algorithms/eager-lazy.h"
#include "overlay/broadcast/algorithms/peer-table.h"

namespace ton::overlay::broadcast::algorithm {
namespace {

using namespace internal;

class EagerLazyAlgorithm final : public BroadcastAlgorithm {
 public:
  EagerLazyAlgorithm(BroadcastInit init, EagerLazyConfig config)
      : BroadcastAlgorithm(init.has_body), config_(config), peers_(init.self, init.id), has_body_(init.has_body) {
  }

  BroadcastStats stats() const override {
    auto pull_peers = pull_statuses();
    return BroadcastStats{.has_body = has_body_,
                          .peer_count = peers_.size(),
                          .last_piece_sender = last_piece_sender_,
                          .pull_tracked_count = pull_peers.size(),
                          .pull_current_request = requested_peer_id(),
                          .pull_peers = std::move(pull_peers),
                          .pull_timer_scheduled = static_cast<bool>(alarm_)};
  }

 private:
  struct PeerState {
    Peer peer;
    PullStatus pull = PullStatus::Unknown;

    bool terminal() const {
      return pull == PullStatus::Cancelled || pull == PullStatus::Served;
    }

    bool can_receive_have() const {
      return pull == PullStatus::Unknown;
    }

    bool can_receive_eager_body() const {
      return pull == PullStatus::Unknown;
    }

    bool can_serve_request() const {
      return !terminal();
    }

    void mark_announced() {
      CHECK(pull == PullStatus::Unknown);
      pull = PullStatus::Announced;
    }

    void mark_requested() {
      CHECK(pull == PullStatus::Announced);
      pull = PullStatus::Requested;
    }

    void mark_unknown() {
      CHECK(pull == PullStatus::Requested);
      pull = PullStatus::Unknown;
    }

    void mark_cancelled() {
      if (pull != PullStatus::Served) {
        pull = PullStatus::Cancelled;
      }
    }

    void mark_served() {
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

  EagerLazyConfig config_;
  PeerTable<PeerState> peers_;
  bool has_body_ = false;
  std::optional<PeerId> last_piece_sender_;

  PeerState *requested_peer() {
    return peers_.find_unique([](const PeerState &peer) { return peer.pull == PullStatus::Requested; });
  }

  const PeerState *requested_peer() const {
    return peers_.find_unique([](const PeerState &peer) { return peer.pull == PullStatus::Requested; });
  }

  std::optional<PeerId> requested_peer_id() const {
    auto peer = requested_peer();
    return peer == nullptr ? std::nullopt : std::make_optional(peer->peer.id);
  }

  PeerState *best_announced_peer() {
    auto announced = peers_.ranked({}, [](const PeerState &peer) { return peer.pull == PullStatus::Announced; });
    return announced.empty() ? nullptr : announced[0];
  }

  bool has_announced_peer() const {
    return peers_.any([](const PeerState &peer) { return peer.pull == PullStatus::Announced; });
  }

  bool mark_body_ready(Actions &out) {
    if (has_body_) {
      return false;
    }
    has_body_ = true;
    alarm_ = td::Timestamp::never();
    out.deliver();
    return true;
  }

  void request_alarm(double delay) {
    alarm_.relax(td::Timestamp::in(delay, now()));
  }

  void send_eager_lazy(Actions &out, PeerRankOptions rank_options) {
    auto ranked = peers_.ranked(rank_options);
    auto eager_limit = std::min<size_t>(config_.active_peer_limit, ranked.size());
    for (size_t i = 0; i < eager_limit; i++) {
      auto &peer = *ranked[i];
      if (peer.can_receive_eager_body()) {
        out.send(peer.peer.id, msg::Piece{0});
      }
    }

    td::uint32 lazy_count = 0;
    for (size_t i = eager_limit; i < ranked.size() && lazy_count < config_.lazy_peer_limit; i++) {
      auto &peer = *ranked[i];
      if (peer.terminal()) {
        continue;
      }
      out.send(peer.peer.id, msg::Have{});
      lazy_count++;
    }
  }

  Actions on_event(const evt::Publish &) override {
    Actions out;
    if (mark_body_ready(out)) {
      send_eager_lazy(out, {});
    }
    return out;
  }

  Actions on_event(const evt::BodyReady &) override {
    Actions out;
    if (mark_body_ready(out)) {
      send_eager_lazy(out, {.except = last_piece_sender_});
    }
    return out;
  }

  Actions on_event(const evt::Timer &) override {
    Actions out;
    if (has_body_) {
      return out;
    }
    expire_requested(out);
    auto requested = request_best_announced(out);
    if (requested != nullptr && has_announced_peer()) {
      request_alarm(config_.request_retry_delay);
    }
    return out;
  }

  Actions on_event(const evt::PeerUpsert &event) override {
    peers_.register_peer(event.peer);
    return {};
  }

  Actions on_message(PeerId from, const msg::Have &) override {
    Actions out;
    auto &peer = peers_.require(from);
    if (!has_body_ && peer.can_receive_have()) {
      peer.mark_announced();
      request_alarm(config_.request_delay);
    }
    return out;
  }

  Actions on_message(PeerId from, const msg::Request &) override {
    Actions out;
    auto &peer = peers_.require(from);
    if (!has_body_ || !peer.can_serve_request()) {
      return out;
    }
    peer.mark_served();
    out.send(peer.peer.id, msg::Piece{0});
    return out;
  }

  Actions on_message(PeerId from, const msg::Cancel &) override {
    auto &peer = peers_.require(from);
    bool was_requested = peer.pull == PullStatus::Requested;
    peer.mark_cancelled();
    if (was_requested) {
      alarm_ = td::Timestamp::never();
    }
    Actions out;
    if (!has_body_ && requested_peer() == nullptr) {
      auto requested = request_best_announced(out);
      if (requested != nullptr && has_announced_peer()) {
        request_alarm(config_.request_retry_delay);
      }
    }
    return out;
  }

  Actions on_message(PeerId from, const msg::Piece &) override {
    Actions out;
    if (!has_body_) {
      auto &peer = peers_.require(from);
      complete_pull_from(out, peer);
    }
    return out;
  }

  void complete_pull_from(Actions &out, PeerState &peer) {
    auto stale = requested_peer();
    alarm_ = td::Timestamp::never();
    last_piece_sender_ = peer.peer.id;
    out.feedback(peer.peer.id, config_.peer_success_delta);
    peer.mark_served();
    if (stale != nullptr && stale != &peer) {
      stale->mark_cancelled();
      out.send(stale->peer.id, msg::Cancel{});
    }
  }

  void expire_requested(Actions &out) {
    if (auto stale = requested_peer()) {
      stale->mark_unknown();
      out.feedback(stale->peer.id, config_.peer_timeout_delta);
    }
  }

  PeerState *request_best_announced(Actions &out) {
    auto requested = best_announced_peer();
    if (requested != nullptr) {
      requested->mark_requested();
      out.send(requested->peer.id, msg::Request{});
    }
    return requested;
  }
};

}  // namespace

}  // namespace ton::overlay::broadcast::algorithm

namespace ton::overlay::broadcast {

AlgorithmFamily make_eager_lazy_family(EagerLazyConfig config) {
  return {.key = "eager-lazy",
          .shared = algorithm::empty_shared(),
          .make_algorithm = [config](algorithm::BroadcastInit init) {
            return std::make_unique<algorithm::EagerLazyAlgorithm>(init, config);
          }};
}

}  // namespace ton::overlay::broadcast
