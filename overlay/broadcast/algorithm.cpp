// Algorithm implementations live in `overlay/broadcast/algorithms/*.cpp`. This file holds the
// only piece of the public surface that doesn't belong to a specific algorithm.

#include "overlay/broadcast/algorithm.h"
#include "td/utils/check.h"
#include "td/utils/logging.h"

namespace ton::overlay::broadcast::algorithm {
namespace {

Actions unexpected_event(const char *name) {
  LOG(ERROR) << "unhandled broadcast algorithm event " << name;
  return {};
}

Actions unexpected_message(PeerId from, const char *name) {
  LOG(ERROR) << "unhandled broadcast algorithm message " << name << " from=" << from;
  return {};
}

}  // namespace

BroadcastAlgorithm::BroadcastAlgorithm(bool has_body) : body_ready_(has_body) {
}

void BroadcastAlgorithm::set_now(td::Timestamp now) {
  CHECK(!now_ || now_.at() <= now.at());
  now_ = now;
}

const td::Timestamp &BroadcastAlgorithm::alarm() const {
  return alarm_;
}

Actions BroadcastAlgorithm::handle_event(const Event &event) {
  CHECK(now_);
  if (auto *receive = std::get_if<evt::Receive>(&event)) {
    check_duplicate_flag(*receive);
  }
  if (std::holds_alternative<evt::Publish>(event) || std::holds_alternative<evt::BodyReady>(event)) {
    body_ready_ = true;
  }
  if (std::holds_alternative<evt::Timer>(event)) {
    CHECK(alarm_);
    // Tolerate sub-millisecond float-precision slip — bsim's scheduling is on doubles, so the
    // alarm may round up by ~1 µs vs `now`. Anything larger than a millisecond is a real bug.
    CHECK(alarm_.at() <= now_.at() + 0.001);
    alarm_ = td::Timestamp::never();
  }
  return step(event);
}

Actions BroadcastAlgorithm::step(const Event &event) {
  return std::visit([&](const auto &e) { return on_event(e); }, event);
}

Actions BroadcastAlgorithm::on_event(const evt::Publish &) {
  return unexpected_event("Publish");
}

Actions BroadcastAlgorithm::on_event(const evt::Receive &event) {
  return std::visit([&](const auto &message) { return on_message(event.from, message); }, event.message);
}

Actions BroadcastAlgorithm::on_event(const evt::BodyReady &) {
  return unexpected_event("BodyReady");
}

Actions BroadcastAlgorithm::on_event(const evt::Timer &) {
  return unexpected_event("Timer");
}

// PeerUpsert / PeerRemove are notifications fanned out by the engine. Algorithms only override
// when they care about peer churn (e.g. send Have to a fresh persistent peer, or maintain a
// per-broadcast peer table). Default is a silent no-op so exhaustive variant dispatch in
// subclasses works without per-algorithm boilerplate.
Actions BroadcastAlgorithm::on_event(const evt::PeerUpsert &) {
  return {};
}

Actions BroadcastAlgorithm::on_event(const evt::PeerRemove &) {
  return {};
}

std::shared_ptr<BroadcastShared> empty_shared() {
  static const std::shared_ptr<BroadcastShared> instance = std::make_shared<BroadcastShared>();
  return instance;
}

Actions BroadcastAlgorithm::on_message(PeerId from, const msg::Have &) {
  return unexpected_message(from, "Have");
}

Actions BroadcastAlgorithm::on_message(PeerId from, const msg::Request &) {
  return unexpected_message(from, "Request");
}

Actions BroadcastAlgorithm::on_message(PeerId from, const msg::Cancel &) {
  return unexpected_message(from, "Cancel");
}

Actions BroadcastAlgorithm::on_message(PeerId from, const msg::Piece &) {
  return unexpected_message(from, "Piece");
}

Actions BroadcastAlgorithm::on_message(PeerId, const msg::sim::HavePieces &) {
  // Simulator-only message — algorithms that don't care (anything not coordinating piece sets via
  // bitmaps) silently drop. Swarm overrides to consume.
  return {};
}

Actions BroadcastAlgorithm::on_message(PeerId, const msg::sim::RequestPieces &) {
  return {};
}

Actions BroadcastAlgorithm::on_message(PeerId, const msg::sim::RequestAnyPieces &) {
  return {};
}

Actions BroadcastAlgorithm::on_message(PeerId, const msg::sim::HaveBucket &) {
  return {};
}

Actions BroadcastAlgorithm::on_message(PeerId, const msg::sim::SubscribeBucket &) {
  return {};
}

Actions BroadcastAlgorithm::on_message(PeerId, const msg::sim::UnsubscribeBucket &) {
  return {};
}

td::Timestamp BroadcastAlgorithm::now() const {
  return now_;
}

void BroadcastAlgorithm::check_duplicate_flag(const evt::Receive &receive) {
  auto *piece = std::get_if<msg::Piece>(&receive.message);
  if (piece == nullptr) {
    return;
  }
  bool seen = !seen_pieces_.insert(piece->id).second;
  // Transport-layer dedup is keyed purely on piece id: duplicate iff this id already arrived.
  // One-way enforcement (transport-says-fresh implies algorithm-hasn't-seen-via-receive)
  // because some algorithms (e.g. OptimumP2P) generate colliding local ids, so bsim's dedup
  // can legitimately flag a first-at-this-algorithm receive as duplicate.
  CHECK(piece->duplicate || !seen);
}

void register_peers(BroadcastAlgorithm &algorithm, std::vector<Peer> peers, td::Timestamp now) {
  algorithm.set_now(now);
  for (auto &peer : peers) {
    algorithm.handle_event(evt::PeerUpsert{std::move(peer)});
  }
}

}  // namespace ton::overlay::broadcast::algorithm
