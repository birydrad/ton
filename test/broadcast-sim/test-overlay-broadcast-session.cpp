// Unit tests for production OverlayBroadcastSession behavior using a fake host Env.
//
// These tests drive the session through its public queueing API. They deliberately keep
// the fake Env small: the goal is to exercise session ordering, validation, duplicate,
// and alarm contracts without involving the full Overlay actor.

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/checksum.h"
#include "overlay/broadcast/overlay-broadcast-session.h"
#include "td/actor/TestScheduler.h"
#include "td/utils/tests.h"

namespace {

using ton::PublicKey;
using ton::PublicKeyHash;
using ton::adnl::AdnlNodeIdShort;
using ton::overlay::BroadcastCheckResult;
using ton::overlay::BroadcastInfo;
using ton::overlay::BroadcastMeta;
using ton::overlay::BroadcastMode;
using ton::overlay::BroadcastPeerInfo;
using ton::overlay::BroadcastSessionOwner;
using ton::overlay::BroadcastSource;
using ton::overlay::Env;
using ton::overlay::Have;
using ton::overlay::IncomingMessage;
using ton::overlay::OverlayBroadcastOptions;
using ton::overlay::OverlayBroadcastSession;
using ton::overlay::SessionContext;
using ton::overlay::SessionEvent;
using ton::overlay::SignaturePayload;
using ton::overlay::WireMessage;
using ton::overlay::mode::EagerLazy;

AdnlNodeIdShort peer_id(unsigned char tag) {
  std::string bytes(32, static_cast<char>(tag));
  return AdnlNodeIdShort{td::Slice(bytes)};
}

BroadcastSource make_source(td::Slice name) {
  auto public_key = PublicKey{ton::pubkeys::Unenc{td::BufferSlice{name}}};
  auto key_hash = public_key.compute_short_id();
  return BroadcastSource{std::move(public_key), key_hash, nullptr};
}

struct BroadcastFixture {
  BroadcastMode mode;
  BroadcastMeta meta;
  BroadcastSource source;
};

BroadcastFixture make_eager_lazy_broadcast(const BroadcastSource &source, AdnlNodeIdShort publisher, td::Slice body) {
  auto mode = BroadcastMode{EagerLazy{}};
  BroadcastInfo info{.common = {.flags = 0,
                                .date = 1'700'000'000,
                                .id_source = source.key_hash,
                                .src_adnl_id = publisher,
                                .data_hash = td::sha256_bits256(body),
                                .data_size = static_cast<td::uint32>(body.size()),
                                .extra = td::BufferSlice{"extra"}},
                     .mode = mode};
  auto broadcast_id = ton::overlay::v2_wire()->compute_broadcast_id(info);
  return BroadcastFixture{
      .mode = mode, .meta = {.broadcast_id = broadcast_id, .info = info.clone()}, .source = source.clone()};
}

SignaturePayload to_sign(const BroadcastInfo &info, WireMessage message) {
  auto payload = ton::overlay::v2_wire()->to_sign(info, message);
  if (payload.is_error()) {
    LOG(FATAL) << payload.error();
  }
  return payload.move_as_ok();
}

IncomingMessage make_piece_message(const BroadcastFixture &fixture, td::BufferSlice data) {
  auto signed_payload = to_sign(fixture.meta.info, WireMessage{td::fec::Symbol{0, data.clone()}});
  return IncomingMessage{.meta = fixture.meta.clone(),
                         .source = fixture.source.clone(),
                         .signature = td::BufferSlice{"signature"},
                         .signed_payload = std::move(signed_payload),
                         .content = td::fec::Symbol{0, std::move(data)}};
}

IncomingMessage make_have_message(const BroadcastFixture &fixture) {
  auto signed_payload = to_sign(fixture.meta.info, WireMessage{Have{}});
  return IncomingMessage{.meta = fixture.meta.clone(),
                         .source = fixture.source.clone(),
                         .signature = td::BufferSlice{"signature"},
                         .signed_payload = std::move(signed_payload),
                         .content = Have{}};
}

class FakeOwner final : public BroadcastSessionOwner {
 public:
  void erase_session(const std::shared_ptr<OverlayBroadcastSession> &) override {
    erase_count++;
  }

  void mark_delivered(const ton::overlay::Overlay::BroadcastHash &broadcast_id) override {
    delivered_ids.push_back(broadcast_id);
  }

  size_t erase_count = 0;
  std::vector<ton::overlay::Overlay::BroadcastHash> delivered_ids;
};

class FakeEnv final : public Env {
 public:
  struct SentMessage {
    AdnlNodeIdShort dst;
    td::BufferSlice wire;
  };

  struct ScheduledAlarm {
    td::Bits256 broadcast_id;
    td::Timestamp alarm;
    td::uint64 token = 0;
  };

  explicit FakeEnv(AdnlNodeIdShort local) : local_(local), signing_key_(make_source("signing-key").public_key) {
  }

  AdnlNodeIdShort local_id() const override {
    return local_;
  }

  std::vector<BroadcastPeerInfo> peers() override {
    return peers_;
  }

  std::optional<BroadcastPeerInfo> peer_info(AdnlNodeIdShort peer) override {
    for (const auto &info : peers_) {
      if (info.id == peer) {
        return info;
      }
    }
    return BroadcastPeerInfo{.id = peer};
  }

  void update_peer_score(AdnlNodeIdShort peer, double delta) override {
    score_updates.push_back({peer, delta});
  }

  td::actor::Task<BroadcastCheckResult> precheck_source(const BroadcastSource &, const BroadcastMeta &, AdnlNodeIdShort,
                                                        bool signature_checked) override {
    precheck_calls++;
    if (signature_checked) {
      checked_precheck_calls++;
    } else {
      unchecked_precheck_calls++;
    }
    if (block_next_precheck_) {
      block_next_precheck_ = false;
      auto [task, promise] = td::actor::StartedTask<BroadcastCheckResult>::make_bridge();
      blocked_precheck_ = std::move(promise);
      co_return co_await std::move(task);
    }
    co_return precheck_result_;
  }

  td::actor::Task<> verify_decoded_body(BroadcastSource, td::BufferSlice) override {
    verify_decoded_body_calls++;
    co_return {};
  }

  void deliver(PublicKeyHash sender, td::BufferSlice body, td::BufferSlice extra) override {
    delivered_senders.push_back(sender);
    delivered_bodies.push_back(std::move(body));
    delivered_extras.push_back(std::move(extra));
  }

  void send(AdnlNodeIdShort dst, td::BufferSlice wire) override {
    sent.push_back({dst, std::move(wire)});
  }

  td::actor::StartedTask<std::pair<td::BufferSlice, PublicKey>> sign(PublicKeyHash, td::BufferSlice) override {
    sign_calls++;
    auto [task, promise] = td::actor::StartedTask<std::pair<td::BufferSlice, PublicKey>>::make_bridge();
    promise.set_value(std::make_pair(td::BufferSlice{"signed"}, signing_key_));
    return std::move(task);
  }

  td::Status verify_signature(PublicKey, td::Slice, td::Slice, AdnlNodeIdShort) override {
    verify_signature_calls++;
    return td::Status::OK();
  }

  void schedule_dispatch_timer(td::Bits256 broadcast_id, td::Timestamp alarm, td::uint64 token) override {
    scheduled_alarms.push_back({broadcast_id, alarm, token});
  }

  void block_next_precheck() {
    block_next_precheck_ = true;
  }

  bool has_blocked_precheck() const {
    return blocked_precheck_.has_value();
  }

  void release_blocked_precheck() {
    CHECK(blocked_precheck_.has_value());
    auto promise = std::move(*blocked_precheck_);
    blocked_precheck_.reset();
    auto result = precheck_result_;
    promise.set_value(std::move(result));
  }

  AdnlNodeIdShort local_;
  PublicKey signing_key_;
  std::vector<BroadcastPeerInfo> peers_;
  BroadcastCheckResult precheck_result_ = BroadcastCheckResult::Allowed;
  size_t precheck_calls = 0;
  size_t checked_precheck_calls = 0;
  size_t unchecked_precheck_calls = 0;
  size_t verify_signature_calls = 0;
  size_t verify_decoded_body_calls = 0;
  size_t sign_calls = 0;
  std::vector<std::pair<AdnlNodeIdShort, double>> score_updates;
  std::vector<PublicKeyHash> delivered_senders;
  std::vector<td::BufferSlice> delivered_bodies;
  std::vector<td::BufferSlice> delivered_extras;
  std::vector<SentMessage> sent;
  std::vector<ScheduledAlarm> scheduled_alarms;

 private:
  bool block_next_precheck_ = false;
  std::optional<td::actor::StartedTask<BroadcastCheckResult>::ExternalPromise> blocked_precheck_;
};

std::shared_ptr<OverlayBroadcastSession> make_session(FakeEnv &env, FakeOwner &owner, const BroadcastFixture &fixture) {
  OverlayBroadcastOptions opts;
  opts.active_peer_limit = 0;
  opts.lazy_peer_limit = 20;
  return OverlayBroadcastSession::make(SessionContext{.env = env, .owner = owner}, opts, fixture.meta.clone());
}

}  // namespace

TEST(OverlayBroadcastSession, QueuedDuplicatePieceAfterValidationSuspensionSkipsSecondValidation) {
  td::actor::TestScheduler scheduler;
  scheduler.run([&]() -> td::actor::Task<td::Unit> {
    auto local = peer_id(1);
    auto publisher = peer_id(2);
    auto relay = peer_id(3);
    auto source = make_source("source-key");
    td::BufferSlice body{"broadcast body"};
    auto fixture = make_eager_lazy_broadcast(source, publisher, body.as_slice());

    FakeEnv env(local);
    FakeOwner owner;
    auto session = make_session(env, owner, fixture);

    env.block_next_precheck();
    session->push_event(SessionEvent{
        SessionEvent::Receive{.incoming = make_piece_message(fixture, body.clone()), .src_peer_id = publisher}});
    co_await scheduler.wait_sync_work();

    ASSERT_TRUE(env.has_blocked_precheck());
    EXPECT_EQ(env.precheck_calls, 1u);
    EXPECT_EQ(env.verify_signature_calls, 0u);

    session->push_event(SessionEvent{SessionEvent::Receive{
        .incoming = make_piece_message(fixture, td::BufferSlice{"tampered duplicate"}), .src_peer_id = relay}});
    co_await scheduler.wait_sync_work();

    EXPECT_EQ(env.precheck_calls, 1u);
    EXPECT_EQ(env.verify_signature_calls, 0u);
    EXPECT_EQ(env.delivered_bodies.size(), 0u);

    env.release_blocked_precheck();
    co_await scheduler.wait_sync_work();

    EXPECT_EQ(env.unchecked_precheck_calls, 1u);
    EXPECT_EQ(env.checked_precheck_calls, 1u);
    EXPECT_EQ(env.precheck_calls, 2u);
    EXPECT_EQ(env.verify_signature_calls, 1u);
    EXPECT_EQ(env.delivered_bodies.size(), 1u);
    EXPECT_EQ(owner.delivered_ids.size(), 1u);
    EXPECT_EQ(env.delivered_bodies[0].as_slice(), body.as_slice());
    EXPECT_EQ(env.sent.size(), 0u);
    EXPECT_EQ(owner.erase_count, 0u);
    co_return td::Unit{};
  });
}

TEST(OverlayBroadcastSession, EarlyAndStaleTimerCallbacksDoNotDispatchAlarm) {
  td::actor::TestScheduler scheduler;
  scheduler.run([&]() -> td::actor::Task<td::Unit> {
    auto local = peer_id(11);
    auto publisher = peer_id(12);
    auto source = make_source("source-key-for-timer");
    td::BufferSlice body{"timer body"};
    auto fixture = make_eager_lazy_broadcast(source, publisher, body.as_slice());

    FakeEnv env(local);
    FakeOwner owner;
    auto session = make_session(env, owner, fixture);

    session->push_event(
        SessionEvent{SessionEvent::Receive{.incoming = make_have_message(fixture), .src_peer_id = publisher}});
    co_await scheduler.wait_sync_work();

    ASSERT_EQ(env.scheduled_alarms.size(), 1u);
    EXPECT_EQ(env.sent.size(), 0u);
    auto first_token = env.scheduled_alarms[0].token;

    session->push_event(SessionEvent{SessionEvent::Timer{.token = first_token}});
    co_await scheduler.wait_sync_work();

    ASSERT_EQ(env.scheduled_alarms.size(), 2u);
    EXPECT_EQ(env.sent.size(), 0u);
    auto second_token = env.scheduled_alarms[1].token;

    session->push_event(SessionEvent{SessionEvent::Timer{.token = first_token}});
    co_await scheduler.wait_sync_work();

    EXPECT_EQ(env.scheduled_alarms.size(), 2u);
    EXPECT_EQ(env.sent.size(), 0u);

    scheduler.advance_time_to(env.scheduled_alarms[1].alarm);
    session->push_event(SessionEvent{SessionEvent::Timer{.token = second_token}});
    co_await scheduler.wait_sync_work();

    EXPECT_EQ(env.sent.size(), 1u);
    EXPECT_EQ(env.sent[0].dst, publisher);
    EXPECT_EQ(env.sign_calls, 0u);
    co_return td::Unit{};
  });
}
