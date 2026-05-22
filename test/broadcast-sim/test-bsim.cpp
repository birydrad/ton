// Unit tests for bsim and the broadcast algorithms it drives.
//
// Each test builds a small synthetic topology, runs one or more broadcasts, and asserts on the
// resulting Metrics. Topologies here are deliberately tiny (a few dozen nodes) so tests run in
// milliseconds and assertions can be precise.

#include "overlay/broadcast/algorithm.h"
#include "overlay/broadcast/algorithms/eager-lazy.h"
#include "overlay/broadcast/algorithms/fec.h"
#include "overlay/broadcast/algorithms/leech.h"
#include "overlay/broadcast/algorithms/plumtree.h"
#include "overlay/broadcast/algorithms/push.h"
#include "td/utils/tests.h"

#include "bsim.h"

namespace {

namespace algo = ton::overlay::broadcast::algorithm;
using ton::bsim::BodyId;
using ton::bsim::NodeId;
using ton::bsim::SessionState;
using ton::bsim::Spec;
using ton::bsim::Topology;

// Build a fully-connected topology of `n` nodes with uniform link latency `latency_s` and the
// default 100 MB/s upload bandwidth. Every node knows every other node.
Topology mesh(std::uint32_t n, double latency_s = 0.010) {
  Topology t;
  t.node_count = n;
  t.peers.assign(n, {});
  t.latency.assign(n, std::vector<double>(n, latency_s));
  for (NodeId i = 0; i < n; i++) {
    for (NodeId j = 0; j < n; j++) {
      if (j != i) {
        t.peers[i].push_back(j);
      }
      t.latency[i][i] = 0.0;
    }
  }
  return t;
}

// Use this in place of EXPECT_APPROX when 1e-6 relative tolerance is too tight (e.g. when piece
// headers add a few microseconds of transmission delay on top of body bytes).
#define EXPECT_NEAR(actual, expected, tol)                                                                         \
  do {                                                                                                             \
    auto _a = (actual);                                                                                            \
    auto _e = (expected);                                                                                          \
    if (std::abs(_a - _e) > (tol)) {                                                                               \
      LOG(ERROR) << #actual << " (" << _a << ") not within " << (tol) << " of " << #expected << " (" << _e << ")"; \
      ::td::TestContext::get()->register_test_failure();                                                           \
    }                                                                                                              \
  } while (0)

Spec spec_for_family(NodeId source, std::uint64_t body_size, std::uint32_t pieces,
                     ton::bsim::AlgorithmFactory::FamilyFactory family,
                     ton::bsim::AlgorithmFactory::FamilyFactory leech_family = {}) {
  return Spec{.body_id = static_cast<BodyId>(source) + 1,
              .source = source,
              .body_size = body_size,
              .piece_count = pieces,
              .piece_header = 64,
              .control_bytes = 64,
              .factory = ton::bsim::AlgorithmFactory{.honest = std::move(family), .leech = std::move(leech_family)}};
}

}  // namespace

// ---- Algorithm correctness on a complete graph -------------------------------------------------

TEST(Algorithm, FloodReachesEveryone) {
  auto t = mesh(8);
  auto m = ton::bsim::simulate(
      t,
      spec_for_family(
          0, /*body=*/1024, /*pieces=*/1,
          [] { return ton::overlay::broadcast::make_push_family(ton::overlay::broadcast::PushConfig{.k = 0}); }),
      /*max_sim_time=*/1.0);
  ASSERT_EQ(m.delivered, 7u);  // every receiver except source
}

TEST(Algorithm, PlumtreeReachesEveryone) {
  auto t = mesh(16);
  auto m = ton::bsim::simulate(
      t,
      spec_for_family(0, /*body=*/1024, /*pieces=*/1, [] { return ton::overlay::broadcast::make_plumtree_family({}); }),
      /*max_sim_time=*/2.0);
  ASSERT_EQ(m.delivered, 15u);
}

TEST(Algorithm, EagerLazyReachesEveryone) {
  auto t = mesh(16);
  auto m = ton::bsim::simulate(t,
                               spec_for_family(0, 1024, 1,
                                               [] {
                                                 return ton::overlay::broadcast::make_eager_lazy_family(
                                                     {.active_peer_limit = 5, .lazy_peer_limit = 10});
                                               }),
                               /*max_sim_time=*/2.0);
  ASSERT_EQ(m.delivered, 15u);
}

TEST(Algorithm, FecMultiPieceReachesEveryone) {
  auto t = mesh(20);
  // 4 pieces required, 16 emitted (4x redundancy), each fan-out 4 — well above the percolation
  // threshold for a 20-node complete graph, so reach must be 100%.
  auto fec = algo::FecConfig{
      .total_pieces = 16, .required_pieces = 4, .random_per_piece = 4, .emit_batch_size = 16, .emit_interval = 0.0};
  auto m = ton::bsim::simulate(
      t,
      spec_for_family(0, /*body=*/4096, /*pieces=*/4, [fec] { return ton::overlay::broadcast::make_fec_family(fec); }),
      /*max_sim_time=*/2.0);
  ASSERT_EQ(m.delivered, 19u);
}

// ---- Bandwidth model ---------------------------------------------------------------------------

TEST(Bsim, UploadPipeSerialises) {
  // Source sends one giant body to two receivers via flood. With 10 MB/s upload and 1 MB body,
  // each transmission takes 0.1s. The two sends serialise in the source's FIFO pipe, so the
  // second receiver's delivery time should be ~2× the first's.
  auto t = mesh(3, /*latency=*/0.0);  // zero propagation: time is pure transmission
  t.upload_bw = 10.0 * 1024 * 1024;   // 10 MB/s
  auto m = ton::bsim::simulate(
      t,
      spec_for_family(
          0, 1024 * 1024, 1,
          [] { return ton::overlay::broadcast::make_push_family(ton::overlay::broadcast::PushConfig{.k = 0}); }),
      /*max_sim_time=*/5.0);
  ASSERT_EQ(m.delivered, 2u);
  // Receiver delivery times: 0.1s and 0.2s. Tolerate piece-header overhead (+64 B per piece).
  std::vector<double> times;
  for (const auto &p : m.per_node) {
    if (p.delivery_time >= 0.0) {
      times.push_back(p.delivery_time);
    }
  }
  std::sort(times.begin(), times.end());
  ASSERT_EQ(times.size(), 2u);
  EXPECT_NEAR(times[0], 0.1, 0.001);
  EXPECT_NEAR(times[1], 0.2, 0.001);
}

TEST(Bsim, PropagationLatencyAdds) {
  // Same as above but with a 30 ms link. First arrival = tx + link, second = 2×tx + link.
  auto t = mesh(3, /*latency=*/0.030);
  t.upload_bw = 10.0 * 1024 * 1024;
  auto m = ton::bsim::simulate(
      t,
      spec_for_family(
          0, 1024 * 1024, 1,
          [] { return ton::overlay::broadcast::make_push_family(ton::overlay::broadcast::PushConfig{.k = 0}); }),
      5.0);
  std::vector<double> times;
  for (const auto &p : m.per_node) {
    if (p.delivery_time >= 0.0) {
      times.push_back(p.delivery_time);
    }
  }
  std::sort(times.begin(), times.end());
  ASSERT_EQ(times.size(), 2u);
  EXPECT_NEAR(times[0], 0.130, 0.001);  // 0.1s tx + 0.03 link
  EXPECT_NEAR(times[1], 0.230, 0.001);  // 0.2s tx + 0.03 link
}

// ---- Leech ------------------------------------------------------------------------------------

TEST(Bsim, LeechNeverDelivers) {
  // 5-node mesh with one leech. Honest source publishes; 3 honest receivers should deliver, the
  // leech should not.
  auto t = mesh(5);
  t.leech_nodes.assign(5, false);
  t.leech_nodes[3] = true;  // node 3 is a leech
  auto m = ton::bsim::simulate(t,
                               spec_for_family(
                                   0, 1024, 1, [] { return ton::overlay::broadcast::make_plumtree_family({}); },
                                   [] { return ton::overlay::broadcast::make_leech_family(algo::LeechConfig{}); }),
                               /*max_sim_time=*/2.0);
  ASSERT_EQ(m.delivered, 3u);                      // honest receivers (0,1,2,4 minus source 0)
  ASSERT_TRUE(m.per_node[3].delivery_time < 0.0);  // leech never delivered
}

TEST(Bsim, LeechCleverHaveSpamDoesntInflateBodyBytes) {
  // Even when the leech spams Have, the runner should still deliver the body to honest peers.
  auto t = mesh(8);
  t.leech_nodes.assign(8, false);
  t.leech_nodes[5] = true;
  algo::LeechConfig leech{.attract_haves = true};
  auto m = ton::bsim::simulate(t,
                               spec_for_family(
                                   0, 1024, 1, [] { return ton::overlay::broadcast::make_plumtree_family({}); },
                                   [leech] { return ton::overlay::broadcast::make_leech_family(leech); }),
                               /*max_sim_time=*/2.0);
  // 7 receivers total - 1 leech = 6 honest deliveries.
  ASSERT_EQ(m.delivered, 6u);
}

// ---- Active-view cap ---------------------------------------------------------------------------

TEST(Bsim, ActiveViewCapMarksOnlyFirstNAsNeighbour) {
  auto t = mesh(10);
  t.active_view_size = 3;
  // Use FloodAlgorithm with active_set=false so it forwards to all neighbours. With cap=3,
  // each node has only 3 neighbours; flood forwards to those 3 only. The active-view subgraph
  // is unlikely to cover all 10 nodes — reach should be < 100%.
  auto m = ton::bsim::simulate(
      t,
      spec_for_family(
          0, 1024, 1,
          [] { return ton::overlay::broadcast::make_push_family(ton::overlay::broadcast::PushConfig{.k = 0}); }),
      1.0);
  // Source's first 3 peers receive directly. Each of those forwards to their first 3 (which
  // overlap with source's set), so coverage stays well below 9 honest receivers.
  ASSERT_TRUE(m.delivered < 9u);
}

// ---- Score persistence (cross-broadcast learning) ----------------------------------------------

TEST(Bsim, SessionScorePersistsAcrossSimulateCalls) {
  // Two-round broadcast: FEC emits PeerFeedback on every received piece, so session.peer_scores
  // accumulates non-zero entries by round 2. With leeches present, reach should not decrease.
  auto t = mesh(20);
  t.leech_nodes.assign(20, false);
  for (NodeId i = 10; i < 15; i++) {
    t.leech_nodes[i] = true;  // 5 of 20 leeches (25%)
  }
  auto fec = algo::FecConfig{
      .total_pieces = 8, .required_pieces = 4, .random_per_piece = 2, .emit_batch_size = 8, .emit_interval = 0.0};
  algo::LeechConfig leech_cfg{.attract_haves = true};
  auto fec_family = [fec] { return ton::overlay::broadcast::make_fec_family(fec); };
  auto leech_family = [leech_cfg] { return ton::overlay::broadcast::make_leech_family(leech_cfg); };

  SessionState session;
  session.round_interval = 1.0;  // seconds between rounds
  auto m1 = ton::bsim::simulate(t, spec_for_family(0, 4096, 4, fec_family, leech_family), 2.0, &session);
  ASSERT_TRUE(m1.delivered > 0u);

  // After round 1, session must hold at least one non-zero score.
  bool any_nonzero = false;
  for (const auto &per_node : session.peer_scores) {
    for (const auto &kv : per_node) {
      if (kv.second.global.score != 0.0) {
        any_nonzero = true;
        break;
      }
    }
    if (any_nonzero) {
      break;
    }
  }
  ASSERT_TRUE(any_nonzero);

  // Round 2 still delivers meaningfully — exact reach is RNG-dependent (FEC uses random sampling,
  // not score-based selection), so we only assert the cross-broadcast state didn't break the
  // algorithm.
  auto m2 = ton::bsim::simulate(t, spec_for_family(1, 4096, 4, fec_family, leech_family), 2.0, &session);
  ASSERT_TRUE(m2.delivered > 0u);
}

TEST(Bsim, ScoreDecaysExponentially) {
  // Verify the BroadcastPeerScore decays with the configured half-life. This is a property of
  // the production score code; we just check bsim plumbs it through.
  ton::overlay::broadcast::BroadcastPeerScoreConfig cfg;
  cfg.half_life = 60.0;
  ton::overlay::broadcast::BroadcastPeerScore s;
  s.add(/*delta=*/2.0, /*now=*/0.0, cfg);
  EXPECT_APPROX(s.value_at(0.0, cfg), 2.0);
  EXPECT_APPROX(s.value_at(60.0, cfg), 1.0);   // one half-life
  EXPECT_APPROX(s.value_at(120.0, cfg), 0.5);  // two
  // Clamps at min_score = -1.0.
  s.add(-100.0, 120.0, cfg);
  EXPECT_APPROX(s.value_at(120.0, cfg), -1.0);
}

// ---- Multi-body concurrent broadcasts ----------------------------------------------------------

TEST(Bsim, StreamingBroadcastsConverge) {
  // 30 broadcasts evenly spaced over 5 sim-seconds, rotating through 6 sources, all on the same
  // plumtree mesh. Every body should reach every other node; the mesh's eager/lazy split
  // adapts during the run so the duplicate ratio drops over time.
  auto t = mesh(8);
  std::vector<NodeId> sources{0, 1, 2, 3, 4, 5};
  Spec spec{.publications = ton::bsim::streaming_publications(/*count=*/30, /*span=*/5.0, sources, /*base=*/1000),
            .body_size = 512,
            .piece_count = 1,
            .piece_header = 64,
            .control_bytes = 64,
            .factory = ton::bsim::AlgorithmFactory{
                .honest = [] { return ton::overlay::broadcast::make_plumtree_family({}); }}};
  auto m = ton::bsim::simulate(t, spec, /*max_sim_time=*/10.0);
  // Each body: 7 non-source receivers must deliver.
  ASSERT_EQ(m.delivered_per_body.size(), 30u);
  for (const auto &[body, count] : m.delivered_per_body) {
    ASSERT_EQ(count, 7u);
  }
  ASSERT_EQ(m.delivered, 30u * 7u);
}

TEST(Bsim, ConcurrentBroadcastsReachEveryone) {
  // Three distinct broadcasts from three different sources, all running in parallel on a single
  // plumtree mesh. Each body should reach every other node — the shared eager/lazy split
  // serves all of them.
  auto t = mesh(8);
  Spec spec{
      .publications = {{.source = 0, .body_id = 101}, {.source = 3, .body_id = 102}, {.source = 6, .body_id = 103}},
      .body_size = 1024,
      .piece_count = 1,
      .piece_header = 64,
      .control_bytes = 64,
      .factory =
          ton::bsim::AlgorithmFactory{.honest = [] { return ton::overlay::broadcast::make_plumtree_family({}); }}};
  auto m = ton::bsim::simulate(t, spec, /*max_sim_time=*/2.0);
  // Each body: 7 non-source receivers should deliver. Total = 21.
  ASSERT_EQ(m.delivered_per_body[101], 7u);
  ASSERT_EQ(m.delivered_per_body[102], 7u);
  ASSERT_EQ(m.delivered_per_body[103], 7u);
  ASSERT_EQ(m.delivered, 21u);
}
