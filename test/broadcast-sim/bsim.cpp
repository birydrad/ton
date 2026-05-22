#include <algorithm>
#include <map>
#include <queue>
#include <unordered_set>
#include <utility>
#include <variant>

#include "td/utils/check.h"
#include "td/utils/overloaded.h"

#include "bsim.h"

namespace ton::bsim {

namespace {

std::uint64_t message_size(const algo::Message &m, const Spec &spec) {
  return std::visit(
      td::overloaded([&](const algo::msg::Have &) -> std::uint64_t { return spec.control_bytes; },
                     [&](const algo::msg::Request &) -> std::uint64_t { return spec.control_bytes; },
                     [&](const algo::msg::Cancel &) -> std::uint64_t { return spec.control_bytes; },
                     [&](const algo::msg::sim::HavePieces &h) -> std::uint64_t {
                       return spec.control_bytes + h.pieces.size() * 4 + h.sender_mask.size() * 8;
                     },
                     [&](const algo::msg::sim::RequestPieces &r) -> std::uint64_t {
                       return spec.control_bytes + r.needs.size() * 4;
                     },
                     [&](const algo::msg::sim::RequestAnyPieces &) -> std::uint64_t { return spec.control_bytes; },
                     [&](const algo::msg::sim::HaveBucket &h) -> std::uint64_t {
                       return spec.control_bytes + h.sender_mask.size() * 8;
                     },
                     [&](const algo::msg::sim::SubscribeBucket &s) -> std::uint64_t {
                       return spec.control_bytes + s.sender_mask.size() * 8;
                     },
                     [&](const algo::msg::sim::UnsubscribeBucket &) -> std::uint64_t { return spec.control_bytes; },
                     [&](const algo::msg::Piece &piece) -> std::uint64_t {
                       auto pieces = std::max<std::uint32_t>(spec.piece_count, 1);
                       return (spec.body_size + pieces - 1) / pieces + spec.piece_header + piece.sender_mask.size() * 8;
                     }),
      m);
}

bool is_body_message(const algo::Message &m) {
  return std::holds_alternative<algo::msg::Piece>(m);
}

bool is_leech(const Topology &topo, NodeId node) {
  return node < topo.leech_nodes.size() && topo.leech_nodes[node];
}

constexpr double kClockOrigin = 1.0;

// Wall-clock-aware sim time: bsim updates this before each simulate() call so the algorithm's
// `now()` is monotonically increasing across rounds. Used by the sub-direct algorithm's
// subscriber-expiry math (needs absolute time, not per-round sim time).
double g_wall_offset = 0.0;

td::Timestamp sim_timestamp(double now) {
  return td::Timestamp::at(kClockOrigin + g_wall_offset + now);
}

double sim_time(td::Timestamp timestamp) {
  return timestamp.at() - kClockOrigin - g_wall_offset;
}

// Per-broadcast state on a single node. The algorithm + decoder dedup set + delivery flag are
// all body-specific; multiple BroadcastInstances coexist when several broadcasts run in parallel.
struct BroadcastInstance {
  std::unique_ptr<algo::BroadcastAlgorithm> algorithm;
  // `received_pieces` is also bumped on apply_send so an echo back to the sender shows up as a
  // duplicate at the receiver's dup check. For "did this node physically have the piece before
  // it tried to send it?" we keep a separate set updated only on apply_recv.
  std::unordered_set<algo::PieceId> received_pieces;
  std::unordered_set<algo::PieceId> physically_received;
  bool body_ready_fired = false;
  bool has_body = false;
  td::Timestamp scheduled_alarm;
  double delivery_time = -1.0;  // -1 = never delivered
  bool delivery_counted = false;
};

// All per-node state owned by the engine. Cross-broadcast (family, peer_state, upload pipe) lives
// here; per-broadcast state is in `broadcasts[body_id]`.
struct NodeState {
  // Per-node algorithm family (key + Shared + make_algorithm). One Shared instance carries
  // state across all broadcasts running on this node — that's the point of the typed Shared
  // pattern (e.g. Plumtree's persistent eager/lazy mesh serves every concurrent broadcast).
  std::optional<::ton::overlay::broadcast::AlgorithmFamily> family;
  std::unordered_map<algo::PeerId, algo::Peer> peer_state;  // bsim's authoritative score view
  double upload_busy_until = 0.0;                           // FIFO upload-pipe free-at timestamp
  std::unordered_map<BodyId, BroadcastInstance> broadcasts;
};

struct ScheduledEvent {
  double at = 0.0;
  std::uint64_t order = 0;  // tiebreaker so the queue is deterministic
  NodeId node = 0;
  BodyId body_id = 0;
  std::variant<algo::evt::Receive, algo::evt::Timer> what;

  bool operator>(const ScheduledEvent &o) const {
    return at != o.at ? at > o.at : order > o.order;
  }
};

class Engine {
 public:
  Engine(const Topology &topo, const Spec &spec, double max_sim_time)
      : topology_(topo), spec_(spec), max_sim_time_(max_sim_time), nodes_(topo.node_count) {
    metrics_.per_node.resize(topo.node_count);
    g_wall_offset = 0.0;  // reset wall clock for non-session (single-shot) runs.
  }

  void set_session(SessionState *session, double wall_time_offset) {
    session_ = session;
    wall_time_offset_ = wall_time_offset;
    g_wall_offset = wall_time_offset;  // HACK: see g_wall_offset comment.
  }

  Metrics run() {
    build_publications();
    seed_shared();
    publish();
    drain();
    return std::move(metrics_);
  }

 private:
  // ---- single-write peer seeding -----------------------------------------------------------

  // Production engine fans peer churn out to each Shared exactly once. In bsim the topology is
  // fixed for the lifetime of a simulate() call, so for every honest node we instantiate its
  // family eagerly and replay on_peer_upsert on its Shared for every other honest peer.
  void seed_shared() {
    auto cap = topology_.active_view_size;
    for (NodeId self = 0; self < topology_.node_count; self++) {
      if (is_leech(topology_, self)) {
        continue;
      }
      auto &state = nodes_[self];
      ensure_family(state, self);
      // Match populate_peers_for_algorithm: each peer is upserted with the same flags the
      // PeerUpsert event carries, so an algorithm reading from shared.peer_table sees identical
      // state to one consuming events from evt::PeerUpsert.
      if (spec_.all_peers_neighbours) {
        // Fully-meshed overlay (twostep / optimum-p2p assume every honest pair is a direct edge).
        for (NodeId peer_id = 0; peer_id < topology_.node_count; peer_id++) {
          if (peer_id == self || is_leech(topology_, peer_id)) {
            continue;
          }
          algo::Peer p{.id = peer_id,
                       .score = seeded_score(self, peer_id),
                       .neighbour = true,
                       .persistent = spec_.all_peers_persistent};
          state.family->shared->on_peer_upsert(p);
        }
      } else {
        const auto &graph = topology_.peers[self];
        for (std::size_t i = 0; i < graph.size(); i++) {
          auto peer_id = graph[i];
          if (peer_id == self || is_leech(topology_, peer_id)) {
            continue;
          }
          algo::Peer p{.id = peer_id,
                       .score = seeded_score(self, peer_id),
                       .neighbour = (cap == 0) || (i < cap),
                       .persistent = spec_.all_peers_persistent};
          state.family->shared->on_peer_upsert(p);
        }
      }
    }
  }

  void ensure_family(NodeState &state, NodeId node) {
    if (state.family) {
      return;
    }
    // Reuse from SessionState if it's already there (cross-round persistence). Otherwise
    // instantiate via the Spec factory and stash back for the next simulate() call.
    if (session_ != nullptr) {
      if (session_->families.size() < topology_.node_count) {
        session_->families.resize(topology_.node_count);
      }
      auto &slot = session_->families[node];
      if (!slot) {
        slot = (is_leech(topology_, node) && spec_.factory.leech) ? spec_.factory.leech() : spec_.factory.honest();
      }
      state.family = *slot;
      return;
    }
    state.family = (is_leech(topology_, node) && spec_.factory.leech) ? spec_.factory.leech() : spec_.factory.honest();
  }

  // ---- publish / drain ----------------------------------------------------------------------

  // Expand spec_.publications, falling back to the legacy single-body fields when empty.
  void build_publications() {
    if (!spec_.publications.empty()) {
      publications_ = spec_.publications;
    } else {
      publications_.push_back({.source = spec_.source, .body_id = spec_.body_id, .at = 0.0});
      for (auto extra : spec_.additional_sources) {
        if (extra == spec_.source)
          continue;
        publications_.push_back({.source = extra, .body_id = spec_.body_id, .at = 0.0});
      }
    }
    for (auto &p : publications_) {
      publishers_per_body_[p.body_id].insert(p.source);
      body_ids_.insert(p.body_id);
    }
  }

  void publish() {
    // Anything published at t=0 fires immediately; later ones go through the event queue.
    for (auto &p : publications_) {
      if (p.at <= 0.0) {
        fire_publish(p);
      } else {
        queue_.push(ScheduledEvent{
            .at = p.at, .order = next_order_++, .node = p.source, .body_id = p.body_id, .what = algo::evt::Timer{}});
        pending_publishes_[{p.source, p.body_id}] = p.at;
      }
    }
  }

  void fire_publish(const Publication &p) {
    auto &inst = ensure_broadcast(p.source, p.body_id, sim_timestamp(p.at));
    inst.has_body = true;
    auto &pb = metrics_.per_body[p.body_id];
    if (pb.publish_time < 0.0) {
      pb.publish_time = p.at;
    }
    apply_actions(p.at, p.source, p.body_id, step_algorithm(p.at, *inst.algorithm, algo::evt::Publish{}));
    schedule_algorithm_alarm(p.at, p.source, p.body_id);
  }

  bool is_publisher(NodeId node, BodyId body_id) const {
    auto it = publishers_per_body_.find(body_id);
    return it != publishers_per_body_.end() && it->second.count(node) != 0;
  }

  void drain() {
    while (!queue_.empty()) {
      auto event = queue_.top();
      queue_.pop();
      if (event.at > max_sim_time_) {
        break;
      }
      // Deferred Publish: fire when the event's slot for (source, body_id) is reached.
      auto pending_it = pending_publishes_.find({event.node, event.body_id});
      if (pending_it != pending_publishes_.end() && pending_it->second == event.at &&
          std::holds_alternative<algo::evt::Timer>(event.what)) {
        pending_publishes_.erase(pending_it);
        for (auto &p : publications_) {
          if (p.source == event.node && p.body_id == event.body_id && p.at == event.at) {
            fire_publish(p);
            break;
          }
        }
        continue;
      }
      if (std::holds_alternative<algo::evt::Timer>(event.what)) {
        auto &state = nodes_[event.node];
        auto inst_it = state.broadcasts.find(event.body_id);
        if (inst_it == state.broadcasts.end())
          continue;
        auto &alarm = inst_it->second.scheduled_alarm;
        if (!alarm || !(alarm == sim_timestamp(event.at))) {
          continue;
        }
        alarm = td::Timestamp::never();
      }
      auto actions = std::visit(
          [&](auto &&e) { return handle_event(event.at, event.node, event.body_id, std::forward<decltype(e)>(e)); },
          event.what);
      apply_actions(event.at, event.node, event.body_id, std::move(actions));
      schedule_algorithm_alarm(event.at, event.node, event.body_id);
    }
  }

  // ---- inbound event handlers --------------------------------------------------------------

  algo::Actions handle_event(double now, NodeId self, BodyId body_id, algo::evt::Timer t) {
    auto &inst = ensure_broadcast(self, body_id, sim_timestamp(now));
    return step_algorithm(now, *inst.algorithm, algo::Event{t});
  }

  algo::Actions handle_event(double now, NodeId self, BodyId body_id, algo::evt::Receive recv) {
    auto &inst = ensure_broadcast(self, body_id, sim_timestamp(now));
    ensure_peer_registered(now, self, recv.from);
    if (!is_body_message(recv.message)) {
      return step_algorithm(now, *inst.algorithm, algo::Event{std::move(recv)});
    }
    auto &piece = std::get<algo::msg::Piece>(recv.message);
    bool dup = inst.received_pieces.count(piece.id) != 0;
    piece.duplicate = dup;
    if (dup) {
      metrics_.duplicate_messages++;
      metrics_.per_node[self].duplicate_messages_in++;
      metrics_.per_body[body_id].duplicate_messages++;
    } else {
      inst.received_pieces.insert(piece.id);
      inst.physically_received.insert(piece.id);
    }
    auto out = step_algorithm(now, *inst.algorithm, algo::Event{std::move(recv)});
    auto pieces_required = std::max<std::uint32_t>(spec_.piece_count, 1);
    if (!inst.body_ready_fired && inst.received_pieces.size() >= pieces_required) {
      inst.body_ready_fired = true;
      inst.has_body = true;
      auto more = step_algorithm(now, *inst.algorithm, algo::Event{algo::evt::BodyReady{}});
      out.items.insert(out.items.end(), std::make_move_iterator(more.items.begin()),
                       std::make_move_iterator(more.items.end()));
    }
    return out;
  }

  // ---- outbound action handlers ------------------------------------------------------------

  void apply_actions(double now, NodeId self, BodyId body_id, algo::Actions actions) {
    for (auto &action : actions.items) {
      std::visit([&](const auto &a) { apply(self, now, body_id, a); }, action);
    }
  }

  void apply(NodeId self, double now, BodyId body_id, const algo::act::Deliver &) {
    auto &inst = nodes_[self].broadcasts.at(body_id);
    if (inst.delivery_counted || is_publisher(self, body_id)) {
      return;
    }
    inst.delivery_counted = true;
    inst.delivery_time = now;
    auto &per_node = metrics_.per_node[self];
    if (per_node.delivery_time < 0.0) {
      per_node.delivery_time = now;
    }
    metrics_.delivered++;
    metrics_.delivered_per_body[body_id]++;
    metrics_.last_delivery = std::max(metrics_.last_delivery, now);
    auto &pb = metrics_.per_body[body_id];
    if (pb.publish_time >= 0.0) {
      pb.delivery_latency.push_back(now - pb.publish_time);
    }
  }

  void apply(NodeId self, double now, BodyId body_id, const algo::act::Send &send) {
    CHECK(send.peer < topology_.node_count);
    bool body = is_body_message(send.message);
    auto bytes = message_size(send.message, spec_);
    auto &pb = metrics_.per_body[body_id];
    if (body) {
      auto pid = std::get<algo::msg::Piece>(send.message).id;
      // Catch algorithm bugs early: a node can only legitimately send a piece if it actually
      // holds it. Sources own the whole body (is_publisher), nodes that fired BodyReady can
      // FEC-decode any piece, and anyone else must have physically received this specific id.
      // RLNC algorithms (optimum-p2p) synthesise derived pieces via linear combinations and
      // emit them with locally-allocated piece-ids above `kLocalPieceIdBase = 1<<31`; those
      // are legitimate even before BodyReady.
      auto &inst_check = nodes_[self].broadcasts[body_id];
      bool rlnc_synth = pid >= (1u << 31);
      bool may_serve = is_publisher(self, body_id) || inst_check.body_ready_fired || rlnc_synth ||
                       inst_check.physically_received.count(pid) > 0;
      CHECK(may_serve);
      // Sender now "has" this piece — any echo back must show up as duplicate at the receiver.
      nodes_[self].broadcasts[body_id].received_pieces.insert(pid);
      metrics_.body_messages++;
      metrics_.body_bytes += bytes;
      metrics_.per_node[self].body_messages_out++;
      metrics_.per_node[self].body_bytes_out += bytes;
      metrics_.per_node[send.peer].body_messages_in++;
      metrics_.per_node[send.peer].body_bytes_in += bytes;
      pb.body_messages++;
      pb.body_bytes += bytes;
    } else {
      metrics_.control_messages++;
      metrics_.control_bytes += bytes;
      metrics_.per_node[self].control_bytes_out += bytes;
      pb.control_messages++;
      pb.control_bytes += bytes;
    }
    auto &busy = nodes_[self].upload_busy_until;
    double tx_start = std::max(now, busy);
    busy = tx_start + static_cast<double>(bytes) / topology_.upload_bw;
    double arrive = busy + topology_.latency[self][send.peer];
    enqueue(arrive, send.peer, body_id, algo::evt::Receive{.from = self, .message = send.message});
  }

  void apply(NodeId self, double now, BodyId body_id, const algo::act::PeerFeedback &fb) {
    auto &state = nodes_[self];
    auto wall_now = wall_time_offset_ + now;
    auto it = state.peer_state.find(fb.peer);
    if (it == state.peer_state.end()) {
      return;  // unknown peer (asymmetric edge sender) — silently drop the score delta
    }
    if (session_ != nullptr) {
      auto &g = session_->peer_scores[self][fb.peer].global;
      g.score = g.value_at(wall_now, session_->score_config);
      g.updated_at = wall_now;
      g.add(fb.delta, wall_now, session_->score_config);
      it->second.score = g.score;
    } else {
      it->second.score += fb.delta;
    }
    // Fan PeerUpsert to every active broadcast on this node: each carries its own per-broadcast
    // peer table (or reads via Shared) and needs to see the score change.
    for (auto &[other_body_id, inst] : state.broadcasts) {
      auto more = step_algorithm(now, *inst.algorithm, algo::evt::PeerUpsert{it->second});
      apply_actions(now, self, other_body_id, std::move(more));
      schedule_algorithm_alarm(now, self, other_body_id);
    }
  }

  // ---- support ------------------------------------------------------------------------------

  algo::Actions step_algorithm(double now, algo::BroadcastAlgorithm &algorithm, algo::Event event) {
    algorithm.set_now(sim_timestamp(now));
    return algorithm.handle_event(event);
  }

  // Find or create the BroadcastInstance for (node, body_id). On first creation, build the
  // algorithm and seed it with the current peer view; subsequent calls return the cached instance.
  BroadcastInstance &ensure_broadcast(NodeId node, BodyId body_id, td::Timestamp now) {
    auto &state = nodes_[node];
    auto it = state.broadcasts.find(body_id);
    if (it != state.broadcasts.end()) {
      return it->second;
    }
    ensure_family(state, node);
    ensure_peer_seed(node);
    NodeId origin = node;
    auto pub_it = publishers_per_body_.find(body_id);
    if (pub_it != publishers_per_body_.end() && !pub_it->second.empty()) {
      origin = *pub_it->second.begin();
    }
    algo::BroadcastInit init{.id = body_id,
                             .self = node,
                             .origin = origin,
                             .required_pieces = std::max<std::uint32_t>(spec_.piece_count, 1)};
    BroadcastInstance inst;
    inst.algorithm = state.family->make_algorithm(init);
    std::vector<algo::Peer> peers;
    peers.reserve(state.peer_state.size());
    for (auto &[id, peer] : state.peer_state) {
      peers.push_back(peer);
    }
    algo::register_peers(*inst.algorithm, std::move(peers), now);
    auto [ins_it, _] = state.broadcasts.emplace(body_id, std::move(inst));
    return ins_it->second;
  }

  // Populate state.peer_state from the topology on first touch. Idempotent.
  void ensure_peer_seed(NodeId node) {
    auto &state = nodes_[node];
    if (!state.peer_state.empty())
      return;
    auto cap = topology_.active_view_size;
    if (spec_.all_peers_neighbours) {
      for (NodeId peer_id = 0; peer_id < topology_.node_count; peer_id++) {
        if (peer_id == node)
          continue;
        state.peer_state[peer_id] = algo::Peer{.id = peer_id,
                                               .score = seeded_score(node, peer_id),
                                               .neighbour = true,
                                               .persistent = spec_.all_peers_persistent};
      }
    } else {
      for (std::size_t i = 0; i < topology_.peers[node].size(); i++) {
        auto peer_id = topology_.peers[node][i];
        state.peer_state[peer_id] = algo::Peer{.id = peer_id,
                                               .score = seeded_score(node, peer_id),
                                               .neighbour = (cap == 0) || (i < cap),
                                               .persistent = spec_.all_peers_persistent};
      }
    }
  }

  // First-message-from-unknown-peer fallback: register the peer in this node's view and fan a
  // PeerUpsert to every active broadcast on the node.
  void ensure_peer_registered(double now, NodeId self, algo::PeerId peer) {
    auto &state = nodes_[self];
    if (state.peer_state.count(peer))
      return;
    algo::Peer p{.id = peer,
                 .score = seeded_score(self, peer),
                 .neighbour = spec_.all_peers_neighbours,
                 .persistent = spec_.all_peers_persistent};
    state.peer_state[peer] = p;
    for (auto &[body_id, inst] : state.broadcasts) {
      apply_actions(now, self, body_id, step_algorithm(now, *inst.algorithm, algo::evt::PeerUpsert{p}));
    }
  }

  double seeded_score(NodeId node, algo::PeerId peer) const {
    if (session_ == nullptr) {
      return 0.0;
    }
    auto it = session_->peer_scores[node].find(peer);
    if (it == session_->peer_scores[node].end()) {
      return 0.0;
    }
    return it->second.global.value_at(wall_time_offset_, session_->score_config);
  }

  void enqueue(double at, NodeId node, BodyId body_id, std::variant<algo::evt::Receive, algo::evt::Timer> what) {
    if (at > max_sim_time_)
      return;
    queue_.push(
        ScheduledEvent{.at = at, .order = next_order_++, .node = node, .body_id = body_id, .what = std::move(what)});
  }

  void schedule_algorithm_alarm(double now, NodeId node, BodyId body_id) {
    auto &inst = ensure_broadcast(node, body_id, sim_timestamp(now));
    auto alarm = inst.algorithm->alarm();
    auto &scheduled = inst.scheduled_alarm;
    if (!alarm) {
      scheduled = td::Timestamp::never();
      return;
    }
    CHECK(alarm.at() >= sim_timestamp(now).at() - 0.001);
    if (scheduled == alarm)
      return;
    auto at = sim_time(alarm);
    scheduled = alarm;
    enqueue(at, node, body_id, algo::evt::Timer{});
  }

  // ---- members --------------------------------------------------------------------------------

  const Topology &topology_;
  const Spec &spec_;
  double max_sim_time_;

  Metrics metrics_;
  std::priority_queue<ScheduledEvent, std::vector<ScheduledEvent>, std::greater<>> queue_;
  std::vector<NodeState> nodes_;
  std::vector<Publication> publications_;
  std::unordered_map<BodyId, std::unordered_set<NodeId>> publishers_per_body_;
  std::unordered_set<BodyId> body_ids_;
  // Deferred Publish bookkeeping: (source, body_id) → planned sim time. Used by drain() to match
  // a Timer slot with its pending Publish.
  std::map<std::pair<NodeId, BodyId>, double> pending_publishes_;
  SessionState *session_ = nullptr;
  double wall_time_offset_ = 0.0;
  std::uint64_t next_order_ = 0;
};

}  // namespace

double PerBody::percentile_delivery(double q) const {
  if (delivery_latency.empty()) {
    return 0.0;
  }
  auto sorted = delivery_latency;
  std::sort(sorted.begin(), sorted.end());
  return sorted[static_cast<size_t>(q * (sorted.size() - 1))];
}

double Metrics::percentile_delivery(double q) const {
  std::vector<double> times;
  times.reserve(per_node.size());
  for (const auto &n : per_node) {
    if (n.delivery_time >= 0.0) {
      times.push_back(n.delivery_time);
    }
  }
  if (times.empty()) {
    return 0.0;
  }
  std::sort(times.begin(), times.end());
  return times[static_cast<size_t>(q * (times.size() - 1))];
}

std::uint64_t Metrics::percentile_body_bytes_out(double q) const {
  std::vector<std::uint64_t> bytes;
  bytes.reserve(per_node.size());
  for (const auto &n : per_node) {
    bytes.push_back(n.body_bytes_out);
  }
  if (bytes.empty()) {
    return 0;
  }
  std::sort(bytes.begin(), bytes.end());
  return bytes[static_cast<size_t>(q * (bytes.size() - 1))];
}

std::uint64_t Metrics::max_body_bytes_out() const {
  std::uint64_t m = 0;
  for (const auto &n : per_node) {
    m = std::max(m, n.body_bytes_out);
  }
  return m;
}

std::uint64_t Metrics::max_body_bytes_in() const {
  std::uint64_t m = 0;
  for (const auto &n : per_node) {
    m = std::max(m, n.body_bytes_in);
  }
  return m;
}

Metrics simulate(const Topology &topology, const Spec &spec, double max_sim_time, SessionState *session) {
  CHECK(topology.peers.size() == topology.node_count);
  CHECK(topology.latency.size() == topology.node_count);
  CHECK(spec.source < topology.node_count);
  CHECK(spec.factory.honest);
  Engine engine(topology, spec, max_sim_time);
  if (session != nullptr) {
    if (session->peer_scores.size() < topology.node_count) {
      session->peer_scores.resize(topology.node_count);
    }
    engine.set_session(session, session->next_wall_time);
  }
  auto metrics = engine.run();
  if (session != nullptr) {
    session->next_wall_time += session->round_interval;
  }
  return metrics;
}

std::vector<Publication> streaming_publications(std::uint32_t count, double span, const std::vector<NodeId> &pool,
                                                BodyId base_body_id) {
  CHECK(!pool.empty());
  std::vector<Publication> out;
  out.reserve(count);
  for (std::uint32_t i = 0; i < count; i++) {
    double at = count <= 1 ? 0.0 : (span * static_cast<double>(i)) / static_cast<double>(count - 1);
    out.push_back(Publication{.source = pool[i % pool.size()], .body_id = base_body_id + i, .at = at});
  }
  return out;
}

}  // namespace ton::bsim
