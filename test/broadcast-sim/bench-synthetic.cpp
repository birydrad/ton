#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <queue>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "overlay/broadcast/algorithms/leech.h"
#include "td/utils/OptionParser.h"
#include "td/utils/Status.h"
#include "td/utils/StringBuilder.h"
#include "td/utils/check.h"
#include "td/utils/logging.h"
#include "td/utils/misc.h"

#include "bsim.h"
#include "catalogue.h"

namespace ton::bsim_runner {

// Defined in bench-mainnet.cpp; reused here so the synthetic --csv path emits the same
// `small`/`medium`/`large` size classes the mainnet path does.
std::string size_class(std::uint64_t body_size);

namespace {

using bsim::BodyId;
using bsim::NodeId;
using bsim::Spec;
using bsim::Topology;

enum class SourcePolicy { Fixed, RotatingHonest };
enum class StatePolicy { Fresh, PersistentScores };
enum class Severity { Info, Warning, Fatal };
enum class View { Algorithm, Scenario, Both };

struct HealthPolicy {
  double min_reach_ratio = 0.99;
  double max_source_x = 16.0;
  double max_total_x = 16.0;
  double max_duplicate_ratio = 1.0;
  double max_control_ratio = 0.50;
  double max_p95_latency_ms = 1000.0;
  bool allow_zero_reach = false;
};

struct Workload {
  std::uint64_t body_size = 0;
  std::uint32_t rounds = 1;
  double max_sim_time = 10.0;
  double round_interval = 60.0;
  SourcePolicy source_policy = SourcePolicy::Fixed;
  StatePolicy state_policy = StatePolicy::Fresh;
};

struct Scenario {
  std::string name;
  Topology topology;
  Workload workload;
  HealthPolicy health;
  NodeId fixed_source = 0;
};

struct HealthFinding {
  Severity severity = Severity::Info;
  std::string code;
};

struct HealthReport {
  std::vector<HealthFinding> findings;

  bool has_fatal() const {
    for (const auto &finding : findings) {
      if (finding.severity == Severity::Fatal) {
        return true;
      }
    }
    return false;
  }

  bool has_warning() const {
    for (const auto &finding : findings) {
      if (finding.severity == Severity::Warning) {
        return true;
      }
    }
    return false;
  }

  std::string label() const {
    if (findings.empty()) {
      return "ok";
    }
    std::string out;
    bool fatal = has_fatal();
    if (fatal) {
      out = "FATAL:";
    }
    for (const auto &finding : findings) {
      if (!out.empty() && out.back() != ':') {
        out += ",";
      }
      out += finding.code;
    }
    return out;
  }
};

struct RunRow {
  std::string scenario;
  std::string algorithm;
  std::uint64_t seed = 0;
  std::uint32_t round = 0;
  NodeId source = 0;
  std::uint32_t delivered = 0;
  std::uint32_t expected = 0;
  double reach_ratio = 0.0;
  double total_x = 0.0;
  double source_x = 0.0;
  double p95_out_amp = 0.0;
  double p95_in_amp = 0.0;
  double honest_x = 0.0;        // body-bytes-out / honest_count / body_size
  double honest_total_x = 0.0;  // (body+control)-bytes-out / honest_count / body_size
  double p50_ms = 0.0;
  double p95_ms = 0.0;
  double p99_ms = 0.0;
  double duplicate_ratio = 0.0;
  double control_ratio = 0.0;
  HealthReport health;
};

struct SummaryRow {
  std::string label;
  std::uint32_t runs = 0;
  std::uint32_t measured = 0;
  std::uint32_t fatal = 0;
  std::uint32_t warning = 0;
  double reach_min = 1.0;
  double reach_sum = 0.0;
  double total_x_sum = 0.0;
  double p95_out_sum = 0.0;
  double source_x_sum = 0.0;
  double p95_ms_sum = 0.0;
  double duplicate_sum = 0.0;
  bool has_infinite_latency = false;
  std::set<std::string> health_codes;

  void add(const RunRow &row) {
    runs++;
    fatal += row.health.has_fatal() ? 1 : 0;
    warning += row.health.has_warning() ? 1 : 0;
    for (const auto &finding : row.health.findings) {
      health_codes.insert(finding.code);
    }
    if (row.expected == 0) {
      return;
    }
    measured++;
    reach_min = std::min(reach_min, row.reach_ratio);
    reach_sum += row.reach_ratio;
    total_x_sum += row.total_x;
    p95_out_sum += row.p95_out_amp;
    source_x_sum += row.source_x;
    duplicate_sum += row.duplicate_ratio;
    if (std::isfinite(row.p95_ms)) {
      p95_ms_sum += row.p95_ms;
    } else {
      has_infinite_latency = true;
    }
  }

  double reach_avg() const {
    return measured == 0 ? 0.0 : reach_sum / measured;
  }
  double total_x_avg() const {
    return measured == 0 ? 0.0 : total_x_sum / measured;
  }
  double p95_out_avg() const {
    return measured == 0 ? 0.0 : p95_out_sum / measured;
  }
  double source_x_avg() const {
    return measured == 0 ? 0.0 : source_x_sum / measured;
  }
  double p95_ms_avg() const {
    if (measured == 0) {
      return 0.0;
    }
    return has_infinite_latency ? std::numeric_limits<double>::infinity() : p95_ms_sum / measured;
  }
  double duplicate_avg() const {
    return measured == 0 ? 0.0 : duplicate_sum / measured;
  }
  std::string health_label() const {
    if (health_codes.empty()) {
      return "ok";
    }
    std::string out;
    for (const auto &code : health_codes) {
      if (!out.empty()) {
        out += ",";
      }
      out += code;
    }
    return out;
  }
};

struct CliConfig {
  std::string suite = "quick";
  std::vector<std::string> algorithm_filter;
  std::string only;
  std::uint64_t seed = 1;
  std::uint32_t seeds = 1;
  std::uint32_t rounds = 0;
  double max_sim_time = 10.0;
  std::vector<std::uint64_t> body_sizes;
  std::string csv_path;  // when set, emit v2-schema CSV (one row per algo×scenario aggregated).
  View view = View::Algorithm;
  bool body_size_override = false;
  bool list_algorithms = false;
  bool algo_details = false;
  bool details = false;
};

bool is_leech(const Topology &topology, NodeId node) {
  return node < topology.leech_nodes.size() && topology.leech_nodes[node];
}

void init_latency(Topology &topology, double latency) {
  topology.latency.assign(topology.node_count, std::vector<double>(topology.node_count, latency));
  for (NodeId i = 0; i < topology.node_count; i++) {
    topology.latency[i][i] = 0.0;
  }
}

void add_edge(Topology &topology, NodeId from, NodeId to) {
  CHECK(from < topology.node_count);
  CHECK(to < topology.node_count);
  if (from != to) {
    topology.peers[from].push_back(to);
  }
}

Topology mesh(std::uint32_t n, double latency = 0.010) {
  Topology topology;
  topology.node_count = n;
  topology.peers.assign(n, {});
  init_latency(topology, latency);
  for (NodeId i = 0; i < n; i++) {
    for (NodeId j = 0; j < n; j++) {
      add_edge(topology, i, j);
    }
  }
  return topology;
}

// Randomize peers[i] per node — only matters when `active_view_size > 0`, which makes the first
// K entries the algorithm's neighbour set. Without this, mesh()'s canonical i=0..n-1 order has
// every node seeing the same first K peers and broadcast stays trapped in that subgraph.
void shuffle_peer_lists(Topology &topology, std::uint64_t seed) {
  for (NodeId i = 0; i < topology.node_count; i++) {
    std::mt19937_64 rng(seed ^ (static_cast<std::uint64_t>(i) * 0x9E3779B97F4A7C15ULL));
    std::shuffle(topology.peers[i].begin(), topology.peers[i].end(), rng);
  }
}

Topology line(std::uint32_t n, double latency = 0.010) {
  Topology topology;
  topology.node_count = n;
  topology.peers.assign(n, {});
  init_latency(topology, latency);
  for (NodeId i = 0; i + 1 < n; i++) {
    add_edge(topology, i, i + 1);
    add_edge(topology, i + 1, i);
  }
  return topology;
}

Topology star(std::uint32_t n, double latency = 0.010) {
  Topology topology;
  topology.node_count = n;
  topology.peers.assign(n, {});
  init_latency(topology, latency);
  for (NodeId i = 1; i < n; i++) {
    add_edge(topology, 0, i);
    add_edge(topology, i, 0);
  }
  return topology;
}

Topology disconnected_source(double latency = 0.010) {
  auto topology = mesh(4, latency);
  topology.peers[0].clear();
  for (NodeId i = 1; i < topology.node_count; i++) {
    auto &peers = topology.peers[i];
    peers.erase(std::remove(peers.begin(), peers.end(), 0), peers.end());
  }
  return topology;
}

Topology random_regular(std::uint32_t n, std::uint32_t degree, std::uint64_t seed, double latency = 0.010) {
  Topology topology;
  topology.node_count = n;
  topology.peers.assign(n, {});
  init_latency(topology, latency);
  std::vector<std::unordered_set<NodeId>> peers(n);
  for (NodeId i = 0; i < n; i++) {
    peers[i].insert((i + 1) % n);
    peers[i].insert((i + n - 1) % n);
  }
  std::mt19937 rng(static_cast<std::uint32_t>(seed));
  for (NodeId i = 0; i < n; i++) {
    while (peers[i].size() < degree) {
      auto peer = static_cast<NodeId>(rng() % n);
      if (peer != i) {
        peers[i].insert(peer);
      }
    }
  }
  for (NodeId i = 0; i < n; i++) {
    topology.peers[i].assign(peers[i].begin(), peers[i].end());
    std::sort(topology.peers[i].begin(), topology.peers[i].end());
  }
  return topology;
}

Topology with_leeches(Topology topology, double ratio, std::uint64_t seed) {
  topology.leech_nodes.assign(topology.node_count, false);
  std::vector<NodeId> candidates;
  for (NodeId i = 1; i < topology.node_count; i++) {
    candidates.push_back(i);
  }
  std::shuffle(candidates.begin(), candidates.end(), std::mt19937(static_cast<std::uint32_t>(seed)));
  auto count =
      std::min<std::size_t>(candidates.size(), static_cast<std::size_t>(std::round(ratio * topology.node_count)));
  for (std::size_t i = 0; i < count; i++) {
    topology.leech_nodes[candidates[i]] = true;
  }
  return topology;
}

std::string size_name(std::uint64_t body_size) {
  if (body_size == 256) {
    return "tiny";
  }
  if (body_size == 4 * 1024) {
    return "small";
  }
  if (body_size == 100 * 1024) {
    return "medium";
  }
  if (body_size == 1024 * 1024) {
    return "large";
  }
  return std::to_string(body_size) + "B";
}

HealthPolicy policy_for(std::uint64_t body_size) {
  HealthPolicy policy;
  if (body_size <= 256) {
    policy.max_control_ratio = 1.0;
    policy.max_total_x = 128.0;
    policy.max_p95_latency_ms = 500.0;
  } else if (body_size >= 1024 * 1024) {
    policy.max_control_ratio = 0.25;
    policy.max_total_x = 16.0;
    policy.max_p95_latency_ms = 5000.0;
  }
  return policy;
}

Scenario make_scenario(std::string name, Topology topology, std::uint64_t body_size, Workload workload) {
  workload.body_size = body_size;
  return Scenario{std::move(name) + "/" + size_name(body_size), std::move(topology), workload, policy_for(body_size)};
}

std::vector<std::uint64_t> default_body_sizes(const CliConfig &cfg) {
  if (cfg.body_size_override) {
    return cfg.body_sizes;
  }
  if (cfg.suite == "default") {
    return {256, 100 * 1024, 1024 * 1024};
  }
  return {256, 100 * 1024};
}

std::vector<Scenario> make_scenarios(const CliConfig &cfg, std::uint64_t seed) {
  std::vector<Scenario> out;
  auto sizes = default_body_sizes(cfg);
  auto rounds = cfg.rounds == 0 ? 1 : cfg.rounds;
  for (auto body_size : sizes) {
    Workload one;
    one.rounds = rounds;
    one.max_sim_time = cfg.max_sim_time;
    if (cfg.suite == "quick") {
      auto clean = random_regular(64, 8, seed);
      out.push_back(make_scenario("clean", clean, body_size, one));
      out.push_back(make_scenario("leech30", with_leeches(clean, 0.30, seed + 1009), body_size, one));
      continue;
    }
    out.push_back(make_scenario("mesh-8", mesh(8), body_size, one));
    out.push_back(make_scenario("line-4", line(4), body_size, one));
    out.push_back(make_scenario("star-8", star(8), body_size, one));

    auto disconnected = make_scenario("disconnected-source", disconnected_source(), body_size, one);
    disconnected.health.allow_zero_reach = true;
    out.push_back(std::move(disconnected));

    Workload repeat = one;
    repeat.rounds = cfg.rounds == 0 ? 4 : cfg.rounds;
    repeat.source_policy = SourcePolicy::RotatingHonest;
    repeat.state_policy = StatePolicy::PersistentScores;
    out.push_back(make_scenario("repeat-8", mesh(8), body_size, repeat));

    if (cfg.suite == "default") {
      out.push_back(make_scenario("mesh-32", mesh(32), body_size, one));
      out.push_back(make_scenario("random-regular-64", random_regular(64, 8, seed), body_size, one));
      out.push_back(make_scenario("sparse-64", random_regular(64, 3, seed + 17), body_size, one));
      auto active = make_scenario("active-view-64", mesh(64), body_size, one);
      active.topology.active_view_size = 6;
      shuffle_peer_lists(active.topology, seed + 2027);
      out.push_back(std::move(active));
    }
  }
  return out;
}

std::vector<NodeId> honest_nodes(const Topology &topology) {
  std::vector<NodeId> nodes;
  for (NodeId i = 0; i < topology.node_count; i++) {
    if (!is_leech(topology, i)) {
      nodes.push_back(i);
    }
  }
  return nodes;
}

NodeId source_for(const Scenario &scenario, std::uint64_t seed, std::uint32_t round) {
  if (scenario.workload.source_policy == SourcePolicy::Fixed) {
    CHECK(!is_leech(scenario.topology, scenario.fixed_source));
    return scenario.fixed_source;
  }
  auto honest = honest_nodes(scenario.topology);
  CHECK(!honest.empty());
  return honest[(seed + round) % honest.size()];
}

std::uint32_t expected_receivers(const Topology &topology, NodeId source) {
  CHECK(source < topology.node_count);
  std::vector<bool> seen(topology.node_count, false);
  std::queue<NodeId> queue;
  seen[source] = true;
  queue.push(source);
  while (!queue.empty()) {
    auto node = queue.front();
    queue.pop();
    if (is_leech(topology, node)) {
      continue;
    }
    for (auto peer : topology.peers[node]) {
      CHECK(peer < topology.node_count);
      if (!seen[peer] && !is_leech(topology, peer)) {
        seen[peer] = true;
        queue.push(peer);
      }
    }
  }
  std::uint32_t count = 0;
  for (NodeId i = 0; i < topology.node_count; i++) {
    if (i != source && seen[i] && !is_leech(topology, i)) {
      count++;
    }
  }
  return count;
}

void add_finding(HealthReport &report, Severity severity, std::string code) {
  report.findings.push_back(HealthFinding{severity, std::move(code)});
}

HealthReport check_health(const RunRow &row, const HealthPolicy &policy) {
  HealthReport report;
  if (row.expected > 0 && row.total_x == 0.0) {
    add_finding(report, Severity::Fatal, "ZERO_SENDS");
  }
  if (!policy.allow_zero_reach && row.expected > 0 && row.delivered == 0) {
    add_finding(report, Severity::Fatal, "ZERO_REACH");
  }
  if (!policy.allow_zero_reach && row.reach_ratio < policy.min_reach_ratio) {
    add_finding(report, Severity::Warning, "LOW_REACH");
  }
  if (row.source_x > policy.max_source_x) {
    add_finding(report, Severity::Warning, "SOURCE_HOT");
  }
  if (row.total_x > policy.max_total_x) {
    add_finding(report, Severity::Warning, "HIGH_TOTAL");
  }
  if (row.duplicate_ratio > policy.max_duplicate_ratio) {
    add_finding(report, Severity::Warning, "DUPLICATES");
  }
  if (row.control_ratio > policy.max_control_ratio) {
    add_finding(report, Severity::Warning, "CONTROL");
  }
  if (row.p95_ms > policy.max_p95_latency_ms) {
    add_finding(report, Severity::Warning, "SLOW");
  }
  return report;
}

double ratio(std::uint64_t num, std::uint64_t den) {
  return den == 0 ? 0.0 : static_cast<double>(num) / static_cast<double>(den);
}

RunRow make_row(const Scenario &scenario, const AlgoEntry &entry, std::uint64_t seed, std::uint32_t round,
                const bsim::Metrics &metrics, NodeId source) {
  auto expected = expected_receivers(scenario.topology, source);
  auto body_size = std::max<std::uint64_t>(1, scenario.workload.body_size);
  auto node_count = std::max<std::uint32_t>(1, scenario.topology.node_count);
  RunRow row;
  row.scenario = scenario.name;
  row.algorithm = entry.label;
  row.seed = seed;
  row.round = round;
  row.source = source;
  row.delivered = metrics.delivered;
  row.expected = expected;
  row.reach_ratio = expected == 0 ? 1.0 : static_cast<double>(metrics.delivered) / expected;
  row.total_x = static_cast<double>(metrics.total_bytes()) / body_size / node_count;
  row.source_x = static_cast<double>(metrics.per_node[source].body_bytes_out) / body_size;
  row.p95_out_amp = static_cast<double>(metrics.percentile_body_bytes_out(0.95)) / body_size;
  // Honest-only network costs (mirrors mainnet's run() so the CSV schema is comparable).
  std::uint64_t honest_body_out = 0;
  std::uint64_t honest_total_out = 0;
  std::uint32_t honest_n = 0;
  std::vector<std::uint64_t> in_bytes;
  in_bytes.reserve(metrics.per_node.size());
  for (NodeId i = 0; i < scenario.topology.node_count; i++) {
    in_bytes.push_back(metrics.per_node[i].body_bytes_in);
    if (!is_leech(scenario.topology, i)) {
      honest_body_out += metrics.per_node[i].body_bytes_out;
      honest_total_out += metrics.per_node[i].body_bytes_out + metrics.per_node[i].control_bytes_out;
      honest_n++;
    }
  }
  auto honest_div = std::max<std::uint32_t>(1, honest_n);
  row.honest_x = static_cast<double>(honest_body_out) / honest_div / body_size;
  row.honest_total_x = static_cast<double>(honest_total_out) / honest_div / body_size;
  std::sort(in_bytes.begin(), in_bytes.end());
  auto p95_in = in_bytes.empty() ? 0 : in_bytes[static_cast<size_t>(0.95 * (in_bytes.size() - 1))];
  row.p95_in_amp = static_cast<double>(p95_in) / body_size;
  if (expected == 0) {
    row.p50_ms = 0.0;
    row.p95_ms = 0.0;
    row.p99_ms = 0.0;
  } else {
    row.p50_ms =
        metrics.delivered == 0 ? std::numeric_limits<double>::infinity() : metrics.percentile_delivery(0.50) * 1000.0;
    row.p95_ms = metrics.delivered < expected ? std::numeric_limits<double>::infinity()
                                              : metrics.percentile_delivery(0.95) * 1000.0;
    row.p99_ms = metrics.delivered < expected ? std::numeric_limits<double>::infinity()
                                              : metrics.percentile_delivery(0.99) * 1000.0;
  }
  row.duplicate_ratio = ratio(metrics.duplicate_messages, metrics.body_messages);
  row.control_ratio = ratio(metrics.control_bytes, metrics.total_bytes());
  row.health = check_health(row, scenario.health);
  return row;
}

Spec make_spec(const Scenario &scenario, const AlgoEntry &entry, std::uint64_t seed, std::uint32_t round,
               NodeId source) {
  algo::LeechConfig leech_config;
  return Spec{
      .body_id = static_cast<BodyId>((seed + 1) * 1000003 + round + 1),
      .source = source,
      .body_size = scenario.workload.body_size,
      .piece_count = entry.piece_count,
      .piece_header = 64,
      .control_bytes = 64,
      .factory = {.honest = entry.family_factory ? entry.family_factory : bsim::simple_family(entry.label, entry.make),
                  .leech = [leech_config] { return ton::overlay::broadcast::make_leech_family(leech_config); }}};
}

std::vector<RunRow> run_algorithm(const Scenario &scenario, const AlgoEntry &entry, std::uint64_t seed) {
  std::vector<RunRow> rows;
  bsim::SessionState session;
  session.round_interval = scenario.workload.round_interval;
  auto *session_ptr = scenario.workload.state_policy == StatePolicy::PersistentScores ? &session : nullptr;
  for (std::uint32_t round = 0; round < scenario.workload.rounds; round++) {
    auto source = source_for(scenario, seed, round);
    auto metrics = bsim::simulate(scenario.topology, make_spec(scenario, entry, seed, round, source),
                                  scenario.workload.max_sim_time, session_ptr);
    rows.push_back(make_row(scenario, entry, seed, round, metrics, source));
  }
  return rows;
}

std::vector<std::string> split_csv(const std::string &value) {
  std::vector<std::string> out;
  std::size_t pos = 0;
  while (pos <= value.size()) {
    auto comma = value.find(',', pos);
    auto part = value.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
    if (!part.empty()) {
      out.push_back(part);
    }
    if (comma == std::string::npos) {
      break;
    }
    pos = comma + 1;
  }
  return out;
}

td::Result<std::vector<AlgoEntry>> select_algorithms(std::uint64_t body_size, const std::vector<std::string> &filter) {
  auto catalogue = standard_catalogue(body_size);
  if (filter.empty()) {
    return catalogue;
  }
  std::vector<AlgoEntry> selected;
  std::set<std::string> used;
  for (const auto &name : filter) {
    auto it = std::find_if(catalogue.begin(), catalogue.end(), [&](const auto &entry) { return entry.label == name; });
    if (it == catalogue.end()) {
      return td::Status::Error("unknown algorithm in --algorithms");
    }
    if (used.insert(name).second) {
      selected.push_back(*it);
    }
  }
  return selected;
}

td::Result<double> parse_double(td::Slice arg) {
  auto str = arg.str();
  char *end = nullptr;
  auto value = std::strtod(str.c_str(), &end);
  if (end == str.c_str() || *end != '\0' || !std::isfinite(value)) {
    return td::Status::Error("expected finite floating-point value");
  }
  return value;
}

td::Status parse_cli(int argc, char *argv[], CliConfig &cfg) {
  td::OptionParser parser;
  parser.set_description("run synthetic broadcast algorithm benchmark scenarios");
  parser.add_option('h', "help", "print help", [&]() {
    char buf[8192];
    td::StringBuilder sb({buf, sizeof(buf) - 1});
    sb << parser;
    std::cout << sb.as_cslice().c_str();
    std::exit(2);
  });
  parser.add_option('\0', "list-algorithms", "print algorithm catalogue and exit",
                    [&]() { cfg.list_algorithms = true; });
  parser.add_option('\0', "algo-details", "print per-scenario tables under each algorithm",
                    [&]() { cfg.algo_details = true; });
  parser.add_option('\0', "details", "print raw per-run rows in addition to summaries", [&]() { cfg.details = true; });
  parser.add_checked_option('\0', "suite", "quick|smoke|default", [&](td::Slice arg) {
    cfg.suite = arg.str();
    if (cfg.suite != "quick" && cfg.suite != "smoke" && cfg.suite != "default") {
      return td::Status::Error("--suite must be quick, smoke, or default");
    }
    return td::Status::OK();
  });
  // --graph: dispatched on at the top of main(); accept it here too so the synthetic parser
  // doesn't complain about an "unrecognised" option.
  parser.add_checked_option('\0', "graph", "synthetic|mainnet (default: synthetic)", [&](td::Slice arg) {
    auto value = arg.str();
    if (value != "synthetic" && value != "mainnet") {
      return td::Status::Error("--graph must be synthetic or mainnet");
    }
    return td::Status::OK();
  });
  parser.add_checked_option('\0', "csv", "emit v2-schema CSV (one row per algo×scenario)", [&](td::Slice arg) {
    cfg.csv_path = arg.str();
    return td::Status::OK();
  });
  parser.add_checked_option('\0', "algorithms", "comma-separated exact algorithm names", [&](td::Slice arg) {
    cfg.algorithm_filter = split_csv(arg.str());
    return td::Status::OK();
  });
  parser.add_checked_option('\0', "view", "algo|scenario|both", [&](td::Slice arg) {
    auto view = arg.str();
    if (view == "algo") {
      cfg.view = View::Algorithm;
    } else if (view == "scenario") {
      cfg.view = View::Scenario;
    } else if (view == "both") {
      cfg.view = View::Both;
    } else {
      return td::Status::Error("--view must be algo, scenario, or both");
    }
    return td::Status::OK();
  });
  parser.add_checked_option('\0', "only", "run scenarios whose name contains this substring", [&](td::Slice arg) {
    cfg.only = arg.str();
    return td::Status::OK();
  });
  parser.add_checked_option('\0', "seed", "first seed", [&](td::Slice arg) {
    TRY_RESULT_ASSIGN(cfg.seed, td::to_integer_safe<std::uint64_t>(arg));
    return td::Status::OK();
  });
  parser.add_checked_option('\0', "seeds", "number of consecutive seeds", [&](td::Slice arg) {
    TRY_RESULT_ASSIGN(cfg.seeds, td::to_integer_safe<std::uint32_t>(arg));
    if (cfg.seeds == 0) {
      return td::Status::Error("--seeds must be positive");
    }
    return td::Status::OK();
  });
  parser.add_checked_option('\0', "rounds", "override rounds per scenario", [&](td::Slice arg) {
    TRY_RESULT_ASSIGN(cfg.rounds, td::to_integer_safe<std::uint32_t>(arg));
    if (cfg.rounds == 0) {
      return td::Status::Error("--rounds must be positive");
    }
    return td::Status::OK();
  });
  parser.add_checked_option('\0', "body-size", "body size in bytes; repeatable", [&](td::Slice arg) {
    TRY_RESULT(value, td::to_integer_safe<std::uint64_t>(arg));
    if (value == 0) {
      return td::Status::Error("--body-size must be positive");
    }
    if (!cfg.body_size_override) {
      cfg.body_sizes.clear();
      cfg.body_size_override = true;
    }
    cfg.body_sizes.push_back(value);
    return td::Status::OK();
  });
  parser.add_checked_option('\0', "max-sim-time", "maximum simulated seconds per broadcast", [&](td::Slice arg) {
    TRY_RESULT_ASSIGN(cfg.max_sim_time, parse_double(arg));
    if (cfg.max_sim_time <= 0.0) {
      return td::Status::Error("--max-sim-time must be positive");
    }
    return td::Status::OK();
  });
  TRY_STATUS(parser.run(argc, argv, 0).move_as_status());
  return td::Status::OK();
}

std::string fmt(double value, int precision, const char *suffix = "") {
  if (!std::isfinite(value)) {
    return "inf";
  }
  std::ostringstream out;
  out << std::fixed << std::setprecision(precision) << value << suffix;
  return out.str();
}

void print_catalogue(std::uint64_t body_size) {
  auto catalogue = standard_catalogue(body_size);
  std::printf("algorithms body=%s (%zu):\n", size_name(body_size).c_str(), catalogue.size());
  for (const auto &entry : catalogue) {
    std::printf("  %-24s pieces=%u\n", entry.label.c_str(), entry.piece_count);
  }
}

void print_run_rows(const std::vector<RunRow> &rows) {
  std::printf("runs=%zu\n", rows.size());
  std::printf("%-22s %-18s %5s %5s %5s %9s %8s %8s %8s %8s %8s %8s %s\n", "scenario", "algorithm", "seed", "round",
              "src", "reach", "total", "source", "p95out", "p50", "p95", "dup", "health");
  for (const auto &row : rows) {
    auto reach =
        std::to_string(row.delivered) + "/" + std::to_string(row.expected) + " " + fmt(100.0 * row.reach_ratio, 1, "%");
    std::printf("%-22s %-18s %5llu %5u %5u %9s %8s %8s %8s %8s %8s %8s %s\n", row.scenario.c_str(),
                row.algorithm.c_str(), static_cast<unsigned long long>(row.seed), row.round, row.source, reach.c_str(),
                fmt(row.total_x, 1, "x").c_str(), fmt(row.source_x, 1, "x").c_str(),
                fmt(row.p95_out_amp, 1, "x").c_str(), fmt(row.p50_ms, 0, "ms").c_str(),
                fmt(row.p95_ms, 0, "ms").c_str(), fmt(100.0 * row.duplicate_ratio, 1, "%").c_str(),
                row.health.label().c_str());
  }
}

std::vector<SummaryRow> summarize_algorithms(const std::vector<RunRow> &rows, const std::vector<AlgoEntry> &catalogue) {
  std::vector<SummaryRow> out;
  for (const auto &entry : catalogue) {
    SummaryRow summary;
    summary.label = entry.label;
    for (const auto &row : rows) {
      if (row.algorithm == entry.label) {
        summary.add(row);
      }
    }
    if (summary.runs != 0) {
      out.push_back(summary);
    }
  }
  return out;
}

std::vector<SummaryRow> summarize_scenarios(const std::vector<RunRow> &rows,
                                            const std::vector<std::string> &scenarios) {
  std::vector<SummaryRow> out;
  for (const auto &scenario : scenarios) {
    SummaryRow summary;
    summary.label = scenario;
    for (const auto &row : rows) {
      if (row.scenario == scenario) {
        summary.add(row);
      }
    }
    if (summary.runs != 0) {
      out.push_back(summary);
    }
  }
  return out;
}

void print_summary(const char *title, const char *label_header, const std::vector<SummaryRow> &rows) {
  std::printf("\n%s:\n", title);
  std::printf("%-24s %5s %5s %5s %5s %9s %9s %8s %8s %8s %8s %s\n", label_header, "runs", "meas", "fatal", "warn",
              "reach_min", "reach_avg", "total", "p95out", "p95ms", "dup", "health");
  for (const auto &row : rows) {
    auto reach_min = row.measured == 0 ? std::string("n/a") : fmt(100.0 * row.reach_min, 1, "%");
    auto reach_avg = row.measured == 0 ? std::string("n/a") : fmt(100.0 * row.reach_avg(), 1, "%");
    auto total = row.measured == 0 ? std::string("n/a") : fmt(row.total_x_avg(), 1, "x");
    auto p95_out = row.measured == 0 ? std::string("n/a") : fmt(row.p95_out_avg(), 1, "x");
    auto p95 = row.measured == 0 ? std::string("n/a") : fmt(row.p95_ms_avg(), 0, "ms");
    auto duplicate = row.measured == 0 ? std::string("n/a") : fmt(100.0 * row.duplicate_avg(), 1, "%");
    std::printf("%-24s %5u %5u %5u %5u %9s %9s %8s %8s %8s %8s %s\n", row.label.c_str(), row.runs, row.measured,
                row.fatal, row.warning, reach_min.c_str(), reach_avg.c_str(), total.c_str(), p95_out.c_str(),
                p95.c_str(), duplicate.c_str(), row.health_label().c_str());
  }
}

const SummaryRow *best_by(const std::vector<SummaryRow> &rows, bool healthy_only,
                          const std::function<double(const SummaryRow &)> &score, bool higher_is_better) {
  const SummaryRow *best = nullptr;
  for (const auto &row : rows) {
    if (row.measured == 0) {
      continue;
    }
    if (healthy_only && (row.fatal != 0 || row.reach_min < 0.99)) {
      continue;
    }
    auto value = score(row);
    if (!std::isfinite(value)) {
      continue;
    }
    if (best == nullptr) {
      best = &row;
      continue;
    }
    auto rhs = score(*best);
    if ((higher_is_better && value > rhs) || (!higher_is_better && value < rhs)) {
      best = &row;
    }
  }
  return best;
}

void print_top_metric(const char *name, const SummaryRow *row, double value, int precision, const char *suffix) {
  if (row == nullptr) {
    return;
  }
  std::printf("  %-9s %-24s %s\n", name, row->label.c_str(), fmt(value, precision, suffix).c_str());
}

void print_top(const std::vector<SummaryRow> &rows) {
  bool has_healthy = false;
  for (const auto &row : rows) {
    has_healthy = has_healthy || (row.fatal == 0 && row.reach_min >= 0.99);
  }
  std::printf("\ntop%s:\n", has_healthy ? "" : " (unhealthy)");
  auto reach = best_by(rows, false, [](const auto &row) { return row.reach_min; }, true);
  auto total = best_by(rows, has_healthy, [](const auto &row) { return row.total_x_avg(); }, false);
  auto p95_out = best_by(rows, has_healthy, [](const auto &row) { return row.p95_out_avg(); }, false);
  auto latency = best_by(rows, has_healthy, [](const auto &row) { return row.p95_ms_avg(); }, false);
  auto duplicate = best_by(rows, has_healthy, [](const auto &row) { return row.duplicate_avg(); }, false);
  print_top_metric("reach", reach, reach == nullptr ? 0.0 : 100.0 * reach->reach_min, 1, "%");
  print_top_metric("total", total, total == nullptr ? 0.0 : total->total_x_avg(), 1, "x");
  print_top_metric("p95_out", p95_out, p95_out == nullptr ? 0.0 : p95_out->p95_out_avg(), 1, "x");
  print_top_metric("latency", latency, latency == nullptr ? 0.0 : latency->p95_ms_avg(), 0, "ms");
  print_top_metric("duplicate", duplicate, duplicate == nullptr ? 0.0 : 100.0 * duplicate->duplicate_avg(), 1, "%");
}

std::vector<RunRow> rows_for_algorithm(const std::vector<RunRow> &all_rows, const std::string &algorithm);

std::vector<RunRow> rows_for_scenario(const std::vector<RunRow> &all_rows, const std::string &scenario);

bool viable(const SummaryRow &row) {
  return row.measured != 0 && row.fatal == 0 && row.reach_min >= 0.99;
}

const char *quick_status(const SummaryRow *row) {
  if (row == nullptr || !viable(*row)) {
    return "fail";
  }
  return row->warning == 0 ? "ok" : "warn";
}

const SummaryRow *find_summary(const std::vector<SummaryRow> &rows, const std::string &label) {
  auto it = std::find_if(rows.begin(), rows.end(), [&](const auto &row) { return row.label == label; });
  return it == rows.end() ? nullptr : &*it;
}

std::string short_scenario_name(const std::string &scenario) {
  auto slash = scenario.find('/');
  return slash == std::string::npos ? scenario : scenario.substr(0, slash);
}

std::string join_strings(const std::vector<std::string> &items, const char *separator) {
  std::string out;
  for (const auto &item : items) {
    if (!out.empty()) {
      out += separator;
    }
    out += item;
  }
  return out;
}

std::string quick_metric_cell(const SummaryRow *row) {
  if (row == nullptr || row->measured == 0) {
    return "n/a";
  }
  return fmt(100.0 * row->reach_min, 1, "%") + " " + fmt(row->total_x_avg(), 1, "x") + " " +
         fmt(row->p95_out_avg(), 1, "x") + " " + fmt(row->p95_ms_avg(), 0, "ms");
}

struct ScenarioTop {
  std::string scenario;
  std::string total;
  std::string p95_out;
  std::string latency;
};

std::vector<ScenarioTop> scenario_tops(const std::vector<RunRow> &all_rows, const std::vector<AlgoEntry> &catalogue,
                                       const std::vector<std::string> &scenario_order) {
  std::vector<ScenarioTop> out;
  for (const auto &scenario : scenario_order) {
    auto summaries = summarize_algorithms(rows_for_scenario(all_rows, scenario), catalogue);
    auto total = best_by(summaries, true, [](const auto &row) { return row.total_x_avg(); }, false);
    auto p95_out = best_by(summaries, true, [](const auto &row) { return row.p95_out_avg(); }, false);
    auto latency = best_by(summaries, true, [](const auto &row) { return row.p95_ms_avg(); }, false);
    out.push_back(ScenarioTop{.scenario = scenario,
                              .total = total == nullptr ? std::string() : total->label,
                              .p95_out = p95_out == nullptr ? std::string() : p95_out->label,
                              .latency = latency == nullptr ? std::string() : latency->label});
  }
  return out;
}

std::string strength_note(const std::string &algorithm, const std::vector<ScenarioTop> &tops) {
  std::vector<std::string> parts;
  for (const auto &top : tops) {
    std::vector<std::string> metrics;
    if (top.total == algorithm) {
      metrics.push_back("total");
    }
    if (top.p95_out == algorithm) {
      metrics.push_back("p95out");
    }
    if (top.latency == algorithm) {
      metrics.push_back("latency");
    }
    if (!metrics.empty()) {
      parts.push_back(short_scenario_name(top.scenario) + ":" + join_strings(metrics, "/"));
    }
  }
  return join_strings(parts, ", ");
}

void print_quick_matrix(const std::vector<RunRow> &all_rows, const std::vector<AlgoEntry> &catalogue,
                        const std::vector<std::string> &scenario_order) {
  auto suite = summarize_algorithms(all_rows, catalogue);
  auto tops = scenario_tops(all_rows, catalogue, scenario_order);

  std::printf("\nquick matrix (cell: reach total/body/node p95_out/body p95_ms; status: ok/warn/fail)\n");
  std::printf("%-24s %-7s", "algorithm", "status");
  for (const auto &scenario : scenario_order) {
    std::printf(" %-33s", short_scenario_name(scenario).c_str());
  }
  std::printf(" %s\n", "strong / health");
  std::printf("%-24s %-7s", "", "");
  for (std::size_t i = 0; i < scenario_order.size(); i++) {
    std::printf(" %-33s", "reach total p95out p95ms");
  }
  std::printf(" %s\n", "");

  for (const auto &entry : catalogue) {
    auto rows = rows_for_algorithm(all_rows, entry.label);
    auto scenarios = summarize_scenarios(rows, scenario_order);
    auto *summary = find_summary(suite, entry.label);
    bool can_compare = summary != nullptr && viable(*summary);
    std::printf("%-24s %-7s", entry.label.c_str(), quick_status(summary));
    for (const auto &scenario : scenario_order) {
      std::printf(" %-33s", quick_metric_cell(find_summary(scenarios, scenario)).c_str());
    }
    auto note = can_compare ? strength_note(entry.label, tops) : std::string();
    if (summary != nullptr && summary->health_label() != "ok") {
      if (!note.empty()) {
        note += "; ";
      }
      note += summary->health_label();
    }
    if (note.empty()) {
      note = "-";
    }
    std::printf(" %s\n", note.c_str());
  }

  std::printf("\nquick winners:\n");
  for (const auto &top : tops) {
    auto summaries = summarize_algorithms(rows_for_scenario(all_rows, top.scenario), catalogue);
    auto total = find_summary(summaries, top.total);
    auto p95_out = find_summary(summaries, top.p95_out);
    auto latency = find_summary(summaries, top.latency);
    std::printf("  %-8s total %-24s %7s  p95out %-24s %7s  latency %-24s %7s\n",
                short_scenario_name(top.scenario).c_str(), top.total.empty() ? "-" : top.total.c_str(),
                total == nullptr ? "" : fmt(total->total_x_avg(), 1, "x").c_str(),
                top.p95_out.empty() ? "-" : top.p95_out.c_str(),
                p95_out == nullptr ? "" : fmt(p95_out->p95_out_avg(), 1, "x").c_str(),
                top.latency.empty() ? "-" : top.latency.c_str(),
                latency == nullptr ? "" : fmt(latency->p95_ms_avg(), 0, "ms").c_str());
  }
}

std::vector<RunRow> rows_for_algorithm(const std::vector<RunRow> &all_rows, const std::string &algorithm) {
  std::vector<RunRow> rows;
  for (const auto &row : all_rows) {
    if (row.algorithm == algorithm) {
      rows.push_back(row);
    }
  }
  return rows;
}

std::vector<RunRow> rows_for_scenario(const std::vector<RunRow> &all_rows, const std::string &scenario) {
  std::vector<RunRow> rows;
  for (const auto &row : all_rows) {
    if (row.scenario == scenario) {
      rows.push_back(row);
    }
  }
  return rows;
}

void print_scenario_group(const std::string &scenario, const std::vector<RunRow> &rows,
                          const std::vector<AlgoEntry> &catalogue, bool details) {
  std::printf("\n== %s ==\n", scenario.c_str());
  auto summaries = summarize_algorithms(rows, catalogue);
  print_summary("algorithms", "algorithm", summaries);
  print_top(summaries);
  if (details) {
    print_run_rows(rows);
  }
}

void print_scenario_view(const std::vector<RunRow> &all_rows, const std::vector<AlgoEntry> &catalogue,
                         const std::vector<std::string> &scenario_order, bool details) {
  for (const auto &scenario : scenario_order) {
    print_scenario_group(scenario, rows_for_scenario(all_rows, scenario), catalogue, details);
  }
}

void print_algorithm_view(const std::vector<RunRow> &all_rows, const std::vector<AlgoEntry> &catalogue,
                          const std::vector<std::string> &scenario_order, bool show_scenarios, bool details) {
  auto suite = summarize_algorithms(all_rows, catalogue);
  print_summary("suite summary", "algorithm", suite);
  print_top(suite);
  if (!show_scenarios) {
    return;
  }

  for (const auto &entry : catalogue) {
    auto rows = rows_for_algorithm(all_rows, entry.label);
    if (rows.empty()) {
      continue;
    }
    std::printf("\n== algorithm %s ==\n", entry.label.c_str());
    auto scenarios = summarize_scenarios(rows, scenario_order);
    print_summary("scenarios", "scenario", scenarios);
    print_top(scenarios);
    if (details) {
      print_run_rows(rows);
    }
  }
}

bool keep_scenario(const CliConfig &cfg, const Scenario &scenario) {
  return cfg.only.empty() || scenario.name.find(cfg.only) != std::string::npos;
}

// Parse "leech30" / "/leech30" / "mesh-8/medium leech=30%" into 0.30. Otherwise 0.0 (clean).
double scenario_leech_fraction(const std::string &scenario_name) {
  auto pos = scenario_name.find("leech");
  if (pos == std::string::npos) {
    return 0.0;
  }
  pos += 5;
  if (pos < scenario_name.size() && scenario_name[pos] == '=') {
    pos++;
  }
  if (pos >= scenario_name.size() || !std::isdigit(static_cast<unsigned char>(scenario_name[pos]))) {
    return 0.0;
  }
  return std::strtod(scenario_name.c_str() + pos, nullptr) / 100.0;
}

void write_synthetic_csv(
    const std::string &path,
    const std::vector<std::tuple<std::uint64_t, std::vector<std::string>, std::vector<RunRow>>> &body_groups) {
  std::ofstream out(path);
  if (!out) {
    std::cerr << "failed to open --csv path: " << path << "\n";
    return;
  }
  // Schema matches the mainnet path (bench-mainnet.cpp emit_csv_header) so render_bench.py can
  // consume either source uniformly.
  out << "algorithm,size,graph,scenario,leech_pct,round,"
         "reach,p50_ms,p95_ms,p99_ms,"
         "node_out_p95,node_in_p95,"
         "network_overhead_honest,network_overhead_honest_total,source_x,"
         "dup_pct,control_pct\n";
  for (const auto &[body_size, scenario_order, rows] : body_groups) {
    // Group by (algorithm, scenario) and accumulate means. We don't reuse SummaryRow because it
    // doesn't track the four extra fields we added for parity with mainnet's AggregateResult.
    struct Agg {
      double reach = 0, total = 0, source = 0, p95_out = 0, p95_in = 0, honest = 0, honest_total = 0;
      double p50 = 0, p95 = 0, p99 = 0, dup = 0, ctrl = 0;
      bool inf50 = false, inf95 = false, inf99 = false;
      std::uint32_t n = 0;
      void add(const RunRow &r) {
        reach += r.reach_ratio;
        total += r.total_x;
        source += r.source_x;
        p95_out += r.p95_out_amp;
        p95_in += r.p95_in_amp;
        honest += r.honest_x;
        honest_total += r.honest_total_x;
        dup += r.duplicate_ratio;
        ctrl += r.control_ratio;
        if (std::isfinite(r.p50_ms))
          p50 += r.p50_ms;
        else
          inf50 = true;
        if (std::isfinite(r.p95_ms))
          p95 += r.p95_ms;
        else
          inf95 = true;
        if (std::isfinite(r.p99_ms))
          p99 += r.p99_ms;
        else
          inf99 = true;
        n++;
      }
    };
    std::map<std::pair<std::string, std::string>, Agg> aggs;
    for (const auto &r : rows) {
      aggs[{r.algorithm, r.scenario}].add(r);
    }
    for (const auto &scenario : scenario_order) {
      // Synthetic scenarios are named "<topology>/<size>" (e.g. "mesh-8/medium"). The size is
      // already in the `size` column, so strip the suffix to keep the scenario token comparable
      // to mainnet's ("clean", "leech30", ...).
      std::string scenario_short = scenario;
      auto slash = scenario_short.rfind('/');
      if (slash != std::string::npos) {
        scenario_short.resize(slash);
      }
      for (const auto &kv : aggs) {
        if (kv.first.second != scenario)
          continue;
        const auto &a = kv.second;
        if (a.n == 0)
          continue;
        double inv = 1.0 / a.n;
        double leech = scenario_leech_fraction(scenario_short);
        out << kv.first.first << ',' << size_class(body_size) << ',' << "synthetic" << ',' << scenario_short << ','
            << static_cast<int>(std::round(leech * 100)) << ',' << 0 << ',' << a.reach * inv << ','
            << (a.inf50 ? 0.0 : a.p50 * inv) << ',' << (a.inf95 ? 0.0 : a.p95 * inv) << ','
            << (a.inf99 ? 0.0 : a.p99 * inv) << ',' << a.p95_out * inv << ',' << a.p95_in * inv << ',' << a.honest * inv
            << ',' << a.honest_total * inv << ',' << a.source * inv << ',' << a.dup * inv << ',' << a.ctrl * inv
            << '\n';
      }
    }
  }
  std::cerr << "csv=" << path << "\n";
}

bool has_fatal(const std::vector<RunRow> &rows) {
  for (const auto &row : rows) {
    if (row.health.has_fatal()) {
      return true;
    }
  }
  return false;
}

}  // namespace

}  // namespace ton::bsim_runner

namespace ton::bsim_runner {
int mainnet_main(int argc, char *argv[]);
}

int main(int argc, char *argv[]) {
  // Peek for `--graph mainnet` / `--graph=mainnet` before parsing — the mainnet path has its own
  // CLI surface, so we hand off the full argv to it. Default is synthetic mode.
  auto graph_value = [&](int i) -> std::string {
    std::string arg = argv[i];
    if (arg.rfind("--graph=", 0) == 0) {
      return arg.substr(std::strlen("--graph="));
    }
    if (arg == "--graph" && i + 1 < argc) {
      return argv[i + 1];
    }
    return {};
  };
  for (int i = 1; i < argc; i++) {
    auto value = graph_value(i);
    if (value.empty()) {
      continue;
    }
    if (value == "mainnet") {
      return ton::bsim_runner::mainnet_main(argc, argv);
    }
    if (value != "synthetic") {
      std::cerr << "--graph must be synthetic or mainnet\n";
      return 2;
    }
  }

  using namespace ton::bsim_runner;
  SET_VERBOSITY_LEVEL(verbosity_WARNING);

  CliConfig cfg;
  auto status = parse_cli(argc, argv, cfg);
  if (status.is_error()) {
    std::cerr << status.message().str() << "\n";
    return 2;
  }
  if (cfg.list_algorithms) {
    for (auto body_size : default_body_sizes(cfg)) {
      print_catalogue(body_size);
    }
    return 0;
  }

  auto body_sizes = default_body_sizes(cfg);
  bool ok = true;
  std::vector<std::tuple<std::uint64_t, std::vector<std::string>, std::vector<RunRow>>> csv_groups;
  for (auto body_size : body_sizes) {
    auto catalogue_result = select_algorithms(body_size, cfg.algorithm_filter);
    if (catalogue_result.is_error()) {
      std::cerr << catalogue_result.move_as_error().message().str() << "\n";
      return 2;
    }
    auto catalogue = catalogue_result.move_as_ok();

    std::vector<std::string> scenario_order;
    std::vector<RunRow> all_rows;
    for (std::uint32_t seed_offset = 0; seed_offset < cfg.seeds; seed_offset++) {
      auto seed = cfg.seed + seed_offset;
      for (const auto &scenario : make_scenarios(cfg, seed)) {
        if (scenario.workload.body_size != body_size || !keep_scenario(cfg, scenario)) {
          continue;
        }
        if (std::find(scenario_order.begin(), scenario_order.end(), scenario.name) == scenario_order.end()) {
          scenario_order.push_back(scenario.name);
        }
        for (const auto &entry : catalogue) {
          auto rows = run_algorithm(scenario, entry, seed);
          all_rows.insert(all_rows.end(), rows.begin(), rows.end());
        }
      }
    }
    std::printf("suite=%s body=%s algorithms=%zu scenarios=%zu seeds=%u\n", cfg.suite.c_str(),
                size_name(body_size).c_str(), catalogue.size(), scenario_order.size(), cfg.seeds);
    if (cfg.suite == "quick" && cfg.view == View::Algorithm && !cfg.details && !cfg.algo_details) {
      print_quick_matrix(all_rows, catalogue, scenario_order);
    } else if (cfg.view == View::Algorithm || cfg.view == View::Both) {
      auto show_scenarios = cfg.algo_details || cfg.details || !cfg.algorithm_filter.empty() || catalogue.size() <= 6;
      print_algorithm_view(all_rows, catalogue, scenario_order, show_scenarios, cfg.details);
    }
    if (cfg.view == View::Scenario || cfg.view == View::Both) {
      print_scenario_view(all_rows, catalogue, scenario_order, cfg.details);
    }
    ok = ok && !has_fatal(all_rows);
    if (!cfg.csv_path.empty()) {
      csv_groups.emplace_back(body_size, std::move(scenario_order), std::move(all_rows));
    }
  }
  if (!cfg.csv_path.empty()) {
    write_synthetic_csv(cfg.csv_path, csv_groups);
  }
  return ok ? 0 : 1;
}
