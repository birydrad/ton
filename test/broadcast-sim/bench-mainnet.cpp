// Run the bsim harness against the mainnet overlay topology. Loads the crawler dump + geo
// matrix, picks N random sources, runs the catalogue against several leech regimes and body
// sizes, prints the resulting comparison tables.
//
// Inputs:
//   * /tmp/overlay-graph-mainnet.json — crawler dump (node hashes + neighbour lists +
//     unresponsive flag).
//   * /tmp/latency_matrix.json        — geolocated nodes (hash → lat/lon/country/as).

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <unordered_set>
#include <vector>

#include "overlay/broadcast/algorithms/leech.h"
#include "td/utils/OptionParser.h"
#include "td/utils/Status.h"
#include "td/utils/check.h"
#include "td/utils/logging.h"
#include "td/utils/misc.h"

#include "bsim.h"
#include "catalogue.h"
#include "mainnet-loader.h"

namespace ton::bsim_runner {

using bsim::BodyId;
using bsim::NodeId;
using bsim::Spec;

// Running mean across samples. add() per sample, mean() at the end. Replaces 15 hand-rolled
// _mean/_min/_max fields with a uniform helper — adding a metric is one line.
struct Stat {
  double sum = 0.0;
  std::uint32_t n = 0;
  void add(double v) {
    sum += v;
    n++;
  }
  double mean() const {
    return n == 0 ? 0.0 : sum / n;
  }
};

// Headline metrics, all averaged over the source set:
//   * reach %             : fraction of honest receivers (excluding source) that delivered.
//   * total/body/node     : average per-node duplication. 1.0 = ideal (each node downloads body
//                           once); plumtree pull achieves ~1.0, naive flood achieves ~mean-degree.
//   * p95_out/body        : 95th-percentile per-node upload (peak source-load metric, robust to
//                           the single high-degree hub that dominates `max`).
//   * p50_ms / p95_ms     : delivery-time percentiles.
struct AggregateResult {
  std::string label;
  // honest_x: average per-honest-node body bytes sent, normalised by body size. Excludes leeches
  // from both numerator and denominator, so this is "what each honest validator actually paid in
  // upload" — leech-induced traffic shows up here as the cost honest peers absorbed serving them.
  Stat reach, total_x, honest_x, honest_total_x, p95_out_x, p95_in_x, p50_ms, p90_ms, p95_ms, p99_ms;
  Stat source_x, dup_pct, control_pct;
};

// All inputs to one algorithm × source-set evaluation. Grouped so adding a new knob (deadline,
// p99, etc.) doesn't change the run() signature.
struct RunRequest {
  const Loaded &loaded;
  const AlgoEntry &entry;
  std::uint64_t body_size = 0;
  const std::vector<NodeId> &sources;
  double max_sim_time = 60.0;
  algo::LeechConfig leech_cfg;
  bsim::SessionState *session = nullptr;
};

bool is_leech(const Loaded &loaded, NodeId node) {
  return node < loaded.topology.leech_nodes.size() && loaded.topology.leech_nodes[node];
}

std::uint32_t honest_receiver_count(const Loaded &loaded, NodeId source) {
  CHECK(source < loaded.topology.node_count);
  CHECK(!is_leech(loaded, source));
  std::uint32_t n = 0;
  for (NodeId i = 0; i < loaded.topology.node_count; i++) {
    if (!is_leech(loaded, i)) {
      n++;
    }
  }
  return n == 0 ? 0 : n - 1;  // exclude source
}

// Honest family from a catalogue entry: stateful entries supply `family_factory` directly;
// stateless ones (most of the catalogue) wrap their plain `make` via simple_family.
bsim::AlgorithmFactory::FamilyFactory entry_family_factory(const AlgoEntry &entry) {
  if (entry.family_factory) {
    return entry.family_factory;
  }
  return bsim::simple_family(entry.label, entry.make);
}

bsim::AlgorithmFactory::FamilyFactory leech_family_factory(algo::LeechConfig cfg) {
  return [cfg] { return ton::overlay::broadcast::make_leech_family(cfg); };
}

AggregateResult run(const RunRequest &req) {
  AggregateResult r{.label = req.entry.label};
  double denom = req.body_size == 0 ? 1.0 : static_cast<double>(req.body_size);
  double per_node = std::max<std::uint32_t>(1, req.loaded.topology.node_count);
  std::uint32_t honest_n = 0;
  for (NodeId i = 0; i < req.loaded.topology.node_count; i++) {
    honest_n += is_leech(req.loaded, i) ? 0 : 1;
  }
  double per_honest = std::max<std::uint32_t>(1, honest_n);
  // Production broadcast-twostep.cpp: k = (2N - 2) / 3 where N = persistent peer count (=honest).
  std::uint32_t piece_count = req.entry.piece_count;
  if (req.entry.dynamic_piece_count_twostep && honest_n >= 4) {
    piece_count = std::max<std::uint32_t>(2, (2 * honest_n - 2) / 3);
  }
  auto leech_cfg = req.leech_cfg;
  auto algo_start = std::chrono::steady_clock::now();
  std::fprintf(stderr, "[bench] start algo=%s sources=%zu body=%llu honest=%u\n", req.entry.label.c_str(),
               req.sources.size(), static_cast<unsigned long long>(req.body_size), honest_n);
  std::fflush(stderr);
  std::size_t src_idx = 0;
  for (auto src : req.sources) {
    auto src_start = std::chrono::steady_clock::now();
    Spec spec{.body_id = static_cast<BodyId>(src) + 1,
              .source = src,
              .body_size = req.body_size,
              .piece_count = piece_count,
              .all_peers_persistent = req.entry.require_persistent_peers,
              .all_peers_neighbours = req.entry.require_all_peers_neighbours,
              .factory = {.honest = entry_family_factory(req.entry), .leech = leech_family_factory(leech_cfg)}};
    auto m = bsim::simulate(req.loaded.topology, spec, req.max_sim_time, req.session);
    auto src_elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - src_start).count();
    // Reach counts source(s) as delivered so the denominator stays fixed at honest_n.
    // With per-source exclusion both numerator and denominator shrank with more senders,
    // making reach drop artificially when unreachable nodes are correlated.
    double reach_pct = honest_n == 0 ? 0.0 : 100.0 * (m.delivered + 1) / honest_n;
    double p95_ms = m.percentile_delivery(0.95) * 1000.0;
    std::fprintf(stderr, "[bench]   algo=%s src=%zu/%zu node=%u reach=%.1f%% p95=%.0fms wall=%.1fs\n",
                 req.entry.label.c_str(), ++src_idx, req.sources.size(), src, reach_pct, p95_ms, src_elapsed);
    std::fflush(stderr);
    r.reach.add(reach_pct);
    r.total_x.add(m.total_bytes() / denom / per_node);
    std::uint64_t honest_body_out = 0;
    std::uint64_t honest_total_out = 0;
    for (NodeId i = 0; i < req.loaded.topology.node_count; i++) {
      if (!is_leech(req.loaded, i)) {
        honest_body_out += m.per_node[i].body_bytes_out;
        honest_total_out += m.per_node[i].body_bytes_out + m.per_node[i].control_bytes_out;
      }
    }
    r.honest_x.add(static_cast<double>(honest_body_out) / per_honest / denom);
    r.honest_total_x.add(static_cast<double>(honest_total_out) / per_honest / denom);
    r.p95_out_x.add(m.percentile_body_bytes_out(0.95) / denom);
    // p95 of per-node body bytes received (peak inbound — mirrors p95_out_x for receivers).
    std::vector<std::uint64_t> in_bytes;
    in_bytes.reserve(m.per_node.size());
    for (const auto &n : m.per_node) {
      in_bytes.push_back(n.body_bytes_in);
    }
    std::sort(in_bytes.begin(), in_bytes.end());
    auto p95_in = in_bytes.empty() ? 0 : in_bytes[static_cast<size_t>(0.95 * (in_bytes.size() - 1))];
    r.p95_in_x.add(static_cast<double>(p95_in) / denom);
    r.p50_ms.add(m.percentile_delivery(0.50) * 1000.0);
    r.p90_ms.add(m.percentile_delivery(0.90) * 1000.0);
    r.p95_ms.add(m.percentile_delivery(0.95) * 1000.0);
    r.p99_ms.add(m.percentile_delivery(0.99) * 1000.0);
    r.source_x.add(static_cast<double>(m.per_node[src].body_bytes_out) / denom);
    r.dup_pct.add(m.body_messages == 0 ? 0.0 : static_cast<double>(m.duplicate_messages) / m.body_messages);
    r.control_pct.add(m.total_bytes() == 0 ? 0.0 : static_cast<double>(m.control_bytes) / m.total_bytes());
  }
  auto algo_elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - algo_start).count();
  std::fprintf(stderr, "[bench] done  algo=%s reach=%.1f%% p95=%.0fms wall=%.1fs\n", req.entry.label.c_str(),
               r.reach.mean(), r.p95_ms.mean(), algo_elapsed);
  std::fflush(stderr);
  return r;
}

void print_table(const std::string &title, const std::vector<AggregateResult> &rows) {
  std::printf("\n== %s ==\n", title.c_str());
  std::printf("  %-22s %8s %8s %8s %8s %8s %8s %8s %8s\n", "algorithm", "reach%", "total", "honest", "p95_out", "p50ms",
              "p90ms", "p95ms", "p99ms");
  for (const auto &r : rows) {
    std::printf("  %-22s %8.1f %7.1fx %7.1fx %7.1fx %8.0f %8.0f %8.0f %8.0f\n", r.label.c_str(), r.reach.mean(),
                r.total_x.mean(), r.honest_x.mean(), r.p95_out_x.mean(), r.p50_ms.mean(), r.p90_ms.mean(),
                r.p95_ms.mean(), r.p99_ms.mean());
  }
}

// ---- CLI -----------------------------------------------------------------------------------

struct CliConfig {
  std::string graph_path = "/tmp/overlay-graph-mainnet.json";
  std::string latency_path = "/tmp/latency_matrix.json";
  std::string peers_path;  // when set, use the v2 /api/overlay-peers loader instead.
  std::uint32_t sources_n = 10;
  std::vector<std::uint64_t> body_sizes = {256, 100 * 1024};
  double max_sim_time = 60.0;
  std::uint32_t active_view = 0;
  double upload_bw_mbs = 100.0;     // = 800 Mbps validator-grade default
  std::uint32_t rounds = 1;         // multi-round mode: per-round convergence trajectory
  std::uint32_t warmup_rounds = 0;  // first N rounds run leech-free so scores converge before attack
  double round_interval = 5.0;      // wall-clock seconds between consecutive multi-round broadcasts
  // Streaming mode: concurrent broadcasts inside a single simulate(). When set, runs N
  // publications spread randomly over `streaming_span` seconds in one simulation. Metrics are
  // aggregated only over the last `(1 - streaming_warmup_ratio)` fraction of bodies (by publish
  // order) so the algorithm has time to converge before measurement starts.
  std::uint32_t streaming_count = 0;
  double streaming_span = 0.0;
  double streaming_warmup_ratio = 0.5;
  std::string per_node_stats_algo;  // when set, dump per-node delivery-count histogram for this row
  std::vector<double> leech_ratios;
  algo::LeechConfig leech_cfg;
  std::string leech_mode_label = "default";
  std::string csv_path;                      // --csv: emit v2 schema, one row per (algo, size, regime).
  std::string anysend_csv_path;              // --anysend-csv: per-source reach histogram (rotating-source).
  std::uint32_t anysend_sources = 50;        // --anysend-sources: rotate source over N honest peers.
  std::vector<std::string> only;             // --only ALGO,...: restrict catalogue (exact label match).
  bool prune_disconnected = false;           // --prune-disconnected: keep only largest honest SCC.
  bool with_synth = false;                   // --with-synth: also run a random-regular-1000 portability graph.
  std::vector<std::uint32_t> multi_senders;  // --multi-senders N: parallel-publisher test sizes.
  std::string descriptions_path;             // --descriptions: emit catalogue descriptions to file and exit.
  bool source_validators_only = false;       // --source-pool=validators: pick sources only from is_validator peers.
};

td::Result<double> parse_double(td::Slice arg) {
  auto str = arg.str();
  char *end = nullptr;
  errno = 0;
  auto value = std::strtod(str.c_str(), &end);
  if (errno != 0 || end == str.c_str() || *end != '\0' || !std::isfinite(value)) {
    return td::Status::Error("expected finite floating-point value");
  }
  return value;
}

td::Status parse_cli(int argc, char *argv[], CliConfig &cfg) {
  td::OptionParser p;
  p.set_description("run broadcast algorithm simulator against a mainnet overlay topology dump");
  p.add_option('h', "help", "print help", [&]() {
    char buf[8192];
    td::StringBuilder sb({buf, sizeof(buf) - 1});
    sb << p;
    std::cout << sb.as_cslice().c_str();
    std::cout.flush();
    std::exit(2);
  });
  // --graph selects synthetic|mainnet at the top-level dispatcher in main(); accept it here so
  // mainnet's own parser doesn't complain on `--graph mainnet`.
  p.add_checked_option('\0', "graph", "synthetic|mainnet (dispatched at top-level)", [&](td::Slice arg) {
    auto value = arg.str();
    if (value != "synthetic" && value != "mainnet") {
      return td::Status::Error("--graph must be synthetic or mainnet");
    }
    return td::Status::OK();
  });
  p.add_checked_option('\0', "graph-path", "overlay graph JSON path", [&](td::Slice arg) {
    cfg.graph_path = arg.str();
    return td::Status::OK();
  });
  p.add_checked_option('\0', "latency", "latency/geolocation JSON path", [&](td::Slice arg) {
    cfg.latency_path = arg.str();
    return td::Status::OK();
  });
  p.add_checked_option('\0', "peers", "v2 /api/overlay-peers JSON path (directed graph)", [&](td::Slice arg) {
    cfg.peers_path = arg.str();
    return td::Status::OK();
  });
  p.add_checked_option('\0', "sources", "number of honest sources to sample", [&](td::Slice arg) {
    TRY_RESULT_ASSIGN(cfg.sources_n, td::to_integer_safe<std::uint32_t>(arg));
    if (cfg.sources_n == 0) {
      return td::Status::Error("--sources must be positive");
    }
    return td::Status::OK();
  });
  p.add_checked_option('\0', "body-size", "body size in bytes; repeat for multiple sizes", [&](td::Slice arg) {
    TRY_RESULT(value, td::to_integer_safe<std::uint64_t>(arg));
    static bool overridden = false;
    if (!overridden) {
      cfg.body_sizes.clear();
      overridden = true;
    }
    cfg.body_sizes.push_back(value);
    return td::Status::OK();
  });
  p.add_checked_option('\0', "max-sim-time", "maximum simulated seconds per broadcast", [&](td::Slice arg) {
    TRY_RESULT_ASSIGN(cfg.max_sim_time, parse_double(arg));
    if (cfg.max_sim_time <= 0.0) {
      return td::Status::Error("--max-sim-time must be positive");
    }
    return td::Status::OK();
  });
  p.add_checked_option('\0', "active-view", "active view size; 0 means all peers are neighbours", [&](td::Slice arg) {
    TRY_RESULT_ASSIGN(cfg.active_view, td::to_integer_safe<std::uint32_t>(arg));
    return td::Status::OK();
  });
  p.add_checked_option('\0', "leech-ratio", "promote honest nodes to this leech ratio; repeatable", [&](td::Slice arg) {
    TRY_RESULT(value, parse_double(arg));
    if (value < 0.0 || value > 1.0) {
      return td::Status::Error("--leech-ratio must be in [0, 1]");
    }
    cfg.leech_ratios.push_back(value);
    return td::Status::OK();
  });
  p.add_checked_option('\0', "upload-bw", "per-node upload bandwidth in MB/s", [&](td::Slice arg) {
    TRY_RESULT_ASSIGN(cfg.upload_bw_mbs, parse_double(arg));
    if (cfg.upload_bw_mbs <= 0.0) {
      return td::Status::Error("--upload-bw must be positive");
    }
    return td::Status::OK();
  });
  p.add_checked_option('\0', "rounds", "number of score-persistent rounds", [&](td::Slice arg) {
    TRY_RESULT_ASSIGN(cfg.rounds, td::to_integer_safe<std::uint32_t>(arg));
    if (cfg.rounds == 0) {
      return td::Status::Error("--rounds must be positive");
    }
    return td::Status::OK();
  });
  p.add_checked_option('\0', "warmup-rounds", "initial rounds excluded from CSV (scoring still updates)",
                       [&](td::Slice arg) {
                         TRY_RESULT_ASSIGN(cfg.warmup_rounds, td::to_integer_safe<std::uint32_t>(arg));
                         return td::Status::OK();
                       });
  p.add_checked_option('\0', "round-interval", "seconds between multi-round broadcasts (default 5.0)",
                       [&](td::Slice arg) {
                         TRY_RESULT_ASSIGN(cfg.round_interval, parse_double(arg));
                         if (cfg.round_interval <= 0.0) {
                           return td::Status::Error("--round-interval must be positive");
                         }
                         return td::Status::OK();
                       });
  p.add_checked_option('\0', "streaming",
                       "concurrent-broadcast mode: \"N@T\" runs N publications spread over T simulated "
                       "seconds in a single simulate(); the last 50% are measured",
                       [&](td::Slice arg) {
                         auto s = arg.str();
                         auto sep = s.find('@');
                         if (sep == std::string::npos) {
                           return td::Status::Error("--streaming must be \"N@T\" (e.g. 120@30)");
                         }
                         TRY_RESULT_ASSIGN(cfg.streaming_count, td::to_integer_safe<std::uint32_t>(s.substr(0, sep)));
                         TRY_RESULT_ASSIGN(cfg.streaming_span, parse_double(s.substr(sep + 1)));
                         if (cfg.streaming_count == 0 || cfg.streaming_span <= 0.0) {
                           return td::Status::Error("--streaming count/span must be positive");
                         }
                         return td::Status::OK();
                       });
  p.add_checked_option('\0', "streaming-warmup",
                       "fraction of streaming bodies excluded from metrics (default 0.5 = first half warms up)",
                       [&](td::Slice arg) {
                         TRY_RESULT_ASSIGN(cfg.streaming_warmup_ratio, parse_double(arg));
                         if (cfg.streaming_warmup_ratio < 0.0 || cfg.streaming_warmup_ratio >= 1.0) {
                           return td::Status::Error("--streaming-warmup must be in [0, 1)");
                         }
                         return td::Status::OK();
                       });
  p.add_checked_option('\0', "per-node-stats", "dump per-node delivery histogram for named algo", [&](td::Slice arg) {
    cfg.per_node_stats_algo = arg.str();
    return td::Status::OK();
  });
  p.add_checked_option('\0', "csv", "emit v2-schema CSV (one row per algo×size×regime)", [&](td::Slice arg) {
    cfg.csv_path = arg.str();
    return td::Status::OK();
  });
  p.add_option('\0', "prune-disconnected", "drop honest nodes outside the largest honest SCC",
               [&]() { cfg.prune_disconnected = true; });
  p.add_option('\0', "with-synth", "also run a random-regular-1000 portability graph",
               [&]() { cfg.with_synth = true; });
  p.add_checked_option('\0', "multi-senders",
                       "AnySend test: comma-separated counts of parallel publishers (e.g. 1,5,10)", [&](td::Slice arg) {
                         cfg.multi_senders.clear();
                         std::string s = arg.str();
                         size_t start = 0;
                         while (start <= s.size()) {
                           auto comma = s.find(',', start);
                           auto part = s.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
                           if (!part.empty()) {
                             TRY_RESULT(n, td::to_integer_safe<std::uint32_t>(part));
                             cfg.multi_senders.push_back(n);
                           }
                           if (comma == std::string::npos)
                             break;
                           start = comma + 1;
                         }
                         return td::Status::OK();
                       });
  p.add_checked_option('\0', "anysend-csv", "emit AnySend (rotating-source) reach histogram CSV", [&](td::Slice arg) {
    cfg.anysend_csv_path = arg.str();
    return td::Status::OK();
  });
  p.add_checked_option('\0', "anysend-sources", "number of sources to rotate for AnySend", [&](td::Slice arg) {
    TRY_RESULT_ASSIGN(cfg.anysend_sources, td::to_integer_safe<std::uint32_t>(arg));
    if (cfg.anysend_sources == 0) {
      return td::Status::Error("--anysend-sources must be positive");
    }
    return td::Status::OK();
  });
  p.add_checked_option('\0', "only", "restrict catalogue to comma-separated labels", [&](td::Slice arg) {
    cfg.only.clear();
    std::string s = arg.str();
    size_t start = 0;
    while (start <= s.size()) {
      auto comma = s.find(',', start);
      auto part = s.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
      if (!part.empty()) {
        cfg.only.push_back(part);
      }
      if (comma == std::string::npos)
        break;
      start = comma + 1;
    }
    return td::Status::OK();
  });
  p.add_checked_option('\0', "leech-mode", "default|quiet|pull|full", [&](td::Slice arg) {
    auto mode = arg.str();
    cfg.leech_mode_label = mode;
    if (mode == "default") {
      cfg.leech_cfg = algo::LeechConfig{};
    } else if (mode == "quiet") {
      cfg.leech_cfg = algo::LeechConfig{.attract_haves = false};
    } else if (mode == "pull") {
      cfg.leech_cfg = algo::LeechConfig{.attract_haves = false, .proactive_request = true};
    } else if (mode == "full") {
      cfg.leech_cfg = algo::LeechConfig{.attract_haves = true, .proactive_request = true};
    } else {
      return td::Status::Error("unknown --leech-mode; expected default, quiet, pull, or full");
    }
    return td::Status::OK();
  });
  p.add_checked_option('\0', "descriptions", "emit catalogue label,description CSV and exit", [&](td::Slice arg) {
    cfg.descriptions_path = arg.str();
    return td::Status::OK();
  });
  p.add_checked_option('\0', "source-pool", "all|validators (default all)", [&](td::Slice arg) {
    auto value = arg.str();
    if (value == "validators") {
      cfg.source_validators_only = true;
    } else if (value != "all") {
      return td::Status::Error("--source-pool must be all or validators");
    }
    return td::Status::OK();
  });
  p.add_check([&]() {
    if (cfg.warmup_rounds > cfg.rounds) {
      return td::Status::Error("--warmup-rounds must be <= --rounds");
    }
    return td::Status::OK();
  });
  TRY_STATUS(p.run(argc, argv, 0).move_as_status());
  return td::Status::OK();
}

// ---- Run setup -----------------------------------------------------------------------------

// One evaluation regime: a topology + the source sample we use against it. Default regimes are
// (no-leeches) and (the natural ~12% mainnet leeches). With --leech-ratio R, additional honest
// nodes are randomly promoted to leech state until the global ratio reaches R.
struct Run {
  std::string label;
  Loaded loaded;
  std::vector<NodeId> sources;
};

Loaded make_synth_topology(std::uint32_t n, std::uint32_t degree, std::uint64_t seed, double latency_ms);

td::Result<Run> build_run(std::string label, Loaded loaded, const CliConfig &cfg) {
  loaded.topology.active_view_size = cfg.active_view;
  loaded.topology.upload_bw = cfg.upload_bw_mbs * 1024.0 * 1024.0;
  if (cfg.prune_disconnected) {
    auto demoted = prune_to_largest_honest_scc(loaded);
    if (demoted > 0) {
      std::printf("pruned %u honest nodes outside largest SCC\n", demoted);
    }
  }
  print_summary(loaded);
  std::vector<NodeId> honest;
  for (NodeId i = 0; i < loaded.topology.node_count; i++) {
    if (!is_leech(loaded, i)) {
      if (cfg.source_validators_only && i < loaded.nodes.size() && !loaded.nodes[i].is_validator) {
        continue;
      }
      honest.push_back(i);
    }
  }
  if (honest.empty()) {
    return td::Status::Error("topology has no honest source nodes");
  }
  if (cfg.source_validators_only) {
    std::printf("source pool restricted to %zu validators\n", honest.size());
  }
  std::shuffle(honest.begin(), honest.end(), std::mt19937{0xC07A});
  auto picked = std::min<std::uint32_t>(cfg.sources_n, static_cast<std::uint32_t>(honest.size()));
  return Run{std::move(label), std::move(loaded), std::vector<NodeId>(honest.begin(), honest.begin() + picked)};
}

td::Result<std::vector<Run>> all_runs(const CliConfig &cfg) {
  std::vector<Run> runs;
  if (!cfg.peers_path.empty()) {
    // V2 directed loader. Each --leech-ratio R rebuilds the graph under the "bad pool" model:
    // R=0 drops every natural-leech node; small R keeps all honest plus a random subset of the
    // bad pool sized to hit the ratio; large R uses the full bad pool + converts some honest.
    if (cfg.leech_ratios.empty()) {
      TRY_RESULT(loaded, load_v2(cfg.peers_path, cfg.latency_path));
      TRY_RESULT(run, build_run("v2 directed", std::move(loaded), cfg));
      runs.push_back(std::move(run));
    }
    for (auto ratio : cfg.leech_ratios) {
      TRY_RESULT(base, load_v2(cfg.peers_path, cfg.latency_path));
      auto extra = apply_leech_regime(base, ratio, /*seed=*/0xBEEF);
      char label[64];
      std::snprintf(label, sizeof(label), "v2 leech=%.0f%%", ratio * 100.0);
      TRY_RESULT(extra_run, build_run(label, std::move(extra), cfg));
      runs.push_back(std::move(extra_run));
    }
    if (cfg.with_synth) {
      // 1000-node random-regular graph, degree 20, uniform 50 ms latency. Same source sample
      // pattern as mainnet (first N honest after shuffle).
      auto synth = make_synth_topology(1000, 20, /*seed=*/0xC0DE, /*latency_ms=*/50.0);
      TRY_RESULT(synth_run, build_run("synth random-regular-1000", std::move(synth), cfg));
      runs.push_back(std::move(synth_run));
      for (auto ratio : cfg.leech_ratios) {
        auto base = make_synth_topology(1000, 20, /*seed=*/0xC0DE, /*latency_ms=*/50.0);
        auto extra = apply_leech_regime(base, ratio, /*seed=*/0xBEEF);
        char label[80];
        std::snprintf(label, sizeof(label), "synth leech=%.0f%%", ratio * 100.0);
        TRY_RESULT(synth_leech_run, build_run(label, std::move(extra), cfg));
        runs.push_back(std::move(synth_leech_run));
      }
    }
    return runs;
  }
  TRY_RESULT(no_leeches, load(cfg.graph_path, cfg.latency_path, true));
  TRY_RESULT(no_leech_run, build_run("no leeches", std::move(no_leeches), cfg));
  runs.push_back(std::move(no_leech_run));
  TRY_RESULT(with_leeches, load(cfg.graph_path, cfg.latency_path, false));
  TRY_RESULT(with_leech_run, build_run("with leeches", std::move(with_leeches), cfg));
  runs.push_back(std::move(with_leech_run));
  for (auto ratio : cfg.leech_ratios) {
    TRY_RESULT(loaded, load(cfg.graph_path, cfg.latency_path, false));
    promote_leeches(loaded, ratio, /*seed=*/0xBEEF);
    char label[64];
    std::snprintf(label, sizeof(label), "leech=%.0f%%", ratio * 100.0);
    TRY_RESULT(run, build_run(label, std::move(loaded), cfg));
    runs.push_back(std::move(run));
  }
  return runs;
}

// ---- Per-regime drivers --------------------------------------------------------------------

void print_single_round(const Run &r, std::uint64_t body_size, const std::vector<AlgoEntry> &catalogue,
                        const CliConfig &cfg, const std::string &title) {
  std::vector<AggregateResult> results;
  for (const auto &entry : catalogue) {
    results.push_back(run(RunRequest{.loaded = r.loaded,
                                     .entry = entry,
                                     .body_size = body_size,
                                     .sources = r.sources,
                                     .max_sim_time = cfg.max_sim_time,
                                     .leech_cfg = cfg.leech_cfg}));
  }
  print_table(title, results);
}

void print_per_node_stats(const Run &r, std::uint64_t body_size, const AlgoEntry &entry, const CliConfig &cfg) {
  std::vector<std::uint32_t> delivered(r.loaded.topology.node_count, 0);
  std::vector<std::uint32_t> participations(r.loaded.topology.node_count, 0);  // rounds where node isn't source
  bsim::SessionState session;
  session.round_interval = cfg.round_interval;
  auto leech_cfg = cfg.leech_cfg;
  std::uint32_t rounds = cfg.rounds;
  for (std::uint32_t round = 0; round < rounds; round++) {
    auto src = r.sources[round % r.sources.size()];
    Spec spec{.body_id = static_cast<BodyId>(round + 1),
              .source = src,
              .body_size = body_size,
              .piece_count = entry.piece_count,
              .all_peers_persistent = entry.require_persistent_peers,
              .all_peers_neighbours = entry.require_all_peers_neighbours,
              .factory = {.honest = entry_family_factory(entry), .leech = leech_family_factory(leech_cfg)}};
    auto m = bsim::simulate(r.loaded.topology, spec, cfg.max_sim_time, &session);
    for (NodeId i = 0; i < r.loaded.topology.node_count; i++) {
      if (i == src)
        continue;
      if (is_leech(r.loaded, i))
        continue;
      participations[i]++;
      if (m.per_node[i].delivery_time >= 0.0) {
        delivered[i]++;
      }
    }
  }
  // Histogram: number of honest nodes per "delivery count" bucket.
  std::vector<std::uint32_t> hist(rounds + 1, 0);
  std::vector<std::pair<std::uint32_t, NodeId>> per_node;
  for (NodeId i = 0; i < r.loaded.topology.node_count; i++) {
    if (is_leech(r.loaded, i))
      continue;
    if (participations[i] == 0)
      continue;
    hist[delivered[i]]++;
    per_node.emplace_back(delivered[i], i);
  }
  std::sort(per_node.begin(), per_node.end());
  std::printf("\n== per-node delivery stats: algo=%s rounds=%u honest=%zu ==\n", entry.label.c_str(), rounds,
              per_node.size());
  std::printf("  delivered/rounds  count  cumulative\n");
  std::uint32_t cum = 0;
  for (std::uint32_t d = 0; d <= rounds; d++) {
    if (hist[d] == 0)
      continue;
    cum += hist[d];
    std::printf("  %5u/%u            %5u  %5u\n", d, rounds, hist[d], cum);
  }
  // Worst 10 honest nodes by delivery count.
  std::printf("  worst 10 honest nodes:\n");
  for (std::size_t i = 0; i < std::min<std::size_t>(10, per_node.size()); i++) {
    auto [d, n] = per_node[i];
    std::printf("    node=%u delivered=%u/%u  hash=%.16s\n", n, d, rounds, r.loaded.nodes[n].hash.c_str());
  }
}

void emit_csv_row(std::ofstream &out, const std::string &regime_label, std::uint64_t body_size,
                  const AggregateResult &r, std::uint32_t round);

void print_multi_round(const Run &r, std::uint64_t body_size, const std::vector<AlgoEntry> &catalogue,
                       const CliConfig &cfg, const std::string &title, std::ofstream *csv_out = nullptr) {
  // Multi-round mode: each round = ONE broadcast from one source, source rotates per round.
  // Peer scores persist across rounds — every node accumulates feedback from every broadcast it
  // participated in. Models how a real overlay learns over time as many publishers broadcast.
  //
  // If --warmup-rounds N is set, the first N rounds run with all leech_nodes cleared so the
  // honest network's scores converge before the attack starts. SessionState (and therefore
  // every node's score table) persists across the warmup→attack switch.
  std::printf("\n== %s | rounds=%u (warmup=%u, 1 broadcast each, source rotates) ==\n", title.c_str(), cfg.rounds,
              cfg.warmup_rounds);
  std::printf("  %-14s %5s %8s %8s %8s %8s %8s %8s\n", "algorithm", "round", "reach%", "total", "honest", "p95_out",
              "p50ms", "p95ms");
  for (const auto &entry : catalogue) {
    // Per-entry shared state (e.g. SubDirectShared) is created fresh inside each catalogue
    // factory's closure on every `standard_catalogue(body_size)` call, so no cross-entry reset
    // is needed here.
    bsim::SessionState session;
    session.round_interval = cfg.round_interval;
    for (std::uint32_t round = 0; round < cfg.rounds; round++) {
      bool warmup = round < cfg.warmup_rounds;
      std::vector<NodeId> single_source = {r.sources[round % r.sources.size()]};
      auto agg = run(RunRequest{.loaded = r.loaded,
                                .entry = entry,
                                .body_size = body_size,
                                .sources = single_source,
                                .max_sim_time = cfg.max_sim_time,
                                .leech_cfg = cfg.leech_cfg,
                                .session = &session});
      std::printf("  %-14s %5u %8.1f %7.1fx %7.1fx %7.1fx %8.0f %8.0f%s\n", entry.label.c_str(), round,
                  agg.reach.mean(), agg.total_x.mean(), agg.honest_x.mean(), agg.p95_out_x.mean(), agg.p50_ms.mean(),
                  agg.p95_ms.mean(), warmup ? " (warmup)" : "");
      // Warmup rounds train the persistent scoring/mesh but stay out of the reported metrics.
      // The CSV gets only post-warmup rows so render_bench averages over the converged regime.
      if (csv_out != nullptr && csv_out->is_open() && !warmup) {
        emit_csv_row(*csv_out, r.label, body_size, agg, round + 1);
      }
    }
  }
  if (csv_out != nullptr && csv_out->is_open())
    csv_out->flush();
}

// Build a synthetic random-regular topology for portability comparison. Used when
// --synth-graph is set on the CLI. Wraps the topology in a `Loaded` so the rest of
// the runner can treat it identically to the mainnet graph.
Loaded make_synth_topology(std::uint32_t n, std::uint32_t degree, std::uint64_t seed, double latency_ms) {
  Loaded l;
  l.nodes.resize(n);
  l.topology.node_count = n;
  l.topology.peers.assign(n, {});
  l.topology.leech_nodes.assign(n, false);
  std::vector<std::unordered_set<NodeId>> peers(n);
  for (NodeId i = 0; i < n; i++) {
    peers[i].insert((i + 1) % n);
    peers[i].insert((i + n - 1) % n);
  }
  std::mt19937 rng(static_cast<std::uint32_t>(seed));
  for (NodeId i = 0; i < n; i++) {
    while (peers[i].size() < degree) {
      auto peer = static_cast<NodeId>(rng() % n);
      if (peer != i)
        peers[i].insert(peer);
    }
  }
  for (NodeId i = 0; i < n; i++) {
    l.topology.peers[i].assign(peers[i].begin(), peers[i].end());
    std::sort(l.topology.peers[i].begin(), l.topology.peers[i].end());
  }
  double lat = latency_ms / 1000.0;
  l.topology.latency.assign(n, std::vector<double>(n, lat));
  for (NodeId i = 0; i < n; i++)
    l.topology.latency[i][i] = 0.0;
  // Stamp generic hashes/country so print_summary doesn't crash. They aren't used for routing.
  for (NodeId i = 0; i < n; i++) {
    char buf[16];
    std::snprintf(buf, sizeof(buf), "synth-%05u", i);
    l.nodes[i] = LoadedNode{.hash = buf, .latlon = {0.0, 0.0}, .country = "synth"};
  }
  return l;
}

std::string size_class(std::uint64_t body_size);
std::string graph_tag(const std::string &regime_label);

// Multi-sender (AnySend): N validators all publish the same body_id at t=0. Per-source upload
// scales linearly; the question is whether parallel paths cut p95 latency and whether the
// honest-network total stays bounded (it should — receivers dedup on body_id).
AggregateResult run_multi_sender(const Run &r, std::uint64_t body_size, const AlgoEntry &entry, std::uint32_t n_senders,
                                 const CliConfig &cfg) {
  AggregateResult agg{.label = entry.label};
  double denom = body_size == 0 ? 1.0 : static_cast<double>(body_size);
  double per_node = std::max<std::uint32_t>(1, r.loaded.topology.node_count);
  std::uint32_t honest_n = 0;
  for (NodeId i = 0; i < r.loaded.topology.node_count; i++) {
    honest_n += is_leech(r.loaded, i) ? 0 : 1;
  }
  double per_honest = std::max<std::uint32_t>(1, honest_n);
  auto leech_cfg = cfg.leech_cfg;
  // Use the same sampled source set as single-sender runs, then add N-1 more sources alongside
  // it so the test compares a single broadcast vs an N-sender parallel broadcast from the same
  // primary. Source selection is deterministic for reproducibility.
  std::vector<NodeId> honest_pool;
  for (NodeId i = 0; i < r.loaded.topology.node_count; i++) {
    if (!is_leech(r.loaded, i))
      honest_pool.push_back(i);
  }
  std::shuffle(honest_pool.begin(), honest_pool.end(), std::mt19937{0xA15E});
  // multi-senders > 1: one primary is enough since N-1 additional sources are already deterministic.
  // Multiple primaries would only differ in the choice of "primary" label (used for body_id seeding),
  // not in the broadcast outcome. Single-sender keeps the full sample for variance.
  auto primaries = n_senders <= 1 ? r.sources : std::vector<NodeId>{r.sources.front()};
  for (auto primary : primaries) {
    if (n_senders == 0 || n_senders > honest_pool.size())
      continue;
    std::vector<NodeId> additional;
    for (auto candidate : honest_pool) {
      if (candidate == primary)
        continue;
      if (additional.size() + 1 >= n_senders)
        break;
      additional.push_back(candidate);
    }
    Spec spec{.body_id = static_cast<BodyId>(primary) + 1,
              .source = primary,
              .additional_sources = additional,
              .body_size = body_size,
              .piece_count = entry.piece_count,
              .all_peers_persistent = entry.require_persistent_peers,
              .all_peers_neighbours = entry.require_all_peers_neighbours,
              .factory = {.honest = entry_family_factory(entry), .leech = leech_family_factory(leech_cfg)}};
    auto m = bsim::simulate(r.loaded.topology, spec, cfg.max_sim_time);
    // Reach counts all senders as delivered: denominator = honest_n, numerator = delivered + n_senders.
    auto n_senders_actual = static_cast<std::uint32_t>(additional.size() + 1);
    agg.reach.add(honest_n == 0 ? 0.0 : 100.0 * (m.delivered + n_senders_actual) / honest_n);
    agg.total_x.add(m.total_bytes() / denom / per_node);
    std::uint64_t honest_body_out = 0;
    std::uint64_t honest_total_out = 0;
    for (NodeId i = 0; i < r.loaded.topology.node_count; i++) {
      if (!is_leech(r.loaded, i)) {
        honest_body_out += m.per_node[i].body_bytes_out;
        honest_total_out += m.per_node[i].body_bytes_out + m.per_node[i].control_bytes_out;
      }
    }
    agg.honest_x.add(static_cast<double>(honest_body_out) / per_honest / denom);
    agg.honest_total_x.add(static_cast<double>(honest_total_out) / per_honest / denom);
    agg.p95_out_x.add(m.percentile_body_bytes_out(0.95) / denom);
    std::vector<std::uint64_t> in_bytes;
    in_bytes.reserve(m.per_node.size());
    for (const auto &n : m.per_node)
      in_bytes.push_back(n.body_bytes_in);
    std::sort(in_bytes.begin(), in_bytes.end());
    auto p95_in = in_bytes.empty() ? 0 : in_bytes[static_cast<size_t>(0.95 * (in_bytes.size() - 1))];
    agg.p95_in_x.add(static_cast<double>(p95_in) / denom);
    agg.p50_ms.add(m.percentile_delivery(0.50) * 1000.0);
    agg.p90_ms.add(m.percentile_delivery(0.90) * 1000.0);
    agg.p95_ms.add(m.percentile_delivery(0.95) * 1000.0);
    agg.p99_ms.add(m.percentile_delivery(0.99) * 1000.0);
    agg.source_x.add(static_cast<double>(m.per_node[primary].body_bytes_out) / denom);
    agg.dup_pct.add(m.body_messages == 0 ? 0.0 : static_cast<double>(m.duplicate_messages) / m.body_messages);
    agg.control_pct.add(m.total_bytes() == 0 ? 0.0 : static_cast<double>(m.control_bytes) / m.total_bytes());
  }
  return agg;
}

void emit_multi_sender_csv_row(std::ofstream &out, const Run &r, std::uint64_t body_size, const AggregateResult &agg,
                               std::uint32_t n_senders) {
  out << agg.label << ',' << size_class(body_size) << ',' << graph_tag(r.label) << ',' << "multi" << n_senders << ','
      << 0 << ',' << 0 << ',' << agg.reach.mean() / 100.0 << ',' << agg.p50_ms.mean() << ',' << agg.p95_ms.mean() << ','
      << agg.p99_ms.mean() << ',' << agg.p95_out_x.mean() << ',' << agg.p95_in_x.mean() << ',' << agg.honest_x.mean()
      << ',' << agg.honest_total_x.mean() << ',' << agg.source_x.mean() << ',' << agg.dup_pct.mean() << ','
      << agg.control_pct.mean() << '\n';
}

// AnySend: rotate the source across a sample of honest nodes, bucket each broadcast's reach
// into five intervals. Tail buckets (<50%, 50-90) flag sources whose broadcast didn't deliver.
void emit_anysend_csv_header(std::ofstream &out) {
  out << "algorithm,size,graph,sources,b_lt50,b_50_90,b_90_99,b_99_999,b_100\n";
}

void emit_anysend(std::ofstream &out, const Run &r, std::uint64_t body_size, const AlgoEntry &entry,
                  const CliConfig &cfg) {
  std::vector<NodeId> honest;
  for (NodeId i = 0; i < r.loaded.topology.node_count; i++) {
    if (!is_leech(r.loaded, i))
      honest.push_back(i);
  }
  std::shuffle(honest.begin(), honest.end(), std::mt19937{0xA11});
  auto n_sources = std::min<std::uint32_t>(cfg.anysend_sources, static_cast<std::uint32_t>(honest.size()));
  std::array<std::uint32_t, 5> buckets{0, 0, 0, 0, 0};
  auto leech_cfg = cfg.leech_cfg;
  for (std::uint32_t i = 0; i < n_sources; i++) {
    auto src = honest[i];
    auto expected = honest_receiver_count(r.loaded, src);
    if (expected == 0)
      continue;
    Spec spec{.body_id = static_cast<BodyId>(src) + 1,
              .source = src,
              .body_size = body_size,
              .piece_count = entry.piece_count,
              .all_peers_persistent = entry.require_persistent_peers,
              .all_peers_neighbours = entry.require_all_peers_neighbours,
              .factory = {.honest = entry_family_factory(entry), .leech = leech_family_factory(leech_cfg)}};
    auto m = bsim::simulate(r.loaded.topology, spec, cfg.max_sim_time);
    double reach = static_cast<double>(m.delivered) / expected;
    if (reach < 0.50)
      buckets[0]++;
    else if (reach < 0.90)
      buckets[1]++;
    else if (reach < 0.99)
      buckets[2]++;
    else if (reach < 0.999)
      buckets[3]++;
    else
      buckets[4]++;
  }
  out << entry.label << ',' << size_class(body_size) << ',' << graph_tag(r.label) << ',' << n_sources;
  for (auto b : buckets)
    out << ',' << b;
  out << '\n';
  out.flush();
}

std::vector<AlgoEntry> filter_catalogue(std::vector<AlgoEntry> all, const std::vector<std::string> &keep) {
  if (keep.empty())
    return all;
  std::vector<AlgoEntry> out;
  for (const auto &name : keep) {
    auto it = std::find_if(all.begin(), all.end(), [&](const auto &e) { return e.label == name; });
    if (it != all.end()) {
      out.push_back(*it);
    } else {
      std::cerr << "warning: --only contains unknown algorithm \"" << name << "\"\n";
    }
  }
  return out;
}

// Map a regime label to a v2 graph tag. Today the loader produces a single mainnet topology
// labeled "v2 directed"; leech sweeps inherit the same graph. Synth/random-regular is not yet
// generated here — until it is, the column stays "mainnet".
std::string graph_tag(const std::string &regime_label) {
  if (regime_label.find("synth") != std::string::npos)
    return "synth";
  if (regime_label.find("v2") != std::string::npos)
    return "mainnet";
  if (regime_label.find("no leeches") != std::string::npos || regime_label.find("with leeches") != std::string::npos)
    return "mainnet-v1";
  return "mainnet";
}

// Map (body_size, regime) into the size_class label the Python renderer expects. The renderer
// shows results as small/medium/large; we keep the column orthogonal to the byte count so future
// scenarios can rename without rewriting the CSV.
std::string size_class(std::uint64_t body_size) {
  if (body_size <= 4 * 1024)
    return "small";  // 256 B (control / tiny payload)
  if (body_size <= 200 * 1024)
    return "medium";  // 100 KiB (typical block-sized)
  return "large";     // 1 MiB (large block / msg bundle)
}

double leech_fraction_for(const std::string &regime_label) {
  // Parse "v2 leech=30%" or "leech=30%" into 0.30. Otherwise 0.0 (clean run).
  auto pos = regime_label.find("leech=");
  if (pos == std::string::npos)
    return 0.0;
  return std::strtod(regime_label.c_str() + pos + 6, nullptr) / 100.0;
}

void emit_csv_header(std::ofstream &out) {
  out << "algorithm,size,graph,scenario,leech_pct,round,"
         "reach,p50_ms,p95_ms,p99_ms,"
         "node_out_p95,node_in_p95,"
         "network_overhead_honest,network_overhead_honest_total,source_x,"
         "dup_pct,control_pct\n";
}

void emit_csv_row(std::ofstream &out, const std::string &regime_label, std::uint64_t body_size,
                  const AggregateResult &r, std::uint32_t round = 0) {
  double leech_frac = leech_fraction_for(regime_label);
  out << r.label << ',' << size_class(body_size) << ',' << graph_tag(regime_label) << ',';
  // Scenario tag: "clean" / "leech30" / "round12" — picks the dimension actually varying.
  if (round > 0) {
    out << "round" << round;
  } else if (leech_frac > 0.0) {
    out << "leech" << static_cast<int>(std::round(leech_frac * 100));
  } else {
    out << "clean";
  }
  out << ',' << static_cast<int>(std::round(leech_frac * 100)) << ',';
  out << round << ',';
  // r.reach is already a percentage (0..100); the renderer expects a 0..1 ratio.
  out << r.reach.mean() / 100.0 << ',' << r.p50_ms.mean() << ',' << r.p95_ms.mean() << ',' << r.p99_ms.mean() << ','
      << r.p95_out_x.mean() << ',' << r.p95_in_x.mean() << ',' << r.honest_x.mean() << ',' << r.honest_total_x.mean()
      << ',' << r.source_x.mean() << ',' << r.dup_pct.mean() << ',' << r.control_pct.mean() << '\n';
}

int mainnet_main(int argc, char *argv[]) {
  SET_VERBOSITY_LEVEL(verbosity_WARNING);
  CliConfig cfg;
  auto cli_status = parse_cli(argc, argv, cfg);
  if (cli_status.is_error()) {
    std::cerr << cli_status.message().str() << "\n";
    return 2;
  }
  if (!cfg.descriptions_path.empty()) {
    std::ofstream out(cfg.descriptions_path);
    if (!out) {
      std::cerr << "failed to open --descriptions path: " << cfg.descriptions_path << "\n";
      return 2;
    }
    out << "algorithm\tdescription\n";
    // Catalogue is body-size aware; descriptions are stable across sizes, so probe with 1 MB.
    for (const auto &entry : standard_catalogue(1024 * 1024)) {
      out << entry.label << '\t' << entry.description << '\n';
    }
    return 0;
  }
  auto runs_result = all_runs(cfg);
  if (runs_result.is_error()) {
    auto error = runs_result.move_as_error();
    std::cerr << error.message().str() << "\n";
    return 2;
  }
  auto runs = runs_result.move_as_ok();
  std::ofstream csv_out;
  if (!cfg.csv_path.empty()) {
    csv_out.open(cfg.csv_path);
    if (!csv_out) {
      std::cerr << "failed to open --csv path: " << cfg.csv_path << "\n";
      return 2;
    }
    emit_csv_header(csv_out);
  }
  std::ofstream anysend_out;
  if (!cfg.anysend_csv_path.empty()) {
    anysend_out.open(cfg.anysend_csv_path);
    if (!anysend_out) {
      std::cerr << "failed to open --anysend-csv path: " << cfg.anysend_csv_path << "\n";
      return 2;
    }
    emit_anysend_csv_header(anysend_out);
  }
  for (auto body_size : cfg.body_sizes) {
    auto catalogue = filter_catalogue(standard_catalogue(body_size), cfg.only);
    for (const auto &r : runs) {
      char title[96];
      std::snprintf(title, sizeof(title), "%s | body=%llu B | leech-mode=%s", r.label.c_str(),
                    static_cast<unsigned long long>(body_size), cfg.leech_mode_label.c_str());
      if (cfg.rounds <= 1) {
        std::vector<AggregateResult> results;
        for (const auto &entry : catalogue) {
          results.push_back(run(RunRequest{.loaded = r.loaded,
                                           .entry = entry,
                                           .body_size = body_size,
                                           .sources = r.sources,
                                           .max_sim_time = cfg.max_sim_time,
                                           .leech_cfg = cfg.leech_cfg}));
        }
        print_table(title, results);
        if (csv_out.is_open()) {
          for (const auto &result : results) {
            emit_csv_row(csv_out, r.label, body_size, result, /*round=*/0);
          }
          csv_out.flush();
        }
        // AnySend: only on the base "clean" run for each graph (no leech) to keep runtime bounded.
        if (anysend_out.is_open() && leech_fraction_for(r.label) == 0.0) {
          for (const auto &entry : catalogue) {
            emit_anysend(anysend_out, r, body_size, entry, cfg);
          }
        }
        // Multi-sender (parallel-publisher AnySend): only on base clean run, only when configured.
        if (csv_out.is_open() && !cfg.multi_senders.empty() && leech_fraction_for(r.label) == 0.0) {
          for (auto n : cfg.multi_senders) {
            for (const auto &entry : catalogue) {
              auto agg = run_multi_sender(r, body_size, entry, n, cfg);
              std::printf(
                  "  multi-senders=%-3u %-22s reach=%.1f%% honest=%.1fx p50=%.0fms p90=%.0fms p95=%.0fms p99=%.0fms\n",
                  n, entry.label.c_str(), agg.reach.mean(), agg.honest_x.mean(), agg.p50_ms.mean(), agg.p90_ms.mean(),
                  agg.p95_ms.mean(), agg.p99_ms.mean());
              emit_multi_sender_csv_row(csv_out, r, body_size, agg, n);
            }
          }
          csv_out.flush();
        }
      } else {
        if (!cfg.per_node_stats_algo.empty()) {
          for (const auto &entry : catalogue) {
            if (entry.label == cfg.per_node_stats_algo) {
              print_per_node_stats(r, body_size, entry, cfg);
            }
          }
        } else {
          print_multi_round(r, body_size, catalogue, cfg, title, csv_out.is_open() ? &csv_out : nullptr);
        }
      }
    }
  }
  if (csv_out.is_open()) {
    std::cerr << "csv=" << cfg.csv_path << "\n";
  }
  return 0;
}

}  // namespace ton::bsim_runner
