#include <algorithm>
#include <cmath>
#include <cstdio>
#include <random>
#include <unordered_map>

#include "td/utils/JsonBuilder.h"
#include "td/utils/buffer.h"
#include "td/utils/filesystem.h"

#include "mainnet-loader.h"

namespace ton::bsim_runner {

namespace {

using bsim::NodeId;

td::Result<std::string> slurp(const std::string &path) {
  TRY_RESULT(data, td::read_file(path));
  return data.as_slice().str();
}

// One-way latency between two lat/lon points. Same fit as packed-sim's "observer-bucket-p50-v1":
// alpha + beta * haversine_km, fitted to median observed RTT/2 from public-overlay crawler.
double link_latency_seconds(std::pair<double, double> a, std::pair<double, double> b) {
  auto rad = [](double d) { return d * (M_PI / 180.0); };
  double p1 = rad(a.first), p2 = rad(b.first);
  double dp = rad(b.first - a.first), dl = rad(b.second - a.second);
  double s = std::sin(dp / 2) * std::sin(dp / 2) + std::cos(p1) * std::cos(p2) * std::sin(dl / 2) * std::sin(dl / 2);
  double km = 2 * 6371.0088 * std::asin(std::sqrt(s));
  constexpr double kAlphaMs = 3.554;
  constexpr double kBetaMsPerKm = 0.008963;
  return (kAlphaMs + kBetaMsPerKm * km) / 1000.0;
}

}  // namespace

td::Result<Loaded> load(const std::string &graph_path, const std::string &latency_path, bool exclude_leeches) {
  TRY_RESULT(graph_str, slurp(graph_path));
  TRY_RESULT(latency_str, slurp(latency_path));
  TRY_RESULT(graph_json, td::json_decode(td::MutableSlice(graph_str.data(), graph_str.size())));
  TRY_RESULT(latency_json, td::json_decode(td::MutableSlice(latency_str.data(), latency_str.size())));
  if (graph_json.type() != td::JsonValue::Type::Array) {
    return td::Status::Error("overlay graph JSON must be an array");
  }
  if (latency_json.type() != td::JsonValue::Type::Object) {
    return td::Status::Error("latency JSON must be an object");
  }

  // Geo first — only nodes that have lat/lon are kept.
  std::unordered_map<std::string, std::pair<double, double>> latlon;
  std::unordered_map<std::string, std::string> country;
  TRY_RESULT(nodes_field, latency_json.get_object().extract_required_field("nodes", td::JsonValue::Type::Object));
  for (auto &kv : nodes_field.get_object().field_values_) {
    if (kv.second.type() != td::JsonValue::Type::Object) {
      return td::Status::Error("latency node entry must be an object");
    }
    auto &info = kv.second.get_object();
    auto hash = kv.first.str();
    TRY_RESULT(lat, info.get_required_double_field("lat"));
    TRY_RESULT(lon, info.get_required_double_field("lon"));
    TRY_RESULT(node_country, info.get_optional_string_field("country", ""));
    latlon[hash] = {lat, lon};
    country[hash] = node_country;
  }

  Loaded out;
  std::unordered_map<std::string, NodeId> id_by_hash;

  // First pass: populate nodes, leech flags.
  std::vector<bool> leech_flags;
  for (auto &n : graph_json.get_array()) {
    if (n.type() != td::JsonValue::Type::Object) {
      return td::Status::Error("overlay graph node entry must be an object");
    }
    auto &o = n.get_object();
    TRY_RESULT(hash, o.get_required_string_field("node"));
    if (latlon.count(hash) == 0) {
      continue;
    }
    TRY_RESULT(unresp, o.get_optional_bool_field("unresponsive", false));
    if (exclude_leeches && unresp) {
      continue;
    }
    id_by_hash.emplace(hash, static_cast<NodeId>(out.nodes.size()));
    out.nodes.push_back({hash, latlon[hash], country[hash]});
    leech_flags.push_back(!exclude_leeches && unresp);
  }
  out.topology.node_count = static_cast<std::uint32_t>(out.nodes.size());
  out.topology.peers.resize(out.topology.node_count);
  out.topology.leech_nodes = std::move(leech_flags);

  // Second pass: fill peer adjacency, restricted to in-set neighbours.
  for (auto &n : graph_json.get_array()) {
    if (n.type() != td::JsonValue::Type::Object) {
      return td::Status::Error("overlay graph node entry must be an object");
    }
    auto &o = n.get_object();
    TRY_RESULT(hash, o.get_required_string_field("node"));
    auto it = id_by_hash.find(hash);
    if (it == id_by_hash.end()) {
      continue;
    }
    auto self = it->second;
    TRY_RESULT(nb, o.extract_required_field("neighbours", td::JsonValue::Type::Array));
    for (auto &peer : nb.get_array()) {
      if (peer.type() != td::JsonValue::Type::String) {
        return td::Status::Error("overlay graph neighbour entry must be a string");
      }
      auto jt = id_by_hash.find(peer.get_string().str());
      if (jt != id_by_hash.end()) {
        out.topology.peers[self].push_back(jt->second);
      }
    }
  }

  // Third pass: latency matrix (full N×N, geo-derived).
  out.topology.latency.assign(out.topology.node_count, std::vector<double>(out.topology.node_count, 0.0));
  for (NodeId i = 0; i < out.topology.node_count; i++) {
    for (NodeId j = 0; j < out.topology.node_count; j++) {
      if (j != i) {
        out.topology.latency[i][j] = link_latency_seconds(out.nodes[i].latlon, out.nodes[j].latlon);
      }
    }
  }
  return out;
}

td::Result<Loaded> load_v2(const std::string &peers_path, const std::string &latency_path) {
  TRY_RESULT(peers_text, slurp(peers_path));
  TRY_RESULT(root, td::json_decode(td::MutableSlice(peers_text.data(), peers_text.size())));
  if (root.type() != td::JsonValue::Type::Object) {
    return td::Status::Error("overlay-peers JSON must be an object");
  }
  TRY_RESULT(peers_field, root.get_object().extract_required_field("peers", td::JsonValue::Type::Array));
  auto &peers_arr = peers_field.get_array();

  // Load latency_matrix.json — same geo data as the v1 loader. Keyed by adnl_id (hex). Peers
  // missing from the matrix will fall back to a default edge latency.
  std::unordered_map<std::string, std::pair<double, double>> latlon;
  std::unordered_map<std::string, std::string> country;
  TRY_RESULT(latency_text, slurp(latency_path));
  TRY_RESULT(latency_json, td::json_decode(td::MutableSlice(latency_text.data(), latency_text.size())));
  TRY_RESULT(nodes_field, latency_json.get_object().extract_required_field("nodes", td::JsonValue::Type::Object));
  for (auto &kv : nodes_field.get_object().field_values_) {
    if (kv.second.type() != td::JsonValue::Type::Object) {
      return td::Status::Error("latency node entry must be an object");
    }
    auto &info = kv.second.get_object();
    auto hash = kv.first.str();
    TRY_RESULT(lat, info.get_required_double_field("lat"));
    TRY_RESULT(lon, info.get_required_double_field("lon"));
    TRY_RESULT(node_country, info.get_optional_string_field("country", ""));
    latlon[hash] = {lat, lon};
    country[hash] = node_country;
  }

  Loaded out;
  std::unordered_map<std::string, NodeId> id_by_hash;
  std::vector<bool> leech_flags;
  std::vector<bool> has_latlon;
  // Cache the parsed neighbour lists per peer (string adnl_ids) — second-pass edge filling
  // can't re-`extract_required_field` because extraction is destructive.
  std::vector<std::vector<std::string>> raw_neighbours;

  // Graph membership: every peer entry becomes a NodeId. Peers without `recent_neighbours` have
  // no observed outgoing edges and are treated as leeches (passive receivers only).
  // Honest = peer has a `fec` field AND non-empty recent_neighbours (we've actually seen them
  // forward in the FEC overlay). Leech = everyone else.
  // Neighbour list is capped to `kNeighbourCap` freshest entries (the crawler dump pre-sorts
  // recent_neighbours newest-first, and the production peer table actively uses ~20 peers).
  constexpr std::size_t kNeighbourCap = 20;
  for (auto &n : peers_arr) {
    if (n.type() != td::JsonValue::Type::Object) {
      return td::Status::Error("overlay-peers entry must be an object");
    }
    auto &o = n.get_object();
    TRY_RESULT(hash, o.get_required_string_field("adnl_id"));
    TRY_RESULT(rn_field, o.extract_required_field("recent_neighbours", td::JsonValue::Type::Array));
    std::vector<std::string> rn;
    rn.reserve(std::min(rn_field.get_array().size(), kNeighbourCap));
    for (auto &peer : rn_field.get_array()) {
      if (peer.type() != td::JsonValue::Type::String) {
        return td::Status::Error("recent_neighbours entry must be a string");
      }
      rn.push_back(peer.get_string().str());
      if (rn.size() >= kNeighbourCap) {
        break;
      }
    }
    // Honest = peer has a `fec` field AND we have outgoing-edge observations. Old crawler dumps
    // used `fec.fec_parts > 0`; the new simulator-compressed dump emits `fec.last_seen` only.
    // Either way, the field's presence is the signal. A peer without outgoing edges can't
    // forward anything so it's a leech even if `fec` is set.
    bool has_fec = false;
    if (auto fec_field = o.extract_optional_field("fec", td::JsonValue::Type::Object); fec_field.is_ok()) {
      has_fec = fec_field.ok().type() == td::JsonValue::Type::Object;
    }
    bool is_validator = false;
    if (auto val_field = o.extract_optional_field("is_validator", td::JsonValue::Type::Boolean); val_field.is_ok()) {
      is_validator = val_field.ok().get_boolean();
    }
    bool is_rebroadcaster = has_fec && !rn.empty();
    id_by_hash.emplace(hash, static_cast<NodeId>(out.nodes.size()));
    auto it_geo = latlon.find(hash);
    std::pair<double, double> ll = (it_geo == latlon.end()) ? std::pair<double, double>{0.0, 0.0} : it_geo->second;
    auto country_str = (it_geo == latlon.end()) ? std::string{} : country[hash];
    out.nodes.push_back({hash, ll, country_str, is_validator});
    leech_flags.push_back(!is_rebroadcaster);
    has_latlon.push_back(it_geo != latlon.end());
    raw_neighbours.push_back(std::move(rn));
  }
  out.topology.node_count = static_cast<std::uint32_t>(out.nodes.size());
  out.topology.peers.assign(out.topology.node_count, {});
  out.topology.leech_nodes = std::move(leech_flags);

  for (NodeId self = 0; self < out.topology.node_count; self++) {
    for (auto &peer_hash : raw_neighbours[self]) {
      auto jt = id_by_hash.find(peer_hash);
      if (jt != id_by_hash.end()) {
        out.topology.peers[self].push_back(jt->second);
      }
    }
  }

  // Geo-derived latency, same formula as v1. When either endpoint has no lat/lon entry,
  // fall back to 50 ms (intercontinental-ish median).
  constexpr double kDefaultLatencyS = 0.050;
  out.topology.latency.assign(out.topology.node_count, std::vector<double>(out.topology.node_count, 0.0));
  for (NodeId i = 0; i < out.topology.node_count; i++) {
    for (NodeId j = 0; j < out.topology.node_count; j++) {
      if (j == i)
        continue;
      out.topology.latency[i][j] = (has_latlon[i] && has_latlon[j])
                                       ? link_latency_seconds(out.nodes[i].latlon, out.nodes[j].latlon)
                                       : kDefaultLatencyS;
    }
  }
  return out;
}

Loaded restrict_to_subset(const Loaded &original, const std::vector<bool> &keep) {
  CHECK(keep.size() == original.topology.node_count);
  std::vector<NodeId> remap(original.topology.node_count, static_cast<NodeId>(-1));
  Loaded out;
  out.topology.active_view_size = original.topology.active_view_size;
  out.topology.upload_bw = original.topology.upload_bw;
  for (NodeId i = 0; i < original.topology.node_count; i++) {
    if (keep[i]) {
      remap[i] = static_cast<NodeId>(out.nodes.size());
      out.nodes.push_back(original.nodes[i]);
    }
  }
  auto n = static_cast<std::uint32_t>(out.nodes.size());
  out.topology.node_count = n;
  out.topology.peers.assign(n, {});
  out.topology.leech_nodes.assign(n, false);
  out.topology.latency.assign(n, std::vector<double>(n, 0.0));
  for (NodeId i = 0; i < original.topology.node_count; i++) {
    if (!keep[i])
      continue;
    auto new_i = remap[i];
    if (i < original.topology.leech_nodes.size()) {
      out.topology.leech_nodes[new_i] = original.topology.leech_nodes[i];
    }
    for (auto p : original.topology.peers[i]) {
      if (p < keep.size() && keep[p]) {
        out.topology.peers[new_i].push_back(remap[p]);
      }
    }
    for (NodeId j = 0; j < original.topology.node_count; j++) {
      if (j != i && keep[j]) {
        out.topology.latency[new_i][remap[j]] = original.topology.latency[i][j];
      }
    }
  }
  return out;
}

Loaded apply_leech_regime(const Loaded &original, double leech_ratio, std::uint32_t seed) {
  // Split into "bad pool" (originally leech in the loaded data — peers without `fec` or without
  // observed outgoing edges) and "good pool" (honest at load time). The bad pool never becomes
  // honest; instead we either drop a subset of it from the graph or include it as leech.
  std::vector<NodeId> bad_pool;
  std::vector<NodeId> good_pool;
  for (NodeId i = 0; i < original.topology.node_count; i++) {
    bool is_bad = i < original.topology.leech_nodes.size() && original.topology.leech_nodes[i];
    (is_bad ? bad_pool : good_pool).push_back(i);
  }
  auto good_n = good_pool.size();
  auto bad_n = bad_pool.size();
  std::mt19937 rng(seed);
  std::shuffle(bad_pool.begin(), bad_pool.end(), rng);
  std::shuffle(good_pool.begin(), good_pool.end(), rng);

  std::size_t include_bad = 0;
  std::size_t convert_good = 0;
  if (leech_ratio > 0.0 && good_n > 0) {
    // Stage 1: keep all good + add bad up to ratio (until bad pool exhausted).
    // ratio = N / (good_n + N) → N = ratio * good_n / (1 - ratio).
    double need_double = leech_ratio * static_cast<double>(good_n) / std::max(1e-9, 1.0 - leech_ratio);
    auto target_bad = static_cast<std::size_t>(std::round(need_double));
    if (target_bad <= bad_n) {
      include_bad = target_bad;
    } else {
      // Stage 2: bad pool exhausted, convert good → leech to reach the target.
      // Total = good_n + bad_n. leech = round(ratio * total).
      include_bad = bad_n;
      auto total = good_n + bad_n;
      auto target_leech = static_cast<std::size_t>(std::round(leech_ratio * static_cast<double>(total)));
      convert_good = target_leech > bad_n ? std::min<std::size_t>(target_leech - bad_n, good_n) : 0;
    }
  }

  std::vector<bool> keep(original.topology.node_count, false);
  for (auto g : good_pool)
    keep[g] = true;
  for (std::size_t i = 0; i < include_bad; i++)
    keep[bad_pool[i]] = true;

  auto out = restrict_to_subset(original, keep);
  // Mark leech in the rebuilt graph: every kept bad is leech (already set via leech_nodes copy),
  // plus the first `convert_good` good nodes converted.
  std::vector<NodeId> remap(original.topology.node_count, static_cast<NodeId>(-1));
  NodeId new_id = 0;
  for (NodeId i = 0; i < original.topology.node_count; i++) {
    if (keep[i])
      remap[i] = new_id++;
  }
  for (std::size_t i = 0; i < convert_good; i++) {
    out.topology.leech_nodes[remap[good_pool[i]]] = true;
  }
  return out;
}

void promote_leeches(Loaded &l, double target_ratio, std::uint32_t seed) {
  if (target_ratio <= 0.0) {
    return;
  }
  if (l.topology.leech_nodes.empty()) {
    l.topology.leech_nodes.assign(l.topology.node_count, false);
  }
  auto target = static_cast<std::size_t>(std::round(target_ratio * l.topology.node_count));
  std::vector<NodeId> honest_pool;
  for (NodeId i = 0; i < l.topology.node_count; i++) {
    if (!l.topology.leech_nodes[i]) {
      honest_pool.push_back(i);
    }
  }
  auto already_leech = l.topology.node_count - honest_pool.size();
  if (already_leech >= target) {
    return;
  }
  if (honest_pool.size() <= 1) {
    return;
  }
  std::shuffle(honest_pool.begin(), honest_pool.end(), std::mt19937{seed});
  auto promote = std::min<std::size_t>(target - already_leech, honest_pool.size() - 1);
  for (std::size_t i = 0; i < promote; i++) {
    l.topology.leech_nodes[honest_pool[i]] = true;
  }
}

std::uint32_t prune_to_largest_honest_scc(Loaded &l) {
  // Tarjan's strongly-connected-components on the honest-only subgraph. Edges where either
  // endpoint is a leech are ignored. Then every honest vertex outside the largest SCC is
  // demoted to leech state so reach math counts only nodes that *can* be reached.
  auto n = l.topology.node_count;
  if (l.topology.leech_nodes.size() < n) {
    l.topology.leech_nodes.assign(n, false);
  }
  std::vector<int> index(n, -1), lowlink(n, 0);
  std::vector<bool> on_stack(n, false);
  std::vector<NodeId> stack;
  std::vector<std::vector<NodeId>> sccs;
  int next_index = 0;
  auto is_leech = [&](NodeId v) { return l.topology.leech_nodes[v]; };
  // Iterative Tarjan to avoid blowing the call stack on 1000+ node graphs.
  for (NodeId root = 0; root < n; root++) {
    if (index[root] != -1 || is_leech(root))
      continue;
    struct Frame {
      NodeId v;
      std::size_t next_edge;
    };
    std::vector<Frame> frames;
    index[root] = next_index;
    lowlink[root] = next_index;
    next_index++;
    stack.push_back(root);
    on_stack[root] = true;
    frames.push_back({root, 0});
    while (!frames.empty()) {
      auto &top = frames.back();
      const auto &peers = l.topology.peers[top.v];
      bool recursed = false;
      while (top.next_edge < peers.size()) {
        auto w = peers[top.next_edge++];
        if (is_leech(w))
          continue;
        if (index[w] == -1) {
          index[w] = next_index;
          lowlink[w] = next_index;
          next_index++;
          stack.push_back(w);
          on_stack[w] = true;
          frames.push_back({w, 0});
          recursed = true;
          break;
        }
        if (on_stack[w]) {
          lowlink[top.v] = std::min(lowlink[top.v], index[w]);
        }
      }
      if (recursed)
        continue;
      auto v = top.v;
      auto v_lowlink = lowlink[v];
      if (v_lowlink == index[v]) {
        std::vector<NodeId> scc;
        while (true) {
          auto w = stack.back();
          stack.pop_back();
          on_stack[w] = false;
          scc.push_back(w);
          if (w == v)
            break;
        }
        sccs.push_back(std::move(scc));
      }
      frames.pop_back();
      if (!frames.empty()) {
        auto parent = frames.back().v;
        lowlink[parent] = std::min(lowlink[parent], v_lowlink);
      }
    }
  }
  if (sccs.empty())
    return 0;
  std::size_t best = 0;
  for (std::size_t i = 1; i < sccs.size(); i++) {
    if (sccs[i].size() > sccs[best].size())
      best = i;
  }
  std::vector<bool> in_largest(n, false);
  for (auto v : sccs[best])
    in_largest[v] = true;
  std::uint32_t demoted = 0;
  for (NodeId v = 0; v < n; v++) {
    if (!is_leech(v) && !in_largest[v]) {
      l.topology.leech_nodes[v] = true;
      demoted++;
    }
  }
  return demoted;
}

void print_summary(const Loaded &l) {
  std::size_t leech = 0, edges = 0, max_deg = 0;
  std::unordered_map<std::string, std::size_t> by_country;
  for (NodeId i = 0; i < l.topology.node_count; i++) {
    leech += (i < l.topology.leech_nodes.size() && l.topology.leech_nodes[i]) ? 1 : 0;
    edges += l.topology.peers[i].size();
    max_deg = std::max(max_deg, l.topology.peers[i].size());
    by_country[l.nodes[i].country]++;
  }
  std::printf("topology: nodes=%u leech=%zu edges=%zu mean-degree=%.1f max-degree=%zu\n", l.topology.node_count, leech,
              edges, l.topology.node_count == 0 ? 0.0 : static_cast<double>(edges) / l.topology.node_count, max_deg);
  std::vector<std::pair<std::string, std::size_t>> top(by_country.begin(), by_country.end());
  std::sort(top.begin(), top.end(), [](auto &a, auto &b) { return a.second > b.second; });
  std::printf("top countries:");
  for (std::size_t i = 0; i < std::min<std::size_t>(top.size(), 5); i++) {
    std::printf(" %s=%zu", top[i].first.c_str(), top[i].second);
  }
  std::printf("\n");
}

}  // namespace ton::bsim_runner
