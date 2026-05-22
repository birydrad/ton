#!/usr/bin/env python3
"""Theoretical pipelined latency floor for broadcast on a given graph.

Store-and-forward + pipelined FEC streaming. For each receiver R:

  T_decode(R) = min_{s in sources} [Σ_edges (latency_e + part_size / bw)] + (K - 1) * part_size / bw
                                     ↑                                          ↑
                                first part traverses path                pipeline tail (K-1 more parts)

That is — first part arrives via shortest-path-with-transmission from nearest source,
then K-1 more parts queue up at the bottleneck output rate (assumed uniform `bw`).

Output: p50/p90/p95/p99/max of T_decode across honest receivers + unreached count.

Assumptions:
- relays only through honest nodes (leech don't forward)
- destination = honest receivers
- uniform bandwidth `bw` on every edge (default 100 MB/s)
- per-source FEC: each source independently streams K parts; receiver picks min arrival

Usage:
    python3 graph_latency_floor.py [graph_path] [sources_csv] [seeds]

Body presets reported:
  100 KB: K=25 parts × 4 KB
  1 MB:   K=30 parts × ~35 KB

Example:
    python3 test/broadcast-sim/graph_latency_floor.py /Users/arseny30/code/ton/broadcast-graph.json 1,10,23,100,400 30
"""

import heapq
import json
import random
import statistics
import sys


BANDWIDTH_BPS = 100 * 1024 * 1024  # 100 MB/s.

BODY_PRESETS = [
    ("100KB", 25, 100 * 1024 // 25),       # 25 parts × 4 KB
    ("1MB",  30, 1024 * 1024 // 30),        # 30 parts × ~35 KB
]


def load_graph(path):
    with open(path) as f:
        return json.load(f)


def percentile(values, p):
    if not values:
        return float("nan")
    s = sorted(values)
    return s[min(int(len(s) * p), len(s) - 1)]


def shortest_path_floor(graph, n_sources, seed, part_size_bytes, k_parts, bandwidth_bps):
    nodes = graph["nodes"]
    n = len(nodes)
    honest = [i for i, node in enumerate(nodes) if not node.get("leech")]
    can_relay = [not node.get("leech") for node in nodes]

    rng = random.Random(seed)
    sources = rng.sample(honest, min(n_sources, len(honest)))
    source_set = set(sources)

    transmission_ms = (part_size_bytes / bandwidth_bps) * 1000.0

    adj = [[] for _ in range(n)]
    for edge in graph["edges"]:
        cost = edge["latency_ms"] + transmission_ms
        adj[edge["from"]].append((edge["to"], cost))

    dist = [float("inf")] * n
    heap = []
    for s in sources:
        dist[s] = 0.0
        heapq.heappush(heap, (0.0, s))

    while heap:
        d, u = heapq.heappop(heap)
        if d > dist[u]:
            continue
        if u not in source_set and not can_relay[u]:
            continue
        for (v, w) in adj[u]:
            nd = d + w
            if nd < dist[v]:
                dist[v] = nd
                heapq.heappush(heap, (nd, v))

    tail_ms = (k_parts - 1) * transmission_ms

    receivers = [i for i in honest if i not in source_set]
    decode_times = [(dist[i] + tail_ms) for i in receivers if dist[i] != float("inf")]
    unreached = len(receivers) - len(decode_times)
    return decode_times, unreached, len(receivers)


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "/Users/arseny30/code/ton/broadcast-graph.json"
    source_counts = [int(x) for x in (sys.argv[2] if len(sys.argv) > 2 else "1,10,23,100,400").split(",")]
    seeds = int(sys.argv[3]) if len(sys.argv) > 3 else 30

    graph = load_graph(path)
    honest_n = sum(1 for n in graph["nodes"] if not n.get("leech"))
    print(f"graph: {graph['node_count']} nodes ({honest_n} honest), {graph['edge_count']} edges, bw={BANDWIDTH_BPS/1024/1024:.0f} MB/s")
    print()

    for body_label, k_parts, part_size in BODY_PRESETS:
        transmission_ms = (part_size / BANDWIDTH_BPS) * 1000.0
        print(f"=== body={body_label}, K={k_parts} parts × {part_size}B, transmission/hop={transmission_ms:.3f}ms ===")
        print(f"{'sources':>8}  {'p50':>6}  {'p90':>6}  {'p95':>6}  {'p99':>6}  {'max':>6}  {'unreached':>9}")
        for k in source_counts:
            runs = []
            for s in range(1, seeds + 1):
                decode_times, unreached, total = shortest_path_floor(
                    graph, k, s, part_size, k_parts, BANDWIDTH_BPS)
                runs.append({
                    "p50": percentile(decode_times, 0.50),
                    "p90": percentile(decode_times, 0.90),
                    "p95": percentile(decode_times, 0.95),
                    "p99": percentile(decode_times, 0.99),
                    "max": max(decode_times) if decode_times else float("inf"),
                    "unreached": unreached,
                })
            avg = {k_: statistics.mean(r[k_] for r in runs) for k_ in runs[0]}
            print(f"{k:>8}  {avg['p50']:>6.1f}  {avg['p90']:>6.1f}  {avg['p95']:>6.1f}  "
                  f"{avg['p99']:>6.1f}  {avg['max']:>6.1f}  {avg['unreached']:>9.1f}")
        print()


if __name__ == "__main__":
    main()
