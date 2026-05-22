#!/usr/bin/env python3
"""Twostep-FEC broadcast floor (numpy-vectorized) matching overlay/broadcast-twostep.cpp.

Protocol per source S, N persistent nodes (other_nodes count = N-1):
  - K_total = N-1 FEC symbols generated; K_needed = (2(N-1)-2)/3 to decode.
  - Symbol i deterministically routed to delegate D_i = peers[i].
  - Each delegate, upon receiving its piece from S, rebroadcasts to all other peers.
  - Leech delegates do NOT rebroadcast (only they themselves get their piece).

Per-receiver arrival of piece i at honest R (delegate D_i):
  - own piece (D_i == R):       T = L[S][R] + tx          (1 hop)
  - honest D_i (D_i != R):      T = L[S][D_i] + L[D_i][R] + 2*tx
  - leech D_i (D_i != R):       T = +inf  (never arrives at R)

Decode time at R = K_needed-th smallest arrival across all N-1 pieces.
Multi-source: min decode across sources.

Latency: L[i][j] = alpha + beta * haversine_km; fallback when geo missing.
"""

import json
import math
import random
import statistics
import sys

import numpy as np


BW = 100 * 1024 * 1024
ALPHA_MS = 3.554
BETA_MS_PER_KM = 0.008963
FALLBACK_MS = 50.0


def load_json(path):
    with open(path) as f:
        return json.load(f)


def percentile(values, p):
    if not values:
        return float("nan")
    s = sorted(values)
    return s[min(int(len(s) * p), len(s) - 1)]


def build_latency_matrix(sim, N):
    lats = np.full(N, np.nan, dtype=np.float64)
    lons = np.full(N, np.nan, dtype=np.float64)
    for i, p in enumerate(sim["peers"]):
        g = p.get("geo")
        if g and isinstance(g.get("lat"), (int, float)) and isinstance(g.get("lon"), (int, float)):
            lats[i] = g["lat"]
            lons[i] = g["lon"]
    valid = ~np.isnan(lats)
    lat_r = np.radians(np.nan_to_num(lats, nan=0.0))
    lon_r = np.radians(np.nan_to_num(lons, nan=0.0))
    dp = lat_r[None, :] - lat_r[:, None]
    dl = lon_r[None, :] - lon_r[:, None]
    a = np.sin(dp / 2) ** 2 + np.cos(lat_r[:, None]) * np.cos(lat_r[None, :]) * np.sin(dl / 2) ** 2
    km = 2 * 6371.0088 * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))
    L = (ALPHA_MS + BETA_MS_PER_KM * km).astype(np.float32)
    pair_valid = valid[:, None] & valid[None, :]
    L = np.where(pair_valid, L, np.float32(FALLBACK_MS))
    np.fill_diagonal(L, 0.0)
    return L, valid.sum()


def compute_decode_per_source(L, peers, source, k_needed, tx_ms):
    """Returns decode time per receiver r in [0..N-1]. r == source → 0.

    peers: honest peers excluding source. Pieces are assigned only to these.
    """
    N = L.shape[0]
    L_sd = L[source, peers]   # [K_total]
    L_dr = L[peers]           # [K_total, N]
    arrivals = L_sd[:, None] + L_dr + np.float32(2 * tx_ms)  # 2-hop

    # own-piece correction: where peers[i] == r, it's 1-hop from S to R
    own_cols = peers
    own_rows = np.arange(len(peers))
    arrivals[own_rows, own_cols] = L[source, own_cols] + np.float32(tx_ms)

    partitioned = np.partition(arrivals, k_needed - 1, axis=0)
    decode = partitioned[k_needed - 1]
    decode[source] = 0.0
    return decode


def main():
    graph_path = sys.argv[1] if len(sys.argv) > 1 else "/Users/arseny30/code/ton/broadcast-graph.json"
    sim_path = sys.argv[2] if len(sys.argv) > 2 else "/Users/arseny30/code/ton/public-overlay/broadcast-sim-packed/data/overlay-peers.sim.json"
    n_src_list = [int(x) for x in (sys.argv[3] if len(sys.argv) > 3 else "1,10,23,100,400").split(",")]
    seeds = int(sys.argv[4]) if len(sys.argv) > 4 else 30

    graph = load_json(graph_path)
    sim = load_json(sim_path)
    N = graph["node_count"]
    honest = np.array([i for i, n in enumerate(graph["nodes"]) if not n.get("leech")], dtype=np.int64)
    is_leech = np.array([bool(n.get("leech")) for n in graph["nodes"]], dtype=bool)

    L, geo_known = build_latency_matrix(sim, N)
    K_total = len(honest) - 1  # pieces assigned only to honest peers (leech excluded)
    K_needed = K_total // 2
    print(f"graph: N={N} ({len(honest)} honest, {N - len(honest)} leech)")
    print(f"geo coverage: {geo_known}/{N}; fallback={FALLBACK_MS}ms; bw={BW/1024/1024:.0f}MB/s")
    print(f"L = {ALPHA_MS}ms + {BETA_MS_PER_KM}ms/km * haversine")
    print(f"FEC: K_total={K_total}, K_needed={K_needed}")
    print()

    for body_label, body_bytes in [("100KB", 100 * 1024), ("1MB", 1024 * 1024)]:
        part_size = max(1, (body_bytes + K_needed - 1) // K_needed)
        tx_ms = (part_size / BW) * 1000.0
        print(f"=== body={body_label}, part_size={part_size}B, tx/hop={tx_ms:.4f}ms ===")

        # Precompute decode matrix [|honest|, N]: row hi = decode time for each receiver
        # when source = honest[hi]. Delegates = honest \ {source}.
        decode_per_source = np.empty((len(honest), N), dtype=np.float32)
        for hi, s in enumerate(honest):
            peers = np.array([h for h in honest if h != s], dtype=np.int64)
            decode_per_source[hi] = compute_decode_per_source(L, peers, int(s), K_needed, tx_ms)

        print(f"  {'sources':>8}  {'p50':>6}  {'p90':>6}  {'p95':>6}  {'p99':>6}  {'max':>6}  {'unreach':>7}")
        for n_src in n_src_list:
            runs = []
            for seed in range(1, seeds + 1):
                rng = random.Random(seed)
                src_indices = rng.sample(range(len(honest)), min(n_src, len(honest)))
                src_indices = np.array(src_indices, dtype=np.int64)
                src_nodes = honest[src_indices]
                # decode for each honest receiver = min across selected sources
                # rows are sources, columns are receivers; pick columns = honest receivers (excluding sources)
                sub = decode_per_source[src_indices][:, honest]  # [n_src, |honest|]
                # mask out self-source decode (set to inf so it's not picked as min)
                src_mask = np.isin(honest, src_nodes)  # [|honest|]
                # actually for non-source receivers we want min. For source receivers we'll exclude.
                best = sub.min(axis=0)  # [|honest|]
                non_src = ~src_mask
                decode_honest = best[non_src]
                finite = decode_honest[np.isfinite(decode_honest)]
                runs.append({
                    "p50": float(np.percentile(finite, 50)) if finite.size else float("inf"),
                    "p90": float(np.percentile(finite, 90)) if finite.size else float("inf"),
                    "p95": float(np.percentile(finite, 95)) if finite.size else float("inf"),
                    "p99": float(np.percentile(finite, 99)) if finite.size else float("inf"),
                    "max": float(finite.max()) if finite.size else float("inf"),
                    "unreach": int(decode_honest.size - finite.size),
                })
            avg = {k: statistics.mean(r[k] for r in runs) for k in runs[0]}
            print(f"  {n_src:>8}  {avg['p50']:>6.1f}  {avg['p90']:>6.1f}  {avg['p95']:>6.1f}  "
                  f"{avg['p99']:>6.1f}  {avg['max']:>6.1f}  {avg['unreach']:>7.1f}")
        print()


if __name__ == "__main__":
    main()
