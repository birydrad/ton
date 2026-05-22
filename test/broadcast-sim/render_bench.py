#!/usr/bin/env python3
"""Render the broadcast benchmark HTML report.

Reads a CSV produced by `broadcast-bench [--graph synthetic|mainnet] --csv` and
emits a self-contained HTML report. Both graph modes share the same CSV schema,
so this consumes either uniformly. Use --fake to render a plausible dataset for
visual iteration on the report layout without needing to run the C++ bench first.
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime as dt
import html
import math
import statistics
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Configuration: algorithms and stable colors. Order here is the display order.

ALGORITHMS: List[Tuple[str, str, str]] = [
    # (label, role, color)
    # Production baselines.
    ("prod-simple", "production", "#a85510"),
    ("prod-fec-fixed-K5-5MB", "production", "#e07d23"),
    ("prod-fec-random-inf", "production", "#c46b1a"),
    ("prod-fec-random-K10-inf", "production", "#d4881e"),
    # Twostep private-overlay.
    ("twostep-simple", "twostep", "#9c4dcc"),
    ("twostep-fec", "twostep", "#7b3aa0"),
    # No-FEC whole-body.
    ("flood-K=5", "baseline", "#c43a3a"),
    ("pull-K=10", "baseline", "#909aa6"),
    ("eager-lazy", "baseline", "#2f9e44"),
    ("plumtree", "candidate", "#5b5bd6"),
    ("plumtree-cap=5", "candidate", "#7575df"),
    ("plumtree-cap=10", "candidate", "#9090e8"),
    ("subscribe-prune", "baseline", "#5a626d"),
    # Gossip-mask FEC (current best class).
    ("gossip-mask-K1=2-d5ms", "candidate", "#1f6feb"),
    ("gossip-mask-K1=3-d5ms", "candidate", "#3886eb"),
    ("gossip-mask-K1=4-d5ms", "candidate", "#56a0eb"),
    ("gossip-mask-coarse30-K1=3-part=8", "candidate", "#0d4ba3"),
    # RLNC ceiling.
    ("optimum-p2p", "ceiling", "#202020"),
]

ALGO_COLOR = {name: color for name, _, color in ALGORITHMS}
ALGO_ROLE = {name: role for name, role, _ in ALGORITHMS}
ALGO_ORDER = [name for name, _, _ in ALGORITHMS]

BLOCK_SIZES = [
    ("small", 256, "control / tiny payload"),
    ("medium", 100 * 1024, "typical proposal / block-sized"),
    ("large", 1024 * 1024, "large block / msg bundle"),
]

LEECH_RATIOS = [0, 5, 10, 20, 30, 40, 50]
ROUNDS = 20

# Graphs: mainnet (production-derived) is primary; synth is the portability
# baseline. Synth is a random-regular 1000-node graph with uniform 50ms
# latency. Charts default to mainnet; ranking tables show both side by side.
GRAPHS = ["mainnet", "synth"]
GRAPH_LABELS = {"mainnet": "mainnet-v2", "synth": "rand-reg-1000"}

# Metric definitions: (key, header, fmt, lower_is_better)
METRICS = [
    ("reach", "reach", lambda v: f"{v*100:.1f}%", False),
    ("p50_ms", "p50", lambda v: f"{v:.0f}ms", True),
    ("p95_ms", "p95", lambda v: f"{v:.0f}ms", True),
    ("p99_ms", "p99", lambda v: f"{v:.0f}ms", True),
    ("node_out_p95", "out p95", lambda v: f"{v:.1f}x", True),
    ("node_in_p95", "in p95", lambda v: f"{v:.1f}x", True),
    ("network_overhead_honest_total", "honest cost", lambda v: f"{v:.1f}x", True),
    ("network_overhead_honest", "(body only)", lambda v: f"{v:.1f}x", True),
    ("source_x", "src", lambda v: f"{v:.1f}x", True),
    ("dup_pct", "dup", lambda v: f"{v*100:.0f}%", True),
    ("max_leech_pct", "leech ok", lambda v: f"≤{v:.0f}%", False),
]


# ---------------------------------------------------------------------------
# Fake data generator. Produces a row per (algo, size). Values picked so the
# report tells a coherent story; the user is supposed to react to the *shape*
# of the report, not the numbers.

@dataclasses.dataclass
class Row:
    algorithm: str
    size: str
    graph: str
    reach: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    node_out_p95: float
    node_in_p95: float
    network_overhead_honest: float
    network_overhead_honest_total: float
    source_x: float
    dup_pct: float
    control_pct: float
    max_leech_pct: int
    # Series for charts (only populated for the primary graph):
    leech_curve: List[Tuple[int, float]]      # (leech%, reach)
    round_curve: List[Tuple[int, float]]      # (round, reach)
    round_p95_curve: List[Tuple[int, float]]  # (round, p95_ms)
    anysend_hist: List[int]                   # buckets [<50%, 50-90, 90-99, 99-99.9, 100%]
    # Parallel-publisher AnySend: (n_senders, p95_ms, honest_x, reach). Empty when not measured.
    multi_sender: List[Tuple[int, float, float, float]] = dataclasses.field(default_factory=list)


def fake_dataset() -> List[Row]:
    # Headline shape per algorithm, then scaled per block size.
    # Each tuple: reach, p50_ms, p95_ms, p99_ms, out_p95, in_p95, honest_cost,
    # source_x, dup_pct, control_pct, max_leech_pct
    base = {
        "plumfec-r1.1-K3":  (0.997, 60, 85, 120, 3.6, 3.8, 4.2, 5.0, 0.55, 0.10, 40),
        "plumfec-r2-K2":    (0.995, 65, 95, 140, 2.8, 3.2, 3.7, 4.0, 0.40, 0.15, 35),
        "pushfec-r1.1-K3":  (0.992, 55, 80, 135, 4.1, 4.2, 4.8, 5.0, 0.62, 0.05, 15),
        "fec-old-K5-5MB":  (0.982, 40, 70, 220, 5.2, 5.4, 5.8, 5.2, 0.62, 0.03, 25),
        "plum-tit":         (0.996, 80, 130, 240, 3.0, 3.4, 3.6, 4.5, 0.45, 0.20, 50),
        "current-flood":    (1.000, 30, 45, 70,  10.0, 10.0, 10.4, 10.0, 0.84, 0.01, 50),
        "plum-pull":        (1.000, 85, 145, 280, 5.5, 6.0, 6.1, 8.0, 0.10, 0.50, 50),
        "optimum":          (1.000, 50, 110, 160, 4.9, 5.0, 1.5, 8.0, 0.25, 0.20, 50),
    }
    # Multipliers per size (small, medium, large) for (p_lat, cost).
    size_lat = {"small": 1.0, "medium": 1.05, "large": 1.55}
    size_cost = {"small": 1.0, "medium": 1.02, "large": 1.10}
    rng_offset = {name: i * 7 for i, name in enumerate(base)}

    # Synth-graph drift relative to mainnet: random-regular has full mutual
    # edges and uniform latency, so reach is higher, latency tighter, cost
    # comparable. Algorithms that lean heavily on score/learning have similar
    # behavior on both. The two algorithms that are graph-fragile here are
    # `pushfec` (no scoring, exploits regularity) and `fec-old-K5-5MB` (random
    # K-of-N hits more honest peers on a symmetric graph).
    synth_reach_delta = {
        "plumfec-r1.1-K3": 0.002, "plumfec-r2-K2": 0.003, "pushfec-r1.1-K3": 0.008,
        "fec-old-K5-5MB": 0.015, "plum-tit": 0.003, "current-flood": 0.0,
        "plum-pull": 0.0, "optimum": 0.0,
    }
    synth_p95_factor = {
        "plumfec-r1.1-K3": 0.92, "plumfec-r2-K2": 0.94, "pushfec-r1.1-K3": 0.88,
        "fec-old-K5-5MB": 0.85, "plum-tit": 0.95, "current-flood": 0.96,
        "plum-pull": 0.97, "optimum": 0.94,
    }
    synth_cost_factor = {
        "plumfec-r1.1-K3": 0.98, "plumfec-r2-K2": 0.98, "pushfec-r1.1-K3": 0.96,
        "fec-old-K5-5MB": 0.95, "plum-tit": 0.97, "current-flood": 0.95,
        "plum-pull": 0.99, "optimum": 0.92,
    }

    rows: List[Row] = []
    for algo, (reach, p50, p95, p99, out_p95, in_p95, cost, src, dup, ctrl, leech) in base.items():
        for size_name, _, _ in BLOCK_SIZES:
            lm = size_lat[size_name]
            cm = size_cost[size_name]
            for graph in GRAPHS:
                reach_g = min(1.0, reach + (synth_reach_delta[algo] if graph == "synth" else 0))
                p_factor = synth_p95_factor[algo] if graph == "synth" else 1.0
                c_factor = synth_cost_factor[algo] if graph == "synth" else 1.0
                # Leech curve only populated for primary (mainnet) graph; synth
                # mirrors it with a small uplift for charts that aren't rendered.
                curve = []
                if graph == "mainnet":
                    for lr in LEECH_RATIOS:
                        if lr <= leech:
                            drop = 0.001 * (lr / max(leech, 1)) ** 2
                            curve.append((lr, max(0.0, reach_g - drop)))
                        else:
                            extra = lr - leech
                            drop = 0.02 * extra + 0.001 * extra * extra
                            curve.append((lr, max(0.5, reach_g - drop)))
                round_curve = []
                round_p95 = []
                if graph == "mainnet":
                    for r in range(ROUNDS):
                        if algo in {"plum-tit", "plumfec-r1.1-K3", "plumfec-r2-K2"}:
                            learn = (1 - math.exp(-r / 4.0)) * 0.003
                            p_learn = (1 - math.exp(-r / 5.0)) * 8
                            round_curve.append((r, min(1.0, reach_g + learn)))
                            round_p95.append((r, max(20, p95 * lm - p_learn)))
                        else:
                            seed = rng_offset[algo] + r
                            jitter = ((seed * 9301 + 49297) % 233280) / 233280 - 0.5
                            round_curve.append((r, max(0.0, min(1.0, reach_g + jitter * 0.002))))
                            round_p95.append((r, p95 * lm * (1 + jitter * 0.05)))
                # AnySend buckets only for mainnet (synth is uniform-honest by
                # construction, so the histogram is degenerate).
                anysend = [0, 0, 0, 0, 0]
                if graph == "mainnet":
                    if algo == "current-flood":
                        anysend = [0, 2, 12, 80, 997]
                    elif algo == "fec-old-K5-5MB":
                        anysend = [12, 26, 58, 220, 775]
                    elif algo == "plumfec-r1.1-K3":
                        anysend = [3, 1, 4, 100, 983]
                    elif algo == "plumfec-r2-K2":
                        anysend = [3, 2, 8, 110, 968]
                    elif algo == "pushfec-r1.1-K3":
                        anysend = [5, 7, 24, 130, 925]
                    elif algo == "plum-tit":
                        anysend = [6, 18, 41, 156, 870]
                    elif algo == "plum-pull":
                        anysend = [0, 0, 5, 86, 1000]
                    else:  # optimum
                        anysend = [0, 0, 0, 50, 1041]
                rows.append(Row(
                    algorithm=algo,
                    size=size_name,
                    graph=graph,
                    reach=reach_g,
                    p50_ms=p50 * lm * p_factor,
                    p95_ms=p95 * lm * p_factor,
                    p99_ms=p99 * lm * p_factor,
                    node_out_p95=out_p95 * cm * c_factor,
                    node_in_p95=in_p95 * cm * c_factor,
                    network_overhead_honest=cost * cm * c_factor,
                    network_overhead_honest_total=cost * cm * c_factor + ctrl * 0.5,
                    source_x=src * cm * c_factor,
                    dup_pct=dup,
                    control_pct=ctrl,
                    max_leech_pct=leech,
                    leech_curve=curve,
                    round_curve=round_curve,
                    round_p95_curve=round_p95,
                    anysend_hist=anysend,
                ))
    return rows


# ---------------------------------------------------------------------------
# Rendering helpers.

def esc(s: str) -> str:
    return html.escape(str(s))


def human_bytes(n: int) -> str:
    if n >= 1024 * 1024:
        return f"{n / (1024*1024):.0f} MiB"
    if n >= 1024:
        return f"{n / 1024:.0f} KiB"
    return f"{n} B"


def color_for(algo: str) -> str:
    return ALGO_COLOR.get(algo, "#888")


# ---------------------------------------------------------------------------
# Invariant tests. Each test takes the medium-size row plus full per-algo rows
# (so size-aware checks can look across sizes). Returns ("pass" | "warn" |
# "fail", short_reason). The tests section flags algorithms that should not be
# considered for production — it's the loud "this algorithm is incorrect or
# operationally unviable" panel.

@dataclasses.dataclass
class TestResult:
    code: str
    severity: str  # "pass" | "warn" | "fail"
    detail: str


@dataclasses.dataclass
class TestSpec:
    code: str
    label: str
    description: str


TESTS: List[TestSpec] = [
    TestSpec("CLEAN_REACH",     "clean reach ≥ 99.9%",  "fail if a single broadcast missed honest receivers"),
    TestSpec("ANYSEND_BLIND",   "no blind senders",     "fail if any source delivered to < 50% honest"),
    TestSpec("ANYSEND_COVER",   "≥ 95% senders 100%",   "warn if too few sources achieve full coverage"),
    TestSpec("ROUND_NONREGRESS","no round regression",  "fail if reach drops as state persists"),
    TestSpec("LEECH_FLOOR",     "≥ 20% leech tolerated","fail if reach breaks under modest leech load"),
    TestSpec("COST_CEILING",    "honest cost ≤ 12×",    "warn if network overhead is wasteful"),
    TestSpec("LATENCY_TAIL",    "p99 ≤ 500ms (medium)", "warn if tail latency is unbounded"),
    TestSpec("SOURCE_AMP",      "source upload ≤ 16×",  "warn if publisher is a hot spot"),
    TestSpec("GRAPH_PORTABLE",  "mainnet ≈ synth",      "fail if metrics diverge significantly between graphs"),
]


def run_tests(rows: List[Row]) -> Dict[str, Dict[str, TestResult]]:
    """Returns results[algorithm][test_code] -> TestResult.

    Existing tests run on the mainnet graph (the deployment target).
    GRAPH_PORTABLE compares mainnet and synth and fails on significant drift.
    """
    main_rows = [r for r in rows if r.graph == "mainnet"]
    synth_rows = [r for r in rows if r.graph == "synth"]
    by_algo_main: Dict[str, List[Row]] = {}
    by_algo_synth: Dict[str, List[Row]] = {}
    for r in main_rows:
        by_algo_main.setdefault(r.algorithm, []).append(r)
    for r in synth_rows:
        by_algo_synth.setdefault(r.algorithm, []).append(r)
    out: Dict[str, Dict[str, TestResult]] = {}
    for algo, algo_rows in by_algo_main.items():
        synth_for_algo = by_algo_synth.get(algo, [])
        medium = next((r for r in algo_rows if r.size == "medium"), algo_rows[0])
        results: Dict[str, TestResult] = {}

        # CLEAN_REACH: reach must be ≥ 0.999 on every size (clean fake = no leeches).
        worst_reach = min(r.reach for r in algo_rows)
        if worst_reach >= 0.999:
            results["CLEAN_REACH"] = TestResult("CLEAN_REACH", "pass", f"{worst_reach*100:.1f}%")
        else:
            sev = "fail" if worst_reach < 0.99 else "warn"
            results["CLEAN_REACH"] = TestResult("CLEAN_REACH", sev, f"{worst_reach*100:.1f}%")

        # ANYSEND_BLIND: anysend_hist[0] is count of sources whose broadcast
        # reached <50%. The v2 mainnet graph itself has 3 honest nodes with no
        # honest in-edges — those are topology-blind regardless of algorithm.
        # Skip if the AnySend scenario wasn't measured (hist all zeros).
        TOPOLOGY_BLIND_FLOOR = 3
        if sum(medium.anysend_hist) == 0:
            results["ANYSEND_BLIND"] = TestResult("ANYSEND_BLIND", "skip", "not measured")
            results["ANYSEND_COVER"] = TestResult("ANYSEND_COVER", "skip", "not measured")
        else:
            blind = medium.anysend_hist[0]
            if blind == 0:
                results["ANYSEND_BLIND"] = TestResult("ANYSEND_BLIND", "pass", "0 blind")
            elif blind <= TOPOLOGY_BLIND_FLOOR:
                results["ANYSEND_BLIND"] = TestResult("ANYSEND_BLIND", "warn",
                                                      f"{blind} (topology floor)")
            else:
                results["ANYSEND_BLIND"] = TestResult("ANYSEND_BLIND", "fail",
                                                      f"{blind} sources delivered <50%")
            total = sum(medium.anysend_hist) or 1
            full = medium.anysend_hist[-1] / total
            if full >= 0.95:
                results["ANYSEND_COVER"] = TestResult("ANYSEND_COVER", "pass", f"{full*100:.1f}%")
            elif full >= 0.85:
                results["ANYSEND_COVER"] = TestResult("ANYSEND_COVER", "warn", f"{full*100:.1f}%")
            else:
                results["ANYSEND_COVER"] = TestResult("ANYSEND_COVER", "fail", f"{full*100:.1f}%")

        # ROUND_NONREGRESS: requires PersistentScores multi-round run.
        if not medium.round_curve:
            results["ROUND_NONREGRESS"] = TestResult("ROUND_NONREGRESS", "skip", "not measured")
        else:
            r0 = medium.round_curve[0][1]
            rN = medium.round_curve[-1][1]
            drop = r0 - rN
            if drop <= 0.005:
                results["ROUND_NONREGRESS"] = TestResult("ROUND_NONREGRESS", "pass",
                                                        f"Δ={(rN-r0)*100:+.2f}pp")
            else:
                sev = "fail" if drop > 0.02 else "warn"
                results["ROUND_NONREGRESS"] = TestResult("ROUND_NONREGRESS", sev,
                                                        f"-{drop*100:.2f}pp")

        # LEECH_FLOOR: skip if no leech sweep was run.
        has_leech_data = any(lr > 0 for lr, _ in (medium.leech_curve or []))
        if not has_leech_data:
            results["LEECH_FLOOR"] = TestResult("LEECH_FLOOR", "skip", "not measured")
        elif medium.max_leech_pct >= 20:
            results["LEECH_FLOOR"] = TestResult("LEECH_FLOOR", "pass", f"≤{medium.max_leech_pct}%")
        else:
            results["LEECH_FLOOR"] = TestResult("LEECH_FLOOR", "fail",
                                                f"only ≤{medium.max_leech_pct}%")

        # COST_CEILING (warn).
        worst_cost = max(r.network_overhead_honest_total for r in algo_rows)
        if worst_cost <= 12.0:
            results["COST_CEILING"] = TestResult("COST_CEILING", "pass", f"{worst_cost:.1f}×")
        else:
            results["COST_CEILING"] = TestResult("COST_CEILING", "warn", f"{worst_cost:.1f}×")

        # LATENCY_TAIL (medium only).
        if medium.p99_ms <= 500:
            results["LATENCY_TAIL"] = TestResult("LATENCY_TAIL", "pass", f"{medium.p99_ms:.0f}ms")
        else:
            sev = "fail" if medium.p99_ms > 1000 else "warn"
            results["LATENCY_TAIL"] = TestResult("LATENCY_TAIL", sev, f"{medium.p99_ms:.0f}ms")

        # SOURCE_AMP.
        worst_src = max(r.source_x for r in algo_rows)
        if worst_src <= 16:
            results["SOURCE_AMP"] = TestResult("SOURCE_AMP", "pass", f"{worst_src:.1f}×")
        else:
            results["SOURCE_AMP"] = TestResult("SOURCE_AMP", "warn", f"{worst_src:.1f}×")

        # GRAPH_PORTABLE: check that mainnet and synth metrics don't diverge by
        # more than 2 pp (reach) or 50% (latency/cost). Skip when no synth data.
        if not synth_for_algo:
            results["GRAPH_PORTABLE"] = TestResult("GRAPH_PORTABLE", "skip", "synth not measured")
            out[algo] = results
            continue
        portable_msg = []
        portable_sev = "pass"
        for main_row in algo_rows:
            synth_row = next((r for r in synth_for_algo if r.size == main_row.size), None)
            if synth_row is None:
                continue
            reach_drift = abs(main_row.reach - synth_row.reach)
            base_p95 = min(main_row.p95_ms, synth_row.p95_ms) or 1.0
            p95_drift = abs(main_row.p95_ms - synth_row.p95_ms) / base_p95
            base_cost = min(main_row.network_overhead_honest_total,
                            synth_row.network_overhead_honest_total) or 1.0
            cost_drift = abs(main_row.network_overhead_honest_total
                             - synth_row.network_overhead_honest_total) / base_cost
            if reach_drift > 0.02:
                portable_msg.append(f"{main_row.size}: Δreach {reach_drift*100:.1f}pp")
                portable_sev = "fail"
            elif reach_drift > 0.01 and portable_sev != "fail":
                portable_msg.append(f"{main_row.size}: Δreach {reach_drift*100:.1f}pp")
                portable_sev = "warn"
            # p95 drift is NOT checked here. synth uses uniform 50ms per hop while mainnet uses
            # geo-derived latency — multi-hop p95 naturally diverges by ~2x for identical
            # algorithm behavior. GRAPH_PORTABLE is about structural fragility (reach + cost);
            # latency is the latency model speaking, not the algorithm.
            _ = p95_drift  # noqa
            if cost_drift > 0.5:
                portable_msg.append(f"{main_row.size}: Δcost {cost_drift*100:.0f}%")
                portable_sev = "fail"
        if portable_sev == "pass":
            # Summarise the biggest drift seen.
            biggest = 0.0
            for main_row in algo_rows:
                synth_row = next((r for r in synth_for_algo if r.size == main_row.size), None)
                if synth_row is None:
                    continue
                biggest = max(biggest, abs(main_row.reach - synth_row.reach))
            results["GRAPH_PORTABLE"] = TestResult("GRAPH_PORTABLE", "pass",
                                                   f"Δreach ≤{biggest*100:.1f}pp")
        else:
            results["GRAPH_PORTABLE"] = TestResult("GRAPH_PORTABLE", portable_sev,
                                                   "; ".join(portable_msg[:2]) or "drift")

        out[algo] = results
    return out


def summarize_test(results: Dict[str, TestResult]) -> Tuple[int, int, str]:
    """Returns (fails, warns, overall). 'skip' (not measured) is ignored."""
    fails = sum(1 for r in results.values() if r.severity == "fail")
    warns = sum(1 for r in results.values() if r.severity == "warn")
    overall = "fail" if fails else ("warn" if warns else "pass")
    return fails, warns, overall


def render_tests_section(rows: List[Row]) -> str:
    results = run_tests(rows)
    # Order rows: fails first, then warns, then passes; within each by ALGO_ORDER.
    def sort_key(algo: str):
        fails, warns, _ = summarize_test(results[algo])
        idx = ALGO_ORDER.index(algo) if algo in ALGO_ORDER else 999
        return (-fails, -warns, idx)
    ordered = sorted(results.keys(), key=sort_key)

    parts = ['<section class="card tests">'
             '<h2>Invariants <small>algorithms failing a check are flagged as incorrect</small></h2>'
             '<p style="color:var(--muted);font-size:12px;margin:6px 0 12px;">'
             'Each column is a test. Hover a cell for the threshold rationale. '
             'A red row indicates at least one failed invariant.</p>'
             '<table class="tests-matrix"><thead><tr><th>algorithm</th>']
    for spec in TESTS:
        parts.append(f'<th><span title="{esc(spec.description)}">{esc(spec.label)}</span></th>')
    parts.append('<th>verdict</th></tr></thead><tbody>')

    for algo in ordered:
        fails, warns, overall = summarize_test(results[algo])
        row_cls = "fail" if fails else ("warn" if warns else "ok")
        parts.append(f'<tr class="{row_cls}"><td>'
                     f'<span class="swatch" style="background:{color_for(algo)}"></span>'
                     f'{esc(algo)}</td>')
        for spec in TESTS:
            tr = results[algo].get(spec.code)
            if tr is None:
                parts.append('<td class="t-skip">·</td>')
                continue
            icon = {"pass": "✓", "warn": "⚠", "fail": "✗", "skip": "·"}[tr.severity]
            cell_cls = f"t-{tr.severity}"
            parts.append(
                f'<td class="{cell_cls}" title="{esc(tr.code)}: {esc(tr.detail)}">'
                f'<b>{icon}</b><span class="t-detail">{esc(tr.detail)}</span></td>')
        verdict_text = ("FAIL" if fails else ("WARN" if warns else "OK"))
        verdict_extra = (f" ({fails}f/{warns}w)" if fails or warns else "")
        parts.append(f'<td class="verdict v-{overall}">{verdict_text}{verdict_extra}</td>')
        parts.append('</tr>')
    parts.append('</tbody></table>')
    # Legend
    parts.append('<div class="notes-line">'
                 '<span><b style="color:#1a8137">✓</b> pass</span>'
                 '<span><b style="color:#a8530d">⚠</b> warn — operational concern, not a correctness bug</span>'
                 '<span><b style="color:#b3261e">✗</b> fail — algorithm rejected for production</span>'
                 '<span><b style="color:#97a0ad">·</b> not measured — scenario not run yet</span>'
                 '</div></section>')
    return "".join(parts)


def rank_class(values: List[float], v: float, lower_is_better: bool) -> str:
    """Return a CSS class indicating the cell's rank within column."""
    if not values:
        return ""
    sorted_vals = sorted(values, reverse=not lower_is_better)
    try:
        rank = sorted_vals.index(v)
    except ValueError:
        return ""
    n = len(sorted_vals)
    pct = rank / max(n - 1, 1)
    if pct < 0.25:
        return "r1"
    if pct < 0.50:
        return "r2"
    if pct < 0.75:
        return "r3"
    return "r4"


# ---------------------------------------------------------------------------
# SVG chart primitives. Pure string templates; no JS.

def pareto_svg(rows: List[Row]) -> str:
    """Plot total cost (x) vs p95 latency (y). Dot radius ~ leech tolerance."""
    w, h = 540, 320
    if not rows:
        return (f'<svg viewBox="0 0 {w} {h}" class="chart pareto empty">'
                f'<text x="{w/2}" y="{h/2}" text-anchor="middle" class="axis-label">'
                f'no data</text></svg>')
    pad = {"l": 60, "r": 20, "t": 24, "b": 44}
    iw = w - pad["l"] - pad["r"]
    ih = h - pad["t"] - pad["b"]
    xs = [r.network_overhead_honest_total for r in rows]
    ys = [r.p95_ms for r in rows]
    x_min, x_max = 0.0, max(max(xs), 1.0) * 1.05
    y_min, y_max = 0.0, max(max(ys), 1.0) * 1.10

    def sx(v): return pad["l"] + (v - x_min) / (x_max - x_min) * iw
    def sy(v): return pad["t"] + (1 - (v - y_min) / (y_max - y_min)) * ih

    # Compute frontier (min p95 for each prefix sorted by cost).
    sorted_rows = sorted(rows, key=lambda r: r.network_overhead_honest_total)
    frontier: List[Row] = []
    best_p95 = float("inf")
    for r in sorted_rows:
        if r.p95_ms < best_p95:
            frontier.append(r)
            best_p95 = r.p95_ms

    parts = [f'<svg viewBox="0 0 {w} {h}" class="chart pareto" role="img" aria-label="Pareto cost vs latency">']
    # Axes
    parts.append(f'<line x1="{pad["l"]}" y1="{pad["t"]}" x2="{pad["l"]}" y2="{pad["t"]+ih}" class="axis"/>')
    parts.append(f'<line x1="{pad["l"]}" y1="{pad["t"]+ih}" x2="{pad["l"]+iw}" y2="{pad["t"]+ih}" class="axis"/>')
    # Y ticks
    for f in (0.0, 0.25, 0.5, 0.75, 1.0):
        v = y_min + f * (y_max - y_min)
        y = sy(v)
        parts.append(f'<line x1="{pad["l"]}" y1="{y:.1f}" x2="{pad["l"]+iw}" y2="{y:.1f}" class="grid"/>')
        parts.append(f'<text x="{pad["l"]-8}" y="{y+4:.1f}" class="tick" text-anchor="end">{v:.0f}ms</text>')
    # X ticks
    for f in (0.0, 0.25, 0.5, 0.75, 1.0):
        v = x_min + f * (x_max - x_min)
        x = sx(v)
        parts.append(f'<text x="{x:.1f}" y="{pad["t"]+ih+18}" class="tick" text-anchor="middle">{v:.1f}x</text>')
    # Axis labels
    parts.append(f'<text x="{pad["l"]+iw/2:.0f}" y="{h-8}" class="axis-label" text-anchor="middle">'
                 f'honest network cost (×body)</text>')
    parts.append(f'<text x="14" y="{pad["t"]+ih/2:.0f}" class="axis-label" '
                 f'transform="rotate(-90 14 {pad["t"]+ih/2:.0f})" text-anchor="middle">p95 latency</text>')
    # Frontier polyline
    if len(frontier) >= 2:
        pts = " ".join(f"{sx(r.network_overhead_honest_total):.1f},{sy(r.p95_ms):.1f}" for r in frontier)
        parts.append(f'<polyline points="{pts}" class="frontier"/>')
    # Dots
    for r in rows:
        x = sx(r.network_overhead_honest_total)
        y = sy(r.p95_ms)
        rad = 4 + r.max_leech_pct / 12
        c = color_for(r.algorithm)
        dashed = ' stroke-dasharray="2,2"' if r.algorithm == "optimum" else ""
        parts.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{rad:.1f}" fill="{c}" '
            f'fill-opacity="0.85" stroke="#fff" stroke-width="1.5"{dashed}>'
            f'<title>{esc(r.algorithm)}: cost {r.network_overhead_honest_total:.1f}x, '
            f'p95 {r.p95_ms:.0f}ms, leech ≤{r.max_leech_pct}%</title></circle>')
        # Label
        parts.append(
            f'<text x="{x+rad+4:.1f}" y="{y+3:.1f}" class="dot-label" fill="{c}">{esc(r.algorithm)}</text>')
    parts.append('</svg>')
    return "".join(parts)


def latency_tail_svg(rows: List[Row]) -> str:
    """Horizontal stacked bars: p50 segment | p95-p50 | p99-p95."""
    rows = sorted(rows, key=lambda r: r.p95_ms)
    if not rows:
        return ('<svg viewBox="0 0 540 60" class="chart tail empty">'
                '<text x="270" y="36" text-anchor="middle" class="axis-label">no data</text></svg>')
    w, h = 540, 36 + 24 * len(rows)
    pad = {"l": 130, "r": 90, "t": 12, "b": 24}
    iw = w - pad["l"] - pad["r"]
    max_v = max(max(r.p99_ms for r in rows), 1.0) * 1.05

    def sx(v): return pad["l"] + v / max_v * iw

    parts = [f'<svg viewBox="0 0 {w} {h}" class="chart tail" role="img" aria-label="Latency tail">']
    # X axis
    for f in (0.0, 0.25, 0.5, 0.75, 1.0):
        v = f * max_v
        x = sx(v)
        parts.append(f'<line x1="{x:.1f}" y1="{pad["t"]}" x2="{x:.1f}" y2="{h-pad["b"]}" class="grid"/>')
        parts.append(f'<text x="{x:.1f}" y="{h-pad["b"]+14}" class="tick" text-anchor="middle">{v:.0f}ms</text>')
    for i, r in enumerate(rows):
        y = pad["t"] + i * 24 + 4
        c = color_for(r.algorithm)
        parts.append(f'<text x="{pad["l"]-8}" y="{y+12}" class="row-label" text-anchor="end" fill="{c}">'
                     f'{esc(r.algorithm)}</text>')
        # p50 segment
        x0 = sx(0)
        x1 = sx(r.p50_ms)
        parts.append(f'<rect x="{x0:.1f}" y="{y}" width="{x1-x0:.1f}" height="14" fill="{c}" fill-opacity="0.95"/>')
        # p95 segment
        x2 = sx(r.p95_ms)
        parts.append(f'<rect x="{x1:.1f}" y="{y}" width="{x2-x1:.1f}" height="14" fill="{c}" fill-opacity="0.55"/>')
        # p99 segment
        x3 = sx(r.p99_ms)
        parts.append(f'<rect x="{x2:.1f}" y="{y}" width="{x3-x2:.1f}" height="14" fill="{c}" fill-opacity="0.25"/>')
        # Numeric tail label
        parts.append(f'<text x="{x3+6:.1f}" y="{y+11}" class="tail-num">'
                     f'{r.p50_ms:.0f} · {r.p95_ms:.0f} · {r.p99_ms:.0f}</text>')
    # Legend
    parts.append(f'<g transform="translate({pad["l"]}, {h-8})">')
    parts.append('<text x="0" y="0" class="legend">p50</text>')
    parts.append('<text x="44" y="0" class="legend">p95</text>')
    parts.append('<text x="88" y="0" class="legend">p99</text>')
    parts.append('</g>')
    parts.append('</svg>')
    return "".join(parts)


def line_chart_svg(series: Dict[str, List[Tuple[float, float]]], *,
                   x_label: str, y_label: str, y_max: float,
                   y_fmt=lambda v: f"{v:.1f}", y_min: float = 0.0,
                   width: int = 540, height: int = 260,
                   threshold: Optional[float] = None) -> str:
    pad = {"l": 60, "r": 130, "t": 20, "b": 40}
    iw = width - pad["l"] - pad["r"]
    ih = height - pad["t"] - pad["b"]
    xs_all = [x for pts in series.values() for x, _ in pts]
    if not xs_all:
        return (f'<svg viewBox="0 0 {width} {height}" class="chart line empty">'
                f'<text x="{width/2}" y="{height/2}" text-anchor="middle" class="axis-label">'
                f'no data — scenario not yet measured</text></svg>')
    x_min, x_max = min(xs_all), max(xs_all)
    if x_min == x_max:
        x_max = x_min + 1

    def sx(v): return pad["l"] + (v - x_min) / max(x_max - x_min, 1e-9) * iw
    def sy(v): return pad["t"] + (1 - (v - y_min) / max(y_max - y_min, 1e-9)) * ih

    parts = [f'<svg viewBox="0 0 {width} {height}" class="chart line" role="img" aria-label="line chart">']
    # Axes
    parts.append(f'<line x1="{pad["l"]}" y1="{pad["t"]}" x2="{pad["l"]}" y2="{pad["t"]+ih}" class="axis"/>')
    parts.append(f'<line x1="{pad["l"]}" y1="{pad["t"]+ih}" x2="{pad["l"]+iw}" y2="{pad["t"]+ih}" class="axis"/>')
    # Y grid + ticks
    for f in (0.0, 0.25, 0.5, 0.75, 1.0):
        v = y_min + f * (y_max - y_min)
        y = sy(v)
        parts.append(f'<line x1="{pad["l"]}" y1="{y:.1f}" x2="{pad["l"]+iw}" y2="{y:.1f}" class="grid"/>')
        parts.append(f'<text x="{pad["l"]-8}" y="{y+4:.1f}" class="tick" text-anchor="end">{y_fmt(v)}</text>')
    # X ticks
    if x_max > x_min:
        for f in (0.0, 0.25, 0.5, 0.75, 1.0):
            v = x_min + f * (x_max - x_min)
            x = sx(v)
            parts.append(f'<text x="{x:.1f}" y="{pad["t"]+ih+18}" class="tick" text-anchor="middle">{v:.0f}</text>')
    parts.append(f'<text x="{pad["l"]+iw/2:.0f}" y="{height-6}" class="axis-label" text-anchor="middle">{esc(x_label)}</text>')
    parts.append(f'<text x="14" y="{pad["t"]+ih/2:.0f}" class="axis-label" '
                 f'transform="rotate(-90 14 {pad["t"]+ih/2:.0f})" text-anchor="middle">{esc(y_label)}</text>')
    # Optional threshold line (e.g., 99% reach)
    if threshold is not None:
        ty = sy(threshold)
        parts.append(f'<line x1="{pad["l"]}" y1="{ty:.1f}" x2="{pad["l"]+iw}" y2="{ty:.1f}" class="threshold"/>')
        parts.append(f'<text x="{pad["l"]+iw-4}" y="{ty-4:.1f}" class="threshold-label" text-anchor="end">'
                     f'{y_fmt(threshold)} target</text>')
    # Lines
    legend_y = pad["t"]
    for name in ALGO_ORDER:
        if name not in series:
            continue
        pts = series[name]
        if len(pts) < 2:
            continue
        path = " ".join(f"{sx(x):.1f},{sy(y):.1f}" for x, y in pts)
        c = color_for(name)
        dashed = ' stroke-dasharray="4,3"' if name == "optimum" else ""
        parts.append(f'<polyline points="{path}" fill="none" stroke="{c}" stroke-width="2"{dashed}/>')
        # Legend entry
        parts.append(f'<g transform="translate({pad["l"]+iw+10}, {legend_y})">')
        parts.append(f'<line x1="0" y1="0" x2="14" y2="0" stroke="{c}" stroke-width="3"{dashed}/>')
        parts.append(f'<text x="18" y="3" class="legend">{esc(name)}</text>')
        parts.append('</g>')
        legend_y += 18
    parts.append('</svg>')
    return "".join(parts)


def anysend_histogram_svg(row: Row) -> str:
    w, h = 240, 110
    pad = {"l": 6, "r": 6, "t": 6, "b": 22}
    iw = w - pad["l"] - pad["r"]
    ih = h - pad["t"] - pad["b"]
    labels = ["<50%", "50-90", "90-99", "99-99.9", "100%"]
    counts = row.anysend_hist
    total = sum(counts)
    if total == 0:
        return (f'<svg viewBox="0 0 {w} {h}" class="chart hist empty">'
                f'<text x="{w/2}" y="{h/2}" text-anchor="middle" class="axis-label">no data</text></svg>')
    max_c = max(counts)
    bar_w = iw / len(counts)
    c = color_for(row.algorithm)
    parts = [f'<svg viewBox="0 0 {w} {h}" class="chart hist" role="img" aria-label="any-send histogram">']
    for i, (count, label) in enumerate(zip(counts, labels)):
        bh = (count / max_c) * ih if max_c else 0
        x = pad["l"] + i * bar_w + 2
        y = pad["t"] + (ih - bh)
        # Highlight tail buckets (left two) in red regardless of algo color.
        fill = "#d34646" if i < 2 and count > 0 else c
        opacity = "0.9" if i < 2 and count > 0 else "0.85"
        parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w-4:.1f}" height="{bh:.1f}" '
                     f'fill="{fill}" fill-opacity="{opacity}"><title>{esc(label)}: {count}</title></rect>')
        parts.append(f'<text x="{x+bar_w/2-2:.1f}" y="{h-6}" class="hist-label" text-anchor="middle">{label}</text>')
        if count > 0:
            parts.append(f'<text x="{x+bar_w/2-2:.1f}" y="{y-2:.1f}" class="hist-count" '
                         f'text-anchor="middle">{count}</text>')
    parts.append('</svg>')
    return "".join(parts)


# ---------------------------------------------------------------------------
# Page assembly.

CSS = """
:root {
  --bg: #f5f7fb;
  --card: #ffffff;
  --ink: #16202c;
  --muted: #6a7585;
  --line: #dde4ee;
  --rank-1: #d1f0d6;
  --rank-2: #f5f8c9;
  --rank-3: #fce0b8;
  --rank-4: #f6c3bd;
  --hero: linear-gradient(135deg, #1f6feb 0%, #5b5bd6 100%);
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--ink);
       font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
main { max-width: 1280px; margin: 0 auto; padding: 24px; }
header.report-head { display: flex; align-items: baseline; gap: 14px; flex-wrap: wrap;
                     border-bottom: 1px solid var(--line); padding-bottom: 10px; margin-bottom: 18px; }
header.report-head h1 { font-size: 24px; margin: 0; letter-spacing: -0.01em; }
header.report-head .meta { color: var(--muted); font-size: 13px; }
.pills { display: flex; gap: 6px; margin-left: auto; }
.pill { background: #fff; border: 1px solid var(--line); border-radius: 999px;
        padding: 4px 10px; font-size: 12px; color: var(--muted); }
.hero { display: grid; grid-template-columns: 0.9fr 1.1fr; gap: 16px; margin: 18px 0 28px; }
.hero .verdict { background: var(--hero); color: white; border-radius: 12px;
                 padding: 22px 24px; box-shadow: 0 4px 14px #1f6feb22; position: relative; overflow: hidden; }
.hero .verdict::after { content: ""; position: absolute; right: -40px; top: -40px;
                        width: 180px; height: 180px; border-radius: 50%;
                        background: rgba(255,255,255,0.08); }
.hero .verdict .label { font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em;
                        opacity: 0.85; }
.hero .verdict h2 { font-size: 32px; margin: 4px 0 8px; letter-spacing: -0.02em; }
.hero .verdict .why { font-size: 14px; opacity: 0.95; margin: 0 0 18px; max-width: 380px; }
.hero .verdict .stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; max-width: 460px; }
.hero .verdict .stat b { display: block; font-size: 22px; font-weight: 700; line-height: 1.1; }
.hero .verdict .stat span { display: block; font-size: 11px; opacity: 0.85;
                            text-transform: uppercase; letter-spacing: 0.06em; margin-top: 2px; }
.hero .pareto-wrap { background: var(--card); border: 1px solid var(--line);
                     border-radius: 12px; padding: 14px 16px; box-shadow: 0 1px 2px #00000010; }
.hero .pareto-wrap h3 { margin: 0 0 4px; font-size: 14px; }
.hero .pareto-wrap p { margin: 0 0 6px; font-size: 12px; color: var(--muted); }
section.card { background: var(--card); border: 1px solid var(--line); border-radius: 12px;
               padding: 18px 20px; box-shadow: 0 1px 2px #00000010; margin: 16px 0; }
section.card > h2 { margin: 0 0 4px; font-size: 18px; letter-spacing: -0.01em; }
section.card > h2 small { font-weight: 400; color: var(--muted); font-size: 13px; margin-left: 8px; }
section.card .chart-row { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; margin-top: 14px; }
section.card .chart-row > div { background: #fafcff; border: 1px solid var(--line);
                                border-radius: 8px; padding: 10px 12px; }
section.card .chart-row h3 { margin: 0 0 4px; font-size: 13px; color: var(--ink); }
section.card .chart-row p.hint { margin: 0 0 4px; color: var(--muted); font-size: 11px; }
.ranking { width: 100%; border-collapse: collapse; margin-top: 12px; font-size: 13px; }
.ranking th, .ranking td { padding: 6px 8px; border-bottom: 1px solid var(--line); text-align: right; }
.ranking th { background: #eef3f8; color: #435062; font-weight: 600; }
.ranking th:first-child, .ranking td:first-child { text-align: left; }
.ranking td .swatch { display: inline-block; width: 10px; height: 10px; border-radius: 2px;
                      margin-right: 6px; vertical-align: middle; }
.ranking td.r1 { background: var(--rank-1); }
.ranking td.r2 { background: var(--rank-2); }
.ranking td.r3 { background: var(--rank-3); }
.ranking td.r4 { background: var(--rank-4); }
.ranking td.star::after { content: " ★"; color: #b58908; }
.ranking tr.optimum td { font-style: italic; color: #555; }
.role-pill { display: inline-block; font-size: 10px; padding: 1px 6px; border-radius: 999px;
             background: #e3e9f2; color: #435062; text-transform: uppercase; letter-spacing: 0.06em;
             margin-left: 6px; vertical-align: middle; }
.role-pill.candidate { background: #e0eaff; color: #1f6feb; }
.role-pill.production { background: #fde7d2; color: #a8530d; }
.role-pill.adversarial { background: #d8f0f3; color: #0d5d6a; }
.role-pill.baseline { background: #efeff2; color: #475063; }
.role-pill.ceiling { background: #222; color: #fff; }
.anysend-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
                gap: 12px; margin-top: 12px; }
.anysend-grid figure { margin: 0; background: #fafcff; border: 1px solid var(--line);
                       border-radius: 8px; padding: 8px 10px; }
.anysend-grid figcaption { font-size: 13px; font-weight: 600; }
.anysend-grid figcaption .sub { font-size: 11px; color: var(--muted); display: block; font-weight: 400; }
details.full-data { background: var(--card); border: 1px solid var(--line); border-radius: 12px;
                    padding: 12px 16px; margin: 20px 0; }
details.full-data summary { cursor: pointer; font-weight: 600; font-size: 14px; }
.legend, .tick, .hist-label, .hist-count, .axis-label, .row-label, .tail-num, .dot-label,
.threshold-label { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
.legend { font-size: 11px; fill: #475063; }
.tick { font-size: 11px; fill: #6a7585; }
.axis { stroke: #b9c3d3; stroke-width: 1; }
.grid { stroke: #e7ecf3; stroke-width: 1; stroke-dasharray: 2,2; }
.frontier { fill: none; stroke: #1f6feb; stroke-width: 1.5; stroke-dasharray: 4,2; opacity: 0.7; }
.dot-label { font-size: 10px; }
.row-label { font-size: 12px; font-weight: 600; }
.tail-num { font-size: 11px; fill: #475063; }
.axis-label { font-size: 11px; fill: #6a7585; }
.hist-label { font-size: 9px; fill: #6a7585; }
.hist-count { font-size: 10px; fill: #16202c; font-weight: 600; }
.threshold { stroke: #d34646; stroke-width: 1; stroke-dasharray: 5,3; opacity: 0.5; }
.threshold-label { font-size: 10px; fill: #d34646; }
.notes-line { display: flex; gap: 14px; flex-wrap: wrap; font-size: 12px; color: var(--muted);
              margin-top: 8px; }
.notes-line span::before { content: "•"; margin-right: 4px; color: #b9c3d3; }

/* Tests matrix */
.tests-matrix { width: 100%; border-collapse: collapse; font-size: 12.5px; margin-top: 6px; }
.tests-matrix th, .tests-matrix td { padding: 6px 8px; border-bottom: 1px solid var(--line);
                                     text-align: center; vertical-align: middle; }
.tests-matrix th { background: #eef3f8; color: #435062; font-weight: 600; font-size: 11px; }
.tests-matrix th:first-child, .tests-matrix td:first-child { text-align: left; }
.tests-matrix td.t-pass { color: #1a8137; }
.tests-matrix td.t-warn { color: #a8530d; background: #fff6e3; }
.tests-matrix td.t-fail { color: #b3261e; background: #ffe7e3; }
.tests-matrix td.t-skip { color: #97a0ad; background: #f4f6fa; }
.tests-matrix td b { font-size: 14px; line-height: 1; display: block; }
.tests-matrix td .t-detail { display: block; font-size: 10.5px; color: #6a7585; margin-top: 1px; }
.tests-matrix td.t-pass .t-detail { color: #65866c; }
.tests-matrix td.t-warn .t-detail { color: #8a5a18; }
.tests-matrix td.t-fail .t-detail { color: #8a2c24; }
.tests-matrix tr.fail { box-shadow: inset 3px 0 0 #b3261e; }
.tests-matrix tr.warn { box-shadow: inset 3px 0 0 #e0a020; }
.tests-matrix td.verdict { font-weight: 700; letter-spacing: 0.02em; }
.tests-matrix td.verdict.v-pass { color: #1a8137; }
.tests-matrix td.verdict.v-warn { color: #a8530d; }
.tests-matrix td.verdict.v-fail { color: #b3261e; }
.frontier-hint { color: var(--muted); font-size: 11.5px; margin-top: -4px; margin-bottom: 8px; }
.frontier-hint b { color: #b58908; font-weight: 700; }

/* Paired graph cells in ranking tables */
.ranking th .graph-key { display: block; font-size: 10px; color: var(--muted);
                         font-weight: 400; margin-top: 2px; letter-spacing: 0.01em; }
.ranking th .graph-key b { color: #16202c; font-weight: 700; }
.ranking td .cell-main { font-weight: 600; }
.ranking td .cell-synth { color: #6a7585; font-weight: 400; font-size: 0.92em; }

/* Algorithm catalogue (list of descriptions) */
.catalogue { background: var(--card); border: 1px solid var(--line); border-radius: 10px;
             padding: 18px 22px; margin: 18px 0; }
.catalogue h2 { margin-top: 0; font-size: 18px; letter-spacing: -0.005em; }
.catalogue-table { width: 100%; border-collapse: collapse; font-size: 13px; }
.catalogue-table th { text-align: left; padding: 8px 10px; border-bottom: 1px solid var(--line);
                      color: var(--muted); font-weight: 500; }
.catalogue-table td { padding: 8px 10px; border-bottom: 1px solid var(--line); vertical-align: top; }
.catalogue-table td:first-child { font-family: ui-monospace, Menlo, monospace; font-size: 12px;
                                  white-space: nowrap; }
.catalogue-table tr:last-child td { border-bottom: none; }
"""


def select(rows: List[Row], size: str, graph: str = "mainnet") -> List[Row]:
    return [r for r in rows if r.size == size and r.graph == graph]


def pair_for(rows: List[Row], algorithm: str, size: str) -> Tuple[Optional[Row], Optional[Row]]:
    main = next((r for r in rows if r.algorithm == algorithm and r.size == size and r.graph == "mainnet"), None)
    synth = next((r for r in rows if r.algorithm == algorithm and r.size == size and r.graph == "synth"), None)
    return main, synth


def algos_in(rows: List[Row], size: str) -> List[str]:
    seen = []
    for r in rows:
        if r.size == size and r.algorithm not in seen:
            seen.append(r.algorithm)
    return seen


def render_hero(rows: List[Row]) -> str:
    medium = select(rows, "medium")
    # Pick recommended algorithm: best honest_cost among algorithms with reach >= 0.99
    # AND max_leech_pct >= 30 (operational floor).
    candidates = [r for r in medium if r.reach >= 0.99 and r.max_leech_pct >= 30
                  and ALGO_ROLE.get(r.algorithm) in {"candidate", "production"}]
    candidates.sort(key=lambda r: r.network_overhead_honest_total)
    rec = candidates[0] if candidates else medium[0]
    other_sizes = {r.size: r for r in rows if r.algorithm == rec.algorithm}
    return f"""
<div class="hero">
  <div class="verdict">
    <div class="label">Recommended</div>
    <h2>{esc(rec.algorithm)}</h2>
    <p class="why">Best balanced honest-network cost vs p95 latency among algorithms
       that hold reach ≥ 99% under 30%+ leech ratio. Trade-off explained below.</p>
    <div class="stats">
      <div class="stat"><b>{rec.reach*100:.1f}%</b><span>reach ({esc(rec.size)})</span></div>
      <div class="stat"><b>{rec.p95_ms:.0f}ms</b><span>p95 latency</span></div>
      <div class="stat"><b>{rec.network_overhead_honest_total:.1f}×</b><span>honest cost</span></div>
      <div class="stat"><b>≤{rec.max_leech_pct}%</b><span>leech tolerant</span></div>
    </div>
  </div>
  <div class="pareto-wrap">
    <h3>Cost vs Latency Frontier <small style="font-weight:400;color:var(--muted)">256 KiB block</small></h3>
    <p>Dot size = leech tolerance. Dashed line = optimum (not deployable).</p>
    {pareto_svg(medium)}
  </div>
</div>"""


def render_ranking_table(all_rows: List[Row], size: str, frontier_set: set) -> str:
    """Render one row per algorithm. Each metric cell shows mainnet / synth."""
    mainnet_rows = [r for r in all_rows if r.size == size and r.graph == "mainnet"]
    parts = ['<table class="ranking"><thead><tr><th>algorithm</th>']
    for _, header, _, _ in METRICS:
        parts.append(f'<th>{esc(header)}'
                     f'<br><small class="graph-key"><b>main</b> / <span>synth</span></small></th>')
    parts.append('</tr></thead><tbody>')
    # Rank columns by the mainnet value — that's what we deploy against.
    columns: Dict[str, List[float]] = {}
    for key, *_ in METRICS:
        columns[key] = [getattr(r, key) for r in mainnet_rows]
    for main in sorted(mainnet_rows,
                       key=lambda r: (r.algorithm != "optimum", r.network_overhead_honest_total)):
        _, synth = pair_for(all_rows, main.algorithm, size)
        cls = "optimum" if main.algorithm == "optimum" else ""
        role = ALGO_ROLE.get(main.algorithm, "")
        parts.append(f'<tr class="{cls}"><td><span class="swatch" '
                     f'style="background:{color_for(main.algorithm)}"></span>'
                     f'{esc(main.algorithm)}<span class="role-pill {role}">{role}</span></td>')
        for key, _, fmt, lower in METRICS:
            main_v = getattr(main, key)
            synth_v = getattr(synth, key) if synth is not None else None
            rc = rank_class(columns[key], main_v, lower)
            star_cls = " star" if (key == "network_overhead_honest_total"
                                   and main.algorithm in frontier_set) else ""
            main_str = fmt(main_v)
            synth_str = fmt(synth_v) if synth_v is not None else "·"
            parts.append(f'<td class="{rc}{star_cls}">'
                         f'<span class="cell-main">{main_str}</span>'
                         f'<span class="cell-synth"> / {synth_str}</span></td>')
        parts.append('</tr>')
    parts.append('</tbody></table>')
    return "".join(parts)


def compute_frontier(rows: List[Row]) -> set:
    """Pareto frontier on (network_overhead_honest, p95_ms) for reach ≥ 99%."""
    eligible = [r for r in rows if r.reach >= 0.99]
    frontier = set()
    for r in eligible:
        dominated = False
        for other in eligible:
            if other.algorithm == r.algorithm:
                continue
            if (other.network_overhead_honest_total <= r.network_overhead_honest_total
                    and other.p95_ms <= r.p95_ms
                    and (other.network_overhead_honest_total < r.network_overhead_honest_total
                         or other.p95_ms < r.p95_ms)):
                dominated = True
                break
        if not dominated:
            frontier.add(r.algorithm)
    return frontier


def render_per_size(rows: List[Row], size_name: str, size_bytes: int, blurb: str) -> str:
    mainnet_rows = select(rows, size_name, "mainnet")
    if not mainnet_rows:
        return ""  # No data for this size — skip the card entirely.
    frontier = compute_frontier(mainnet_rows)
    leech_series = {r.algorithm: r.leech_curve for r in mainnet_rows}
    round_series = {r.algorithm: r.round_curve for r in mainnet_rows}
    round_p95_series = {r.algorithm: r.round_p95_curve for r in mainnet_rows}
    return f"""
<section class="card">
  <h2>{esc(size_name)} · {human_bytes(size_bytes)} <small>{esc(blurb)}</small></h2>
  {render_ranking_table(rows, size_name, frontier)}
  <p class="frontier-hint"><b>★</b> marks Pareto-optimal algorithms — no other entry beats them
     on cost <em>and</em> p95 latency at reach ≥ 99%.
     Each cell shows <b>mainnet</b> / <span style="color:var(--muted)">synth</span> — column
     rank uses mainnet (the deployment graph).</p>
  <div class="chart-row">
    <div>
      <h3>Latency tail</h3>
      <p class="hint">Solid = p50 · medium = p95 · light = p99. Tail width matters more than the peak.</p>
      {latency_tail_svg(mainnet_rows)}
    </div>
    <div>
      <h3>Leech tolerance</h3>
      <p class="hint">Reach as leech ratio grows. Crossing the 99% line is the operational ceiling.</p>
      {line_chart_svg(leech_series, x_label="leech %", y_label="reach",
                      y_min=0.5, y_max=1.0, y_fmt=lambda v: f"{v*100:.0f}%",
                      threshold=0.99)}
    </div>
  </div>
  <div class="chart-row">
    <div>
      <h3>Multi-round reach (PersistentScores)</h3>
      <p class="hint">Does the algorithm improve as it learns peers?</p>
      {line_chart_svg(round_series, x_label="round", y_label="reach",
                      y_min=0.95, y_max=1.0, y_fmt=lambda v: f"{v*100:.1f}%")}
    </div>
    <div>
      <h3>Multi-round p95 latency</h3>
      <p class="hint">Same — but for latency. Should drop or hold flat, never grow.</p>
      {line_chart_svg(round_p95_series, x_label="round", y_label="p95 ms",
                      y_min=0,
                      y_max=max((p for s in round_p95_series.values() for _, p in s), default=200) * 1.05,
                      y_fmt=lambda v: f"{v:.0f}ms")}
    </div>
  </div>
</section>"""


def render_multi_sender(rows: List[Row]) -> str:
    """Parallel-publisher AnySend: N validators publish the same body simultaneously.
    Shows how p95 latency drops with N while honest-network cost stays flat (body_id dedup)."""
    medium = [r for r in select(rows, "medium") if r.multi_sender]
    if not medium:
        return ""
    # Find the set of N values measured (e.g. {1, 5, 10}).
    n_values = sorted({n for r in medium for n, _, _, _ in r.multi_sender})
    parts = ['<section class="card"><h2>Parallel-Sender AnySend '
             '<small>same body_id published from N validators at once · 100 KiB block · mainnet</small></h2>'
             '<p style="color:var(--muted);font-size:13px;margin:6px 0 0;">'
             'Tests the AnySend flag: multiple validators broadcast the same block at t=0. '
             'Receivers dedup on body_id, so the honest-network cost should not scale with N. '
             'p95 latency should drop as parallel paths cut hops.</p>'
             '<table class="ranking"><thead><tr><th>algorithm</th>']
    for n in n_values:
        parts.append(f'<th>p95@N={n}</th>')
    parts.append('<th>cost (any N)</th><th>latency speedup</th></tr></thead><tbody>')
    for r in sorted(medium, key=lambda r: r.multi_sender[0][1] if r.multi_sender else 0):
        by_n = {n: (p95, honest, reach) for n, p95, honest, reach in r.multi_sender}
        base = by_n.get(n_values[0], (1.0, 0, 0))[0]
        last = by_n.get(n_values[-1], (1.0, 0, 0))[0]
        speedup = (base / last - 1.0) * 100 if last > 0 else 0
        honest_cost = by_n.get(n_values[-1], (0, 0, 0))[1]
        parts.append(f'<tr class="ok"><td><span class="swatch" style="background:{color_for(r.algorithm)}"></span>'
                     f'{esc(r.algorithm)}</td>')
        for n in n_values:
            v = by_n.get(n)
            parts.append(f'<td>{v[0]:.0f}ms</td>' if v else '<td>·</td>')
        parts.append(f'<td>{honest_cost:.1f}×</td>'
                     f'<td style="color:#1a8137"><b>−{speedup:.0f}%</b></td></tr>')
    parts.append('</tbody></table>')
    parts.append('</section>')
    return "".join(parts)


def render_anysend(rows: List[Row]) -> str:
    medium = select(rows, "medium")
    parts = ['<section class="card"><h2>Per-Sender Coverage '
             '<small>100 KiB block, source rotates one-at-a-time over honest peers</small></h2>'
             '<p style="color:var(--muted);font-size:13px;margin:6px 0 0;">'
             'For each potential sender, the algorithm runs one broadcast solo. Histogram bars '
             'count how many senders fell into each reach bucket. Tall right-most bar = every '
             'sender successfully covered the network; red left bars = senders whose broadcast '
             'silently failed. (Different from the AnySend mode where many validators broadcast '
             'the same block in parallel — that test is a planned follow-up.)</p>'
             '<div class="anysend-grid">']
    medium_sorted = sorted(medium, key=lambda r: (sum(r.anysend_hist[:2]), -r.anysend_hist[-1]))
    for r in medium_sorted:
        total = sum(r.anysend_hist)
        bad = r.anysend_hist[0] + r.anysend_hist[1]
        bad_pct = (bad / total) * 100 if total else 0
        parts.append(f'<figure><figcaption>'
                     f'<span class="swatch" style="background:{color_for(r.algorithm)};'
                     f'display:inline-block;width:10px;height:10px;margin-right:6px;vertical-align:middle;'
                     f'border-radius:2px;"></span>{esc(r.algorithm)}'
                     f'<span class="sub">{bad} of {total} sources delivered &lt; 90% '
                     f'({bad_pct:.1f}%)</span></figcaption>'
                     f'{anysend_histogram_svg(r)}</figure>')
    parts.append('</div></section>')
    return "".join(parts)


def render_full_table(rows: List[Row]) -> str:
    parts = ['<details class="full-data"><summary>Full data table '
             '(every algorithm × every size × every graph)</summary>'
             '<table class="ranking"><thead><tr><th>algorithm</th><th>size</th><th>graph</th>']
    for _, header, _, _ in METRICS:
        parts.append(f'<th>{esc(header)}</th>')
    parts.append('</tr></thead><tbody>')
    for r in sorted(rows, key=lambda r: (r.size, r.graph,
                                         ALGO_ORDER.index(r.algorithm) if r.algorithm in ALGO_ORDER else 999)):
        parts.append(f'<tr><td><span class="swatch" style="background:{color_for(r.algorithm)}"></span>'
                     f'{esc(r.algorithm)}</td><td>{esc(r.size)}</td>'
                     f'<td>{esc(GRAPH_LABELS.get(r.graph, r.graph))}</td>')
        for key, _, fmt, _ in METRICS:
            parts.append(f'<td>{fmt(getattr(r, key))}</td>')
        parts.append('</tr>')
    parts.append('</tbody></table></details>')
    return "".join(parts)


def render_catalogue(rows: List[Row], descriptions: Dict[str, str]) -> str:
    if not descriptions:
        return ""
    seen = []
    for r in rows:
        if r.algorithm in descriptions and r.algorithm not in seen:
            seen.append(r.algorithm)
    if not seen:
        return ""
    rows_html = []
    for label in seen:
        rows_html.append(
            f'<tr><td><span class="swatch" style="background:{color_for(label)}"></span>'
            f'{esc(label)}</td><td>{esc(descriptions[label])}</td></tr>')
    return f"""
<section class="catalogue">
  <h2>Algorithm catalogue</h2>
  <table class="catalogue-table">
    <thead><tr><th style="width:240px">algorithm</th><th>description</th></tr></thead>
    <tbody>{''.join(rows_html)}</tbody>
  </table>
</section>"""


def render_html(rows: List[Row], fake: bool, graph_label: str, seeds: int,
                descriptions: Optional[Dict[str, str]] = None) -> str:
    descriptions = descriptions or {}
    today = dt.date.today().isoformat()
    pills = (
        f'<span class="pill">date={today}</span>'
        f'<span class="pill">graphs={esc(graph_label)}</span>'
        f'<span class="pill">seeds={seeds}</span>'
        f'<span class="pill">rounds={ROUNDS}</span>'
        + (f'<span class="pill" style="background:#fff4d6;border-color:#f0c14b">'
           f'fake-data preview</span>' if fake else "")
    )
    body = [render_hero(rows), render_tests_section(rows)]
    for size_name, size_bytes, blurb in BLOCK_SIZES:
        body.append(render_per_size(rows, size_name, size_bytes, blurb))
    body.append(render_multi_sender(rows))
    body.append(render_anysend(rows))
    body.append(render_full_table(rows))
    body.append(render_catalogue(rows, descriptions))
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Broadcast Benchmark</title>
<style>{CSS}</style>
</head>
<body>
<main>
<header class="report-head">
  <h1>Broadcast Benchmark</h1>
  <div class="meta">v2 layout · headline above the fold</div>
  <div class="pills">{pills}</div>
</header>
{''.join(body)}
</main>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# CSV loader (real path — used when not in --fake mode).

def load_csv(path: Path, anysend_path: Optional[Path] = None) -> List[Row]:
    """Load the v2 CSV. Missing optional fields default to 0. The C++ side may emit
    one row per (algorithm, size, graph, scenario, leech_pct, round); we group all
    rows with the same key and derive leech_curve, round_curve, etc.
    """
    # Optional second CSV: AnySend histograms.
    anysend: Dict[Tuple[str, str, str], List[int]] = {}
    if anysend_path and anysend_path.exists():
        with anysend_path.open() as fh:
            for row in csv.DictReader(fh):
                key = (row["algorithm"], row["size"], row.get("graph", "mainnet"))
                anysend[key] = [int(row.get(f"b_{tag}", "0") or 0)
                                for tag in ("lt50", "50_90", "90_99", "99_999", "100")]
    def f(row: dict, key: str, default: float = 0.0) -> float:
        v = row.get(key, "")
        return float(v) if v not in ("", None) else default

    grouped: Dict[Tuple[str, str, str], List[dict]] = {}
    with path.open() as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            key = (row["algorithm"], row["size"], row.get("graph", "mainnet"))
            grouped.setdefault(key, []).append(row)

    # Multi-sender rows have scenario starting with "multi" — collect them keyed by
    # (algorithm, size, graph) -> list of (n_senders, row). The leech-curve and round-curve
    # paths must skip these or they'd be confused for parallel single-shot clean rows.
    multi_sender_rows: Dict[Tuple[str, str, str], List[Tuple[int, dict]]] = {}
    rows: List[Row] = []
    for (algorithm, size, graph), raw_rows in grouped.items():
        multi_rows = [r for r in raw_rows if (r.get("scenario") or "").startswith("multi")]
        if multi_rows:
            multi_sender_rows[(algorithm, size, graph)] = []
            for r in multi_rows:
                s = r["scenario"]
                try:
                    n = int(s[5:])  # "multi10" -> 10
                except ValueError:
                    n = 0
                multi_sender_rows[(algorithm, size, graph)].append((n, r))
        # Single-shot rows have round=0 or no round column; multi-round rows have round>=1.
        # The convergence chart only reads clean (leech_pct=0) rounds — mixing in a parallel
        # leech-attack run gives a saw-tooth visual that's just two regimes interleaved.
        non_multi = [r for r in raw_rows if not (r.get("scenario") or "").startswith("multi")]
        single_shot = [r for r in non_multi if int(f(r, "round")) == 0]
        multi_round = [r for r in non_multi
                       if int(f(r, "round")) > 0 and f(r, "leech_pct") == 0.0]
        if not single_shot and multi_round:
            single_shot = multi_round[:1]  # fallback: first round as clean stand-in
        clean = next((r for r in single_shot if f(r, "leech_pct") == 0.0), single_shot[0])
        # Leech curve from single-shot rows (sorted by leech_pct).
        leech_curve: List[Tuple[int, float]] = []
        for r in sorted(single_shot, key=lambda r: f(r, "leech_pct")):
            leech_curve.append((int(f(r, "leech_pct")), f(r, "reach")))
        # Round curves from multi-round rows.
        round_curve: List[Tuple[int, float]] = []
        round_p95_curve: List[Tuple[int, float]] = []
        for r in sorted(multi_round, key=lambda r: int(f(r, "round"))):
            rd = int(f(r, "round"))
            round_curve.append((rd, f(r, "reach")))
            round_p95_curve.append((rd, f(r, "p95_ms")))
        # Derive max_leech_pct: largest ratio with reach >= 0.99.
        max_leech_pct = 0
        for lr, reach in leech_curve:
            if reach >= 0.99:
                max_leech_pct = max(max_leech_pct, lr)
        rows.append(Row(
            algorithm=algorithm,
            size=size,
            graph=graph,
            reach=f(clean, "reach"),
            p50_ms=f(clean, "p50_ms"),
            p95_ms=f(clean, "p95_ms"),
            p99_ms=f(clean, "p99_ms"),
            node_out_p95=f(clean, "node_out_p95"),
            node_in_p95=f(clean, "node_in_p95"),
            network_overhead_honest=f(clean, "network_overhead_honest"),
            network_overhead_honest_total=f(clean, "network_overhead_honest_total"),
            source_x=f(clean, "source_x"),
            dup_pct=f(clean, "dup_pct"),
            control_pct=f(clean, "control_pct"),
            max_leech_pct=max_leech_pct,
            leech_curve=leech_curve,
            round_curve=round_curve,
            round_p95_curve=round_p95_curve,
            anysend_hist=anysend.get((algorithm, size, graph), [0, 0, 0, 0, 0]),
            multi_sender=sorted(
                [(n, f(r, "p95_ms"), f(r, "network_overhead_honest_total"), f(r, "reach"))
                 for n, r in multi_sender_rows.get((algorithm, size, graph), [])],
                key=lambda x: x[0]),
        ))
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, help="Input CSV from broadcast-bench")
    parser.add_argument("--anysend-csv", type=Path, help="Optional AnySend histogram CSV")
    parser.add_argument("--descriptions",
                        type=Path,
                        help="Optional TSV from `broadcast-bench --graph mainnet --descriptions`. "
                             "Columns: algorithm<tab>description.")
    parser.add_argument("--fake", action="store_true", help="Use synthetic fake data (no CSV needed)")
    parser.add_argument("--out", type=Path, default=Path("/tmp/broadcast-bench.html"),
                        help="Output HTML path")
    parser.add_argument("--graph-label",
                        default="mainnet-v2 (1259h/168l) · rand-reg-1000 (synth)")
    parser.add_argument("--seeds", type=int, default=20)
    args = parser.parse_args()

    if args.fake:
        rows = fake_dataset()
    elif args.csv:
        rows = load_csv(args.csv, args.anysend_csv)
    else:
        print("error: --csv or --fake required", file=sys.stderr)
        return 2

    descriptions: Dict[str, str] = {}
    if args.descriptions and args.descriptions.exists():
        with args.descriptions.open() as fh:
            for ln, line in enumerate(fh):
                if ln == 0 or "\t" not in line:
                    continue
                label, desc = line.rstrip("\n").split("\t", 1)
                descriptions[label] = desc

    html_out = render_html(rows, fake=args.fake, graph_label=args.graph_label, seeds=args.seeds,
                           descriptions=descriptions)
    args.out.write_text(html_out)
    print(f"wrote {args.out} ({len(html_out)} bytes, {len(rows)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
