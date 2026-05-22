# Overlay Broadcast V2 — Design

This is the overlay broadcast V2 stack. It ships when an experimental algorithm is configured for
an overlay category (public, fast-sync, or private). C++ subsystem: `OverlayBroadcasts`. TL wire
family: `overlay.broadcastV2*`. Test CLI flag: `--v2-broadcast` / `--v2-plumtree`.

## Quick start

```bash
# Build
cmake -B cmake-build-relwithdebinfo -GNinja
ninja -C cmake-build-relwithdebinfo broadcast-bench test-bsim test-overlay-broadcast-session test-overlay-leech

# Unit tests (algorithm correctness, bandwidth model, scoring, leech behaviour)
cmake-build-relwithdebinfo/test/broadcast-sim/test-bsim
cmake-build-relwithdebinfo/test/broadcast-sim/test-overlay-broadcast-session

# Benchmark on synthetic topology (procedural mesh/line/star, no external data needed)
cmake-build-relwithdebinfo/test/broadcast-sim/broadcast-bench --suite=quick

# Benchmark on real mainnet topology — first prepare the inputs:
#   cp /path/to/overlay-peers.sim.json /tmp/overlay-peers.json
#   test/broadcast-sim/prepare_mainnet_topology.sh    # geolocates IPs → /tmp/latency_matrix.json
# Then run:
cmake-build-relwithdebinfo/test/broadcast-sim/broadcast-bench \
    --graph=mainnet --peers=/tmp/overlay-peers.json --latency=/tmp/latency_matrix.json \
    --body-size=1048576 --rounds=10 --seeds=3 --view=algo

# HTML report from a CSV
cmake-build-relwithdebinfo/test/broadcast-sim/broadcast-bench \
    --graph=mainnet --peers=/tmp/overlay-peers.json --csv=/tmp/bench.csv
python3 test/broadcast-sim/render_bench.py --csv=/tmp/bench.csv --out=/tmp/report.html

# End-to-end leech experiment against real ADNL + overlay process
cmake-build-relwithdebinfo/test/test-overlay-leech --v2-broadcast \
    -v0 -n10 -k10 -b10 -F -B600 -S30 -R5 --churn-honest=2
```

See `broadcast-bench --help` and `test-overlay-leech --help` for the full CLI surface.

## Code layout

```
overlay/broadcast/
├── algorithm.h                  Public interface: events, actions, messages, configs.
├── algorithm.cpp                Just register_peers — see algorithms/ for implementations.
├── algorithms/
│   ├── peer-table.h             Internal: PeerTable + PeerRankOptions, shared by every algorithm.
│   ├── push.cpp                 PushAlgorithm (flood / active-set).
│   ├── pull-only.cpp            PullOnlyAlgorithm (pure-pull Have→Request→Piece).
│   ├── fec.cpp                  FecAlgorithm (push-only multi-piece flood, FEC + FecRand).
│   ├── sub-direct.cpp           SubDirectAlgorithm (subscribe/push with optional PRUNE).
│   ├── twostep.cpp              TwostepAlgorithm (private-overlay 2-hop push).
│   ├── eager-lazy.cpp           EagerLazyAlgorithm (push to top-K + Have + pull repair).
│   ├── plumtree.cpp             PlumtreeAlgorithm (eager-lazy with PRUNE/GRAFT-style adaptation).
│   ├── gossip-mask-fec.cpp      GossipMaskFec (FEC + per-peer have-mask gossip).
│   ├── optimum-p2p.cpp          OptimumP2PAlgorithm (RLNC theoretical lower bound, not deployable).
│   └── leech.cpp                LeechAlgorithm (adversarial — sim/test only, never ships).
├── wire.h/.cpp                  TL parse/encode for V2 messages.
├── storage.h/.cpp               Body assembly + ready-decision.
├── profile.h/.cpp               Bind a (wire, storage, algorithm) tuple into a BroadcastMode.
├── overlay-broadcast.h/.cpp     Subsystem owner: anti-replay, mode planning, routing.
├── overlay-broadcast-session.h/.cpp  Per-broadcast session runtime.
├── rlnc-codec.h/.cpp            Local RLNC helpers used by storage and tests.
├── score.h/.cpp                 Cross-broadcast peer scoring (BroadcastPeerScore + decay).
└── docs/                        This document and broadcast.md (research log).
```

## Architecture: events → algorithms → actions

Each algorithm is a pure reducer. Before stepping it, the session updates the algorithm's current
clock with `set_now()`, then feeds it an `Event`; the algorithm returns `Action`s. The session
encodes actions into wire messages and sends them via ADNL. The algorithm exposes any desired retry
wakeup through `alarm()` as an absolute `td::Timestamp`; the host owns concrete scheduling and
filters stale timer callbacks. The algorithm holds no side-channel state — everything observable
goes through `stats()` and `alarm()`.

```
                                       Event                       Action
            ┌───────────────────┐    ┌──────────┐               ┌────────────┐
            │ adnl wire receive │ → ─│ Receive  │               │ Send       │── → wire encode
            │ publish API call  │ → ─│ Publish  │               │ Deliver
            │ storage decoded   │ → ─│ BodyReady│  algorithm    │ PeerFeedback
            │ peer joined       │ → ─│ Register │ set_now+handle▶│
            │ retry alarm fired │ → ─│ Timer    │   (Event)     │
            └───────────────────┘    └──────────┘               └────────────┘
                                                 │
                                                 ▼
                                          alarm(): Timestamp
                                                                         │
                                                                         ▼
                                                            session.peer_score (persistent)
```

`Event` variants: `Publish`, `Receive{from, message}`, `BodyReady`, `Timer`, `RegisterPeer{peer}`.
`Action` variants: `Deliver`, `Send{peer, message}`, `PeerFeedback{peer, delta}`.
Production `Message` variants: `Have`, `Request`, `Cancel`, `Piece{id, duplicate}`.
Simulator-only `Message` variants: `sim::HavePieces{pieces}`, `sim::RequestPieces{needs}`.

The simulator-only messages are not serializable by the current V2 wire; production profiles use
plain control messages only. `GossipMaskFec` uses the simulator-only `HavePieces`/`RequestPieces`
to coordinate piece-mask exchange during research benchmarks.

## Shared state: per-overlay vs per-broadcast

Each algorithm family declares its own `BroadcastShared` subclass. The engine creates **one
Shared instance per overlay** (= per registered family) and passes it to every per-broadcast
algorithm via the family's `make_algorithm` closure. So state splits two ways:

- **Per-broadcast** (`BroadcastAlgorithm` member fields): pieces received, pending requests,
  this body's pull state. Discarded when the session closes.
- **Per-overlay** (`BroadcastShared` subclass): adapts across broadcasts. Examples:
  - `PlumtreeShared` — the eager/lazy peer split. PRUNE/GRAFT in one broadcast reshapes the
    mesh for the next; the tree converges over rounds.
  - `SubDirectShared` — subscription leases (which peers asked to receive bodies from us).
  - `PushShared` / `PullOnlyShared` / `FecShared` — just a `PeerTable` of known peers, no
    cross-broadcast learning.
  - `empty_shared()` for fully-stateless algorithms.

The engine fans peer churn through `Shared::on_peer_upsert` / `on_peer_remove` exactly once per
Shared (i.e., once per overlay), then per-broadcast algorithms see the same change via a
`RegisterPeer` event. That avoids re-running peer-table inserts N times when N broadcasts are
active on one overlay.

`AlgorithmFamily::make_algorithm` captures `std::shared_ptr<ConcreteShared>` next to the
type-erased `shared` field, so the algorithm dereferences its typed Shared without a runtime
downcast. The engine never touches concrete Shared types — only `BroadcastShared::on_peer_*`.

Peer scoring (`BroadcastPeerScore` with half-life decay) lives **outside** Shared, in
`SessionState::peer_score` at the overlay layer — it's cross-family (the same score table
serves whichever algorithm is currently running on that overlay).

## Modes (current)

| TL mode           | Class                | Used by                |
|-------------------|----------------------|------------------------|
| `EagerLazy`       | EagerLazyAlgorithm   | public overlay default |
| `Plumtree`        | PlumtreeAlgorithm    | public overlay opt-in  |
| `OptimumP2P`      | OptimumP2PAlgorithm  | research only          |
| `TwostepPush`     | TwostepAlgorithm     | private-overlay body   |
| `TwostepFec`      | TwostepAlgorithm     | private-overlay FEC    |

Plumtree has its own TL constructor even though it shares the EagerLazy V2 message shape — the
extra mode tag keeps algorithm choice explicit on the wire.

`PushAlgorithm`, `PullOnlyAlgorithm`, `FecAlgorithm`, `SubDirectAlgorithm`,
`GossipMaskFecAlgorithm`, and `LeechAlgorithm` exist but are only wired into the simulator
catalogue today; no production TL mode for them yet.

## Runtime flow

Publish:

1. `OverlayImpl::send_broadcast_ex` chooses the V2 path when enabled.
2. `choose_overlay_broadcast_mode` picks a `BroadcastMode` from the configured algorithm.
3. `OverlayBroadcasts::send` computes a content-addressed broadcast id, creates a session, stores
   the source body, and emits `Publish`.
4. The session asks the algorithm for actions.
5. `Send` actions are encoded by the wire and shipped through ADNL.

Receive:

1. `OverlayImpl::process_broadcast` parses the TL message via `Wire`.
2. `OverlayBroadcasts::process` finds or creates a session for the broadcast id.
3. The session verifies signatures (cached per `(source, payload)` to amortise across algorithms),
   feeds body data into storage, and forwards protocol messages to the algorithm.
4. `BodyReady` fires when storage has a complete body.
5. `Deliver` marks the broadcast delivered and inserts the id into the local delivered cache.

## Peer scoring

`score.h` defines `BroadcastPeerScore` — a per-peer running score with exponential half-life
decay (default 60 s). Scores live in `SessionState::peer_score`, a per-overlay table that
persists across broadcasts. Algorithms emit `PeerFeedback` actions; the session adds the delta
to the peer's score and re-publishes a `RegisterPeer` event so the algorithm's local
`peers_.ranked()` sees the updated value on the next decision.

Default deltas in `PeerFeedbackConfig`:

| signal              | delta  | meaning                                       |
|---------------------|-------:|-----------------------------------------------|
| `peer_success`      | -0.25  | We received a Piece from this peer.           |
| `peer_timeout`      | +1.00  | We sent Request, no Piece arrived in time.    |
| `tree_promote`      | -0.50  | Peer relayed via off-tree path; treat as good upstream. |
| `tree_demote`       | +0.75  | Peer cancelled our Request (no longer useful).         |

Lower score = better. `peers_.ranked()` sorts ascending, so good peers go to the eager set first.

## Tests and benchmarks

Two layers, both under `test/broadcast-sim/`:

### `bsim` — the simulator engine (`bsim.cpp`/`bsim.h`)

Event-driven, deterministic. Simulates a directed graph of `NodeId`s with:

- **Per-node FIFO upload pipe** at `topology.upload_bw` bytes/s (default 100 MB/s ≈ 800 Mbps,
  validator-grade). Pieces queue behind earlier sends from the same node, then arrive after the
  per-link `topology.latency[from][to]`.
- **Honest vs leech** factories per node, picked from `topology.leech_nodes[i]`. Leeches run
  `LeechAlgorithm`; honest run whatever the catalogue row chose.
- **`SessionState`** with production `BroadcastPeerScore` per `(node, peer)`. Time advances
  `round_interval` seconds between simulate() calls so half-life decay actually fires across
  rounds.

`Spec` is the broadcast-under-test: source, body size, piece count, factory pair.
`Metrics` is the result: per-node body bytes in/out, delivery times, control bytes, duplicates.
`percentile_*` helpers compute p50/p95 across nodes.

### Catalogues and runners

- `test-bsim.cpp` — 12 hand-written unit tests. Algorithm correctness, bandwidth model, leech
  behaviour, score persistence + decay, regressions for fixed bugs (e.g.,
  `PlumtreeServeBudgetCancelDoesNotAssert`).

- `bench-synthetic.cpp` + `bench-mainnet.cpp` — both compile into the single
  `broadcast-bench` binary, dispatched on `--graph=synthetic|mainnet`. Mainnet mode loads
  `/tmp/overlay-graph-mainnet.json` (crawled overlay dump) + `/tmp/latency_matrix.json`
  (geo-derived). Useful flags:
  - `--rounds N` — multi-round mode, source rotates each round, scores persist.
  - `--warmup-rounds K` — first K rounds run with all leeches cleared so honest scores
    converge before the attack starts.
  - `--leech-ratio R` (repeatable) — promote random honest nodes to leech.
  - `--leech-mode {default|quiet|pull|full}` — `default` spams Have / swallows Request;
    `quiet` is a silent dropper; `pull` spams Request to drain honest serve budget; `full` both.
  - `--algorithms a,b,c` — restrict to a subset of the catalogue.
  - `--csv=path` — emit v2-schema CSV for `render_bench.py`.
  - `--body-size`, `--max-sim-time`, `--sources`, `--seeds`.

- `catalogue.cpp` — the `standard_catalogue()` of 18 algorithm rows the runner exercises:
  production baselines (`prod-simple`, `prod-fec-*`), twostep, no-FEC family
  (`flood-K=5`, `pull-K=10`, `eager-lazy`, `plumtree`, `plumtree-cap=5/10`, `subscribe-prune`),
  gossip-mask FEC variants, and `optimum-p2p` as the theoretical ceiling.

- `mainnet-loader.cpp` — graph + latency loader; promotes leeches by ratio.

- `render_bench.py` — turns the bench CSV into a self-contained HTML report (algorithm
  comparison, per-size tables, leech curves).

### Headline metrics

For each algorithm row the runner reports:

| column     | meaning                                                                         |
|------------|---------------------------------------------------------------------------------|
| `reach`    | Fraction of honest receivers (excluding source) that emitted Deliver.           |
| `total`    | Total network bytes (body + control) divided by `honest_count × body_size` — what each honest validator paid on average. |
| `p95_out`  | 95th-percentile per-node body upload, divided by body size — peak-load metric.  |
| `p50ms` / `p95ms` | Median / 95th-percentile delivery latency across honest receivers.       |
| `dup`      | Fraction of received pieces that were duplicates (collision / over-amplification). |

### `test-overlay-leech` — end-to-end against real overlays

`test/test-overlay-leech.cpp` spins up a real ADNL/overlay process, configurable static leech
set, optional honest churn during measurement rounds. Slower than bsim but exercises the wire
and storage paths.

```bash
./test-overlay-leech --v2-broadcast \
  -v 0 -n 10 -k 10 -b 10 -c 0 -F -B 600 -S 30 -R 5 --churn-honest 2
```

## Promising directions

Ideas worth pursuing, split by overlay class. See `broadcast.md` for the matching empirical
study on mainnet topology.

### Public overlay (≥1000 nodes, anonymous, mixed honest + leech)

**1. Pure-pull plumtree as the bandwidth floor.**
Empirically the best deployable algorithm against the mainnet graph: 1.01× of the theoretical
ideal (105.0 MB for a 100 KB body across 1060 nodes). Recipe: don't push bodies, only Haves;
let receivers pull immediately on Have; widen the active view from 5 to 32 so Haves spread
fast. Latency is pure-pull's weakness — adds 1 RTT per hop — so for sub-second consensus the right answer is probably
plumtree with `tree_peer_limit=2..4` to push critical messages directly while leaving the bulk
to pull.

**2. Larger active view (32 vs current 5).**
The single biggest topology-config win in the optimisation trail. Production's
`max_neighbours_=5` was sized for HyParView in ~50-node networks; at 1200 nodes it confines
Have spread for plumtree and bottlenecks eager-lazy fan-out. Beacon-chain CL runs mesh-of-8
over a peer pool of 50–100; we're an order of magnitude tighter.

**3. GossipSub-style mesh + scoring + PRUNE/GRAFT.**
Iterations 51–100 distill a four-primitive design: content-addressed broadcast id (cross-overlay
dedup), eager mesh of D=8 with PRUNE on duplicate body and GRAFT on heartbeat from
`top-half-by-score + random-half`, plus opportunistic graft (~10%) for partition resistance.
GossipSub's score signals (P2 first-delivery, P3 mesh-delivery-rate deficit, P4 invalid-count,
P6 IP colocation, validator anchor bonus) feed selection and graylist. We already have most of
this machinery latent — `peer_score`, `tree_promote_delta`/`tree_demote_delta`, the lazy-vs-eager
peer split. The pieces missing in production: explicit GRAFT/PRUNE wire messages, mesh-rate
deficit measurement, the heartbeat refill cadence.

**4. Defending pull-leech.**
The hardest unsolved attack: an adversary that sends Request to all peers on Publish without
ever serving a Piece back. plum-pull at 50% pull-leech inflates honest validator upload from 1.0×
to **20.9×** the body. The existing scoring is reactive to *timeouts* — leeches that don't
attract pulls (no Have) never trigger any signal. A serve-cost / tit-for-tat gate recovers a
chunk of the inflation but causes false-positive blacklists on the asymmetric pull-only path.
A fundamentally better defence likely needs either score gossip (a peer that I detect can be
propagated), per-IP rate limits, or an authenticated-forwarder tier that pull-leeches can't
join.

**5. Cross-overlay body dedup via content-addressed id.**
A node in K overlays today receives a popular body K times because each overlay computes its
own broadcast id. With `body_hash` as the id and a single global `delivered` LRU, the body
travels once. Easy ~K× win for nodes in multiple overlays. (Cert/proof messages still go per
overlay.)

**6. Cert-then-body split (FIBRE insight).**
FIBRE's lesson: emit fast on a curated tier (fast-sync = our validators+privileged), let public
overlay catch up via separate downstream relay rather than parallel emission. Today we publish
to public, fast-sync, and custom in parallel — the fast-sync latency advantage is mostly wasted.
Restructure to: cert (small, validator-signed) flooded everywhere; body emitted into fast-sync
once and re-broadcast into public ~100 ms later by bridge nodes (FIBRE-style).

**7. Compact-block-style "don't send what they have".**
Bitcoin BIP-152 ships block bodies as ~6-byte short-IDs since most txns are already in the
recipient's mempool. For TON candidate-block broadcast, validators already see the same external
messages and historical state — a candidate body could be much smaller if we transmit only
deltas. Out of scope for V2 today but a clear future direction.

### Private overlay (≤16–100 nodes, all members trusted)

**1. `mesh = members` for small overlays.**
Below ~24 members the general mesh maintenance is overkill. Auto-trigger when
`|members| ≤ 2 × D_high` (= 24): mesh = members, skip GRAFT/PRUNE entirely, skip scoring
(members are trusted). Today this is the twostep family; the missing piece is the auto-trigger
on overlay size.

**2. Twostep transport for intra-mesh chunk distribution.**
TwostepFec already distributes chunks 1-hop from publisher to mesh peers. The natural
generalisation: each mesh peer receives a *different* chunk, then mesh peers exchange
within-mesh. Publisher upload drops to one body-equivalent (instead of `|mesh| × body`), evenly
distributed across mesh peers. Direct fit with our existing twostep machinery — change is
mostly in chunk-assignment logic.

**3. Latency-aware peer selection.**
Validators already publish RTT to neighbours via the ping path. A mild latency bonus in the
score (e.g., `+1` for RTT < 50 ms, `0` for < 200 ms, `-0.5` otherwise) nudges mesh edges
toward low-RTT peers. Risk: geographic partitioning — counter is to draw `D_out` edges from
unfiltered peers and to apply a `+5` validator-anchor bonus that overrides the latency
penalty.

**4. Adaptive D as overlay size grows.**
`target_D(|members|)` — small overlays use mesh=all, large use D=8, very large use higher D
to keep diameter bounded. Same code path; the only knob is the size threshold.

**5. Reuse the public-overlay scoring substrate.**
Even though private members are trusted, scoring still helps prefer healthy peers (low RTT,
high success rate) over flapping ones. The existing `BroadcastPeerScore` table works as-is.

### Cross-cutting

- **Replay window** (±20 s via the `date` field) and per-peer inbound rate limits are already
  in production; the simulator doesn't model them yet.
- **Observability**: mesh-size histogram, GRAFT/PRUNE counts, per-peer score histogram,
  per-(IP,/24) cohort size — feed dashboards before any new mode rolls out.

## Known caveats from the simulator

- Latency model is great-circle × 1.5 detour + 5 ms baseline; absolute p95/last numbers are
  ±30%. Relative ordering between algorithms is robust.
- bsim doesn't model packet loss or congestion control — only FIFO upload bandwidth. For
  correctness/regression that's fine; for absolute throughput numbers, less so.
- `min_vertex_cut` of the mainnet graph is 1 (one leaf node); the bulk has core ≥ 28. Reach
  numbers below 100% under no-leech are usually that one leaf. The dense core dominates
  algorithmic comparisons.

## See also

- [broadcast.md](broadcast.md) — research log: theoretical floor, twostep, prod-FEC,
  plumtree, gossip-mask, optimum-p2p compared on mainnet topology.
