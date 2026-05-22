#!/usr/bin/env bash
# Prepare the mainnet topology inputs for `broadcast-bench --graph=mainnet`.
#
# Expects:
#   * /tmp/overlay-peers.json — overlay dump from the broadcast-simulator crawler
#     (a JSON object with .peers[].adnl_id, .recent_neighbours, .addresses[].ip).
#     Get this from a long-lived validator running the public-overlay simulator;
#     example dumps live in https://github.com/ton-blockchain/public-overlay.
#
# Produces:
#   * /tmp/latency_matrix.json — adnl_id → {ip, lat, lon, country, as}, geolocated
#     via ip-api.com. Cached: re-runs only query IPs missing from any --cache file.
#
# After this script, run:
#   broadcast-bench --graph=mainnet --peers=/tmp/overlay-peers.json \
#       --body-size=1048576 --rounds=10 --seeds=3 --view=algo

set -e
cd "$(dirname "$0")"

PEERS="${PEERS:-/tmp/overlay-peers.json}"
LATENCY="${LATENCY:-/tmp/latency_matrix.json}"
CACHE="${CACHE:-}"

if [ ! -f "$PEERS" ]; then
  cat >&2 <<EOF
error: $PEERS not found.

You need an overlay-peers JSON dump. Either:
  1. Run the public-overlay broadcast-simulator crawler and copy its output here, or
  2. Use an existing snapshot, e.g.:
       cp /path/to/overlay-peers.sim.json $PEERS

Then re-run this script.
EOF
  exit 1
fi

if [ -f "$LATENCY" ] && [ -z "$CACHE" ]; then
  CACHE="$LATENCY"
fi

CACHE_FLAG=()
if [ -n "$CACHE" ] && [ -f "$CACHE" ]; then
  CACHE_FLAG=(--cache "$CACHE")
fi

python3 build_latency_matrix.py --peers "$PEERS" --out "$LATENCY" "${CACHE_FLAG[@]}"

cat <<EOF

Topology ready. Run a benchmark with:

  broadcast-bench --graph=mainnet --peers=$PEERS \\
      --latency=$LATENCY \\
      --body-size=1048576 --rounds=10 --seeds=3 --view=algo

Add --csv=/tmp/bench.csv and then:

  python3 render_bench.py --csv /tmp/bench.csv --out /tmp/report.html
EOF
