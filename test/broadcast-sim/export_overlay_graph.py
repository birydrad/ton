#!/usr/bin/env python3
"""Export the crawler overlay dump as a simple directed latency graph.

Input:
  * /tmp/overlay-peers.json   -- /api/overlay-peers dump
  * /tmp/latency_matrix.json  -- geolocated nodes produced by build_latency_matrix.py

Output format is intentionally boring: nodes are indexed by integer id, edges
are directed, and every edge carries one-way latency in milliseconds.
"""

import argparse
import json
import math
from pathlib import Path


DEFAULT_PEERS = "/tmp/overlay-peers.json"
DEFAULT_LATENCY = "/tmp/latency_matrix.json"
DEFAULT_OUT = "/tmp/broadcast-graph.json"
DEFAULT_RECENT_NEIGHBOUR_LIMIT = 20
DEFAULT_MIN_OUT_DEGREE = 0

SPEED_KM_PER_MS = 200.0
DETOUR = 1.5
BASELINE_MS = 5.0
FALLBACK_LATENCY_MS = 50.0


def haversine_km(a, b):
    lat1, lon1 = a
    lat2, lon2 = b
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    x = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(x))


def one_way_latency_ms(src, dst):
    return haversine_km(src, dst) * DETOUR / SPEED_KM_PER_MS + BASELINE_MS


def load_json(path):
    with open(path) as f:
        return json.load(f)


def first_udp_ip(peer):
    for addr in peer.get("addresses") or []:
        if addr.get("type") == "udp" and addr.get("ip"):
            return addr["ip"]
    return ""


def fec_last_seen(peer):
    fec = peer.get("fec")
    if isinstance(fec, dict):
        return fec.get("last_seen")
    return None


def build_graph(peers_path, latency_path, recent_neighbour_limit, include_empty_neighbours):
    peers_root = load_json(peers_path)
    latency_root = load_json(latency_path)
    peers = peers_root["peers"]
    geo_nodes = latency_root.get("nodes", {})
    is_simulator_graph = bool(peers_root.get("simulator_graph"))
    include_empty_neighbours = include_empty_neighbours or is_simulator_graph

    vertices = []
    raw_neighbours = []
    id_by_adnl = {}
    for peer in peers:
        neighbours = peer.get("recent_neighbours") or []
        if not neighbours and not include_empty_neighbours:
            continue
        adnl_id = peer["adnl_id"]
        id_by_adnl[adnl_id] = len(vertices)
        geo = geo_nodes.get(adnl_id, {})
        last_seen = fec_last_seen(peer)
        vertices.append(
            {
                "id": len(vertices),
                "adnl_id": adnl_id,
                "ip": geo.get("ip") or first_udp_ip(peer),
                "lat": geo.get("lat"),
                "lon": geo.get("lon"),
                "country": geo.get("country", ""),
                "as": geo.get("as", ""),
                "leech": last_seen is None,
                "fec_last_seen": last_seen,
            }
        )
        if recent_neighbour_limit > 0:
            neighbours = neighbours[:recent_neighbour_limit]
        raw_neighbours.append(neighbours)

    edges = []
    for src, neighbours in enumerate(raw_neighbours):
        seen = set()
        src_node = vertices[src]
        src_has_geo = src_node["lat"] is not None and src_node["lon"] is not None
        for peer_adnl in neighbours:
            dst = id_by_adnl.get(peer_adnl)
            if dst is None or dst == src or dst in seen:
                continue
            seen.add(dst)
            dst_node = vertices[dst]
            dst_has_geo = dst_node["lat"] is not None and dst_node["lon"] is not None
            if src_has_geo and dst_has_geo:
                latency_ms = one_way_latency_ms((src_node["lat"], src_node["lon"]), (dst_node["lat"], dst_node["lon"]))
            else:
                latency_ms = FALLBACK_LATENCY_MS
            edges.append({"from": src, "to": dst, "latency_ms": round(latency_ms, 3)})

    return {
        "format": "ton-broadcast-graph-v1",
        "directed": True,
        "latency_unit": "ms",
        "latency_kind": "one_way_geo_estimate",
        "fallback_latency_ms": FALLBACK_LATENCY_MS,
        "source": {
            "peers": str(peers_path),
            "latency": str(latency_path),
            "edge_source": "recent_neighbours",
            "simulator_graph": is_simulator_graph,
            "source_recent_neighbour_limit": peers_root.get("recent_neighbour_limit"),
            "recent_neighbour_limit": recent_neighbour_limit,
            "filter_ttl_seconds": peers_root.get("filter_ttl_seconds"),
            "updated_at": peers_root.get("updated_at"),
            "filtered_at": peers_root.get("filtered_at"),
            "leech_policy": "missing fec.last_seen",
        },
        "node_count": len(vertices),
        "edge_count": len(edges),
        "nodes": vertices,
        "edges": edges,
    }


def largest_scc(edges, active_nodes):
    active = set(active_nodes)
    if not active:
        return set()

    adj = {node: [] for node in active}
    rev = {node: [] for node in active}
    for edge in edges:
        src = edge["from"]
        dst = edge["to"]
        if src in active and dst in active:
            adj[src].append(dst)
            rev[dst].append(src)

    seen = set()
    order = []
    for node in active:
        if node in seen:
            continue
        stack = [(node, 0)]
        seen.add(node)
        while stack:
            cur, index = stack[-1]
            if index < len(adj[cur]):
                nxt = adj[cur][index]
                stack[-1] = (cur, index + 1)
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append((nxt, 0))
            else:
                order.append(cur)
                stack.pop()

    components = []
    assigned = set()
    for node in reversed(order):
        if node in assigned:
            continue
        component = []
        stack = [node]
        assigned.add(node)
        while stack:
            cur = stack.pop()
            component.append(cur)
            for nxt in rev[cur]:
                if nxt not in assigned:
                    assigned.add(nxt)
                    stack.append(nxt)
        components.append(component)

    return set(max(components, key=len))


def prune_low_out_degree(node_count, edges, active_nodes, min_out_degree):
    active = set(active_nodes)
    if min_out_degree <= 0:
        return active
    while True:
        out_degree = [0] * node_count
        for edge in edges:
            src = edge["from"]
            dst = edge["to"]
            if src in active and dst in active:
                out_degree[src] += 1
        remove = {node for node in active if out_degree[node] < min_out_degree}
        if not remove:
            return active
        active -= remove


def filter_graph(graph, keep_largest_scc, min_out_degree):
    node_count = graph["node_count"]
    edges = graph["edges"]
    active = set(range(node_count))
    original_node_count = len(active)
    original_edge_count = len(edges)

    if keep_largest_scc:
        active = largest_scc(edges, active)

    while True:
        before = set(active)
        active = prune_low_out_degree(node_count, edges, active, min_out_degree)
        if keep_largest_scc:
            active = largest_scc(edges, active)
        if active == before:
            break

    remap = {old_id: new_id for new_id, old_id in enumerate(sorted(active))}
    nodes = []
    for old_id in sorted(active):
        node = dict(graph["nodes"][old_id])
        node["id"] = remap[old_id]
        nodes.append(node)

    filtered_edges = []
    for edge in edges:
        src = edge["from"]
        dst = edge["to"]
        if src in remap and dst in remap:
            item = dict(edge)
            item["from"] = remap[src]
            item["to"] = remap[dst]
            filtered_edges.append(item)

    graph = dict(graph)
    graph["nodes"] = nodes
    graph["edges"] = filtered_edges
    graph["node_count"] = len(nodes)
    graph["edge_count"] = len(filtered_edges)
    graph["filter"] = {
        "keep_largest_scc": keep_largest_scc,
        "min_out_degree": min_out_degree,
        "original_node_count": original_node_count,
        "original_edge_count": original_edge_count,
        "removed_node_count": original_node_count - len(nodes),
        "removed_edge_count": original_edge_count - len(filtered_edges),
    }
    return graph


def public_graph(graph):
    return {
        "format": graph["format"],
        "directed": graph["directed"],
        "latency_unit": graph["latency_unit"],
        "latency_kind": graph["latency_kind"],
        "fallback_latency_ms": graph["fallback_latency_ms"],
        "node_count": graph["node_count"],
        "edge_count": graph["edge_count"],
        "nodes": [{"id": node["id"], "leech": bool(node.get("leech"))} for node in graph["nodes"]],
        "edges": [
            {
                "from": edge["from"],
                "to": edge["to"],
                "latency_ms": edge["latency_ms"],
            }
            for edge in graph["edges"]
        ],
    }


def print_summary(graph):
    degrees = [0] * graph["node_count"]
    latencies = []
    leeches = 0
    fallback_edges = 0
    for edge in graph["edges"]:
        degrees[edge["from"]] += 1
        latencies.append(edge["latency_ms"])
        if edge["latency_ms"] == graph["fallback_latency_ms"]:
            fallback_edges += 1
    for node in graph["nodes"]:
        leeches += int(bool(node.get("leech")))
    latencies.sort()
    mean_degree = sum(degrees) / len(degrees) if degrees else 0.0
    max_degree = max(degrees) if degrees else 0

    def percentile(q):
        if not latencies:
            return 0.0
        return latencies[int(q * (len(latencies) - 1))]

    print(
        f"nodes={graph['node_count']} edges={graph['edge_count']} "
        f"leeches={leeches} "
        f"mean_out_degree={mean_degree:.1f} max_out_degree={max_degree} "
        f"latency_ms p50={percentile(0.50):.1f} p95={percentile(0.95):.1f} "
        f"fallback_edges={fallback_edges}"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--peers", default=DEFAULT_PEERS, help="input /api/overlay-peers JSON")
    parser.add_argument("--latency", default=DEFAULT_LATENCY, help="input geolocation JSON")
    parser.add_argument("--out", default=DEFAULT_OUT, help="output graph JSON")
    parser.add_argument("--indent", type=int, default=None, help="pretty-print JSON with this indent")
    parser.add_argument(
        "--recent-neighbour-limit",
        type=int,
        default=DEFAULT_RECENT_NEIGHBOUR_LIMIT,
        help="keep only this many freshest recent_neighbours per node; use 0 to keep all",
    )
    parser.add_argument(
        "--include-empty-neighbours",
        action="store_true",
        help="include nodes with an empty recent_neighbours list",
    )
    parser.add_argument(
        "--largest-scc",
        action="store_true",
        help="restrict the graph to its largest strongly connected component",
    )
    parser.add_argument(
        "--min-out-degree",
        type=int,
        default=DEFAULT_MIN_OUT_DEGREE,
        help="iteratively remove nodes with smaller out-degree after filtering",
    )
    parser.add_argument(
        "--public",
        action="store_true",
        help="omit raw crawler identifiers, IPs, geo data, timestamps, and local source paths",
    )
    args = parser.parse_args()

    graph = build_graph(
        Path(args.peers),
        Path(args.latency),
        args.recent_neighbour_limit,
        args.include_empty_neighbours,
    )
    graph = filter_graph(graph, args.largest_scc, args.min_out_degree)
    if args.public:
        graph = public_graph(graph)
    with open(args.out, "w") as f:
        json.dump(graph, f, indent=args.indent, sort_keys=False)
        f.write("\n")
    print(f"wrote {args.out}")
    print_summary(graph)


if __name__ == "__main__":
    main()
