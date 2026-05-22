#!/usr/bin/env python3
"""Geolocate overlay peers and write a latency matrix input.

The output shape is what mainnet-loader.cpp expects: nodes keyed by adnl_id hex.
"""
import argparse
import json
import math
import time
import urllib.request

PEERS = "/tmp/overlay-peers.json"
OUT = "/tmp/latency_matrix.json"
BATCH = "http://ip-api.com/batch"
SPEED_KM_PER_MS = 200.0
DETOUR = 1.5
BASELINE_MS = 5.0


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def rtt_ms(d_km):
    return 2.0 * (d_km * DETOUR / SPEED_KM_PER_MS + BASELINE_MS)


def geolocate(ips):
    out = {}
    for i in range(0, len(ips), 100):
        chunk = ips[i:i + 100]
        body = json.dumps([{"query": ip, "fields": "status,query,lat,lon,country,as"}
                          for ip in chunk]).encode()
        req = urllib.request.Request(BATCH, data=body,
                                     headers={"Content-Type": "application/json"})
        rs = None
        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=30) as r:
                    rs = json.loads(r.read())
                break
            except Exception as e:
                print(f"batch {i // 100} attempt {attempt} failed: {e}")
                time.sleep(2)
        if rs is None:
            continue
        ok = 0
        for entry in rs:
            if entry.get("status") == "success":
                out[entry["query"]] = (entry["lat"], entry["lon"],
                                       entry.get("country"), entry.get("as"))
                ok += 1
        print(f"batch {i // 100 + 1}/{(len(ips) + 99) // 100}: ok={ok}/{len(rs)}")
        time.sleep(1.4)
    return out


def first_udp_ip(peer):
    for addr in peer.get("addresses") or []:
        if addr.get("type") == "udp" and addr.get("ip"):
            return addr["ip"]
    return None


def load_cached_ip_geo(path):
    if path is None:
        return {}
    with open(path) as f:
        nodes = json.load(f).get("nodes", {})
    out = {}
    for info in nodes.values():
        ip = info.get("ip")
        if ip is None:
            continue
        if info.get("lat") is None or info.get("lon") is None:
            continue
        out[ip] = (info["lat"], info["lon"], info.get("country"), info.get("as"))
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--peers", default=PEERS, help="input overlay peers JSON")
    parser.add_argument("--out", default=OUT, help="output latency/geolocation JSON")
    parser.add_argument("--cache", help="existing latency JSON to reuse by IP")
    args = parser.parse_args()

    with open(args.peers) as f:
        d = json.load(f)
    peers = d["peers"]

    node_ip = {}
    for p in peers:
        ip = first_udp_ip(p)
        if ip is not None:
            node_ip[p["adnl_id"]] = ip
    ips = sorted(set(node_ip.values()))
    print(f"peers={len(peers)} with-ip={len(node_ip)} unique-ips={len(ips)}")

    geo = load_cached_ip_geo(args.cache)
    cached = len(set(geo) & set(ips))
    missing_ips = [ip for ip in ips if ip not in geo]
    print(f"cached: {cached}/{len(ips)} need-query={len(missing_ips)}")
    geo.update(geolocate(missing_ips))
    covered_ips = len(set(geo) & set(ips))
    print(f"geolocated: {covered_ips}/{len(ips)}")

    nodes_out = {}
    for h, ip in node_ip.items():
        if ip not in geo:
            continue
        lat, lon, country, asn = geo[ip]
        nodes_out[h] = {"ip": ip, "lat": lat, "lon": lon,
                        "country": country or "", "as": asn or ""}

    rtts = []
    for a in nodes_out:
        for b in nodes_out:
            if b <= a:
                continue
            d_km = haversine_km(nodes_out[a]["lat"], nodes_out[a]["lon"],
                                nodes_out[b]["lat"], nodes_out[b]["lon"])
            rtts.append(rtt_ms(d_km))
    rtts.sort()
    if rtts:
        n = len(rtts)
        print(f"pairs={n} rtt_ms p05={rtts[n // 20]:.1f} "
              f"p50={rtts[n // 2]:.1f} p95={rtts[int(n * 0.95)]:.1f}")

    json.dump({"nodes": nodes_out}, open(args.out, "w"))
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
