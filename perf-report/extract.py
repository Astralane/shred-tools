#!/usr/bin/env python3
"""Shred-source performance extraction.

Streams the last WINDOW_HOURS of shred_arrivals (competition samples) from the
metrics DB hour-by-hour and tallies per-source/per-hub stats, head-to-head
wins, per-leader-provider splits (leader schedule fetched live from Solana
RPC), plus a 7-day daily trend, first_provider wins, and IP coverage.
Writes CSVs to OUT_DIR, consumed by build_xlsx.py / build_leader_xlsx.py.

delta_ns semantics: arrival time relative to the jito baseline source on the
same hub for the same FEC set; negative = earlier than jito.

Environment:
  SHRED_METRICS_URL  required  postgres URL of the shred_metrics DB
  ASTRALANE_DB_URL   required  postgres URL of astralane_db (shred_providers)
  MERRY_DB_URL       optional  postgres URL of the merry test-hub DB;
                               enables the astra-vs-lucky-vs-jito section
  RPC_URL            optional  Solana RPC (default mainnet-beta)
  OUT_DIR            optional  output dir for CSVs (default ./data)
  WINDOW_HOURS       optional  main analysis window (default 24)

Usage: extract.py [--merry-only]
"""

import csv
import datetime as dt
import json
import os
import sys
from collections import defaultdict

import psycopg
import requests

SM = os.environ["SHRED_METRICS_URL"]
AS = os.environ["ASTRALANE_DB_URL"]
MERRY = os.environ.get("MERRY_DB_URL")
RPC = os.environ.get("RPC_URL", "https://api.mainnet-beta.solana.com")
OUT_DIR = os.environ.get("OUT_DIR", "./data")
WINDOW_H = int(os.environ.get("WINDOW_HOURS", "24"))

# Leader-provider identity pubkeys. Confirmed = supplied by providers;
# the rest were matched from on-chain validator-info by brand name.
PROVIDER_PUBKEYS = {
    "everstake": ["EvnRmnMrd69kFdbLMxWkTn1icZ7DCceRhvmb2SJXqDo4"],  # confirmed
    "p2p.org": [
        # confirmed ("one of these")
        "DWvDTSh3qfn88UoQTEKRV2JnLt5jtJAVoiCo3ivtMwXP",
        "G1bLKfyNm7zsmmYEL9dyxBvMtxpFcwy2s84bHDj2ZFUY",
        "8ZQg3K1V1Z2BVJkjmnxpi43WKhjPGXphzu5QmBkJibSP",
        # extra on-chain validator-info matches (P2P.org brand)
        "FopTvQaGp6K5FadWKZtsLJmrX7gnNGFS2fQ7rv5KHyE1",
        "H13uDKDbPxv7zsh6zk6APQniBUi4tYW2HWkmMmWNJCWx",
    ],
    "lucky-stake": [
        "4Q9edSNUK5YZFDEjhnHoBmEZAxE2QnsVaHhZ3qhQsQRG",
        "TiMxX1yasS4CiGyRcnn7sy9T2fvaNdFpkf8tFDhhDkG",
    ],
    "twinstake": [
        "GoeW4aFK4dGoekJySgUynWDxBZiQJqm8GDAF4H53tDK9",  # confirmed
        "3tzpLMWRkWucvTRWU5PjgKzN1iwJuV69yCCjmuuo4gTk",  # TruFin by Twinstake
    ],
    "jupiter": [
        "JupmVLmA8RoyTUbTMMuTtoPWHEiNQobxgTeGTrPNkzT",  # confirmed
        "JupRhwjrF5fAcs6dFhLH59r3TJFvbcyLP2NRM8UGH9H",
    ],
    "thor": ["BEx3ZzH9cswWcJr3BcKg37rRURHmZPW98XY3RkXDprN4"],
    "dawn-labs": ["4k6wgP5WPBKQpsFGtzuXNrjcTE2fKWLj17nDvFeG5zSF"],
    "asymmetric": ["Certusm1sa411sMpV9FPqU5dXAYhmmhygvxJ23S6hJ24"],
    # Jito Labs' own validators (vanity J1to* identities) — small leaders.
    # The "12M SOL" figure is JitoSOL pool TVL delegated across third-party
    # validators, not these identities.
    "jito": [
        "CXPeim1wQMkcTvEHx9QdhgKREYYJD8bnaCCqPRwJ1to1",
        "A4hyMd3FyvUJSRafDUSwtLLaQcxRP4r1BRC9w2AJ1to2",
        "23U4mgK9DMCxsv2StC4y2qAptP25Xv5b2cybKCeJ1to3",
        "3DwujerKfjNe6Hie3924WQMJQaheSzEY79YmykaJ1to4",
    ],
    "staking-facilities": [
        "Awes4Tr6TX8JDzEhCZY2QVNimT6iD1zWHzf1vNyGvpLM",  # confirmed
        "73hojLdq1vZDSxeVQEqVFJ4iwLngdvEJPEpEHkSdv6BZ",  # SuperteamDE
        "8JPFVBhqntSsA3XJ15EWBzu4iB6aAwrqA9GSm3nTj8zx",  # Lido / SF
    ],
}


def brand_of(source):
    """Map a source name to its provider brand (for own-slot comparison)."""
    s = source.lower()
    for prefix, brand in (("everstake", "everstake"), ("lucky", "lucky-stake"),
                          ("twinstake", "twinstake"), ("jupiter", "jupiter"),
                          ("thor", "thor"), ("dawn", "dawn-labs"),
                          ("assymetric", "asymmetric"),
                          ("asymmetric", "asymmetric"),
                          ("staking-fac", "staking-facilities"),
                          ("p2p", "p2p.org"), ("jito", "jito"),
                          ("soyas", "soyas"), ("soldiver", "soldiver")):
        if s.startswith(prefix):
            return brand
    return "other"


def rpc_call(method, params=None):
    r = requests.post(RPC, json={"jsonrpc": "2.0", "id": 1, "method": method,
                                 "params": params or []}, timeout=60)
    r.raise_for_status()
    body = r.json()
    if "error" in body:
        raise RuntimeError(f"{method}: {body['error']}")
    return body["result"]


def fetch_leaders():
    """slot -> leader identity for the current and previous epoch."""
    info = rpc_call("getEpochInfo")
    epoch = info["epoch"]
    start = info["absoluteSlot"] - info["slotIndex"]
    slots_per = info["slotsInEpoch"]
    leaders = {}
    epochs = []
    for ep, ep_start in ((epoch - 1, start - slots_per), (epoch, start)):
        sched = rpc_call("getLeaderSchedule", [ep_start])
        if sched is None:
            continue
        epochs.append(ep)
        for leader, idxs in sched.items():
            for i in idxs:
                leaders[ep_start + i] = leader
        print(f"leader schedule epoch {ep}: "
              f"{sum(len(v) for v in sched.values())} slots", flush=True)
    return leaders, epochs


class SrcStats:
    __slots__ = ("n", "sum_d", "min_d", "max_d", "wins", "sum_rank", "hist", "neg")

    BIN_NS = 20_000          # 20 us histogram bins (percentile resolution)
    LO = -100_000_000        # -100 ms
    HI = 100_000_000         # +100 ms

    def __init__(self):
        self.n = 0
        self.sum_d = 0
        self.min_d = None
        self.max_d = None
        self.wins = 0
        self.sum_rank = 0
        self.neg = 0
        self.hist = defaultdict(int)

    def add(self, d, rank):
        self.n += 1
        self.sum_d += d
        self.min_d = d if self.min_d is None else min(self.min_d, d)
        self.max_d = d if self.max_d is None else max(self.max_d, d)
        if rank == 0:
            self.wins += 1
        self.sum_rank += rank
        if d < 0:
            self.neg += 1
        b = (max(self.LO, min(self.HI - 1, d)) - self.LO) // self.BIN_NS
        self.hist[b] += 1

    def pct(self, q):
        if not self.n:
            return None
        target = q * self.n
        c = 0
        for b in sorted(self.hist):
            c += self.hist[b]
            if c >= target:
                return (self.LO + (b + 0.5) * self.BIN_NS) / 1e6  # ms
        return None


def ms(ns):
    return None if ns is None else round(ns / 1e6, 4)


def dedup_sources(srcs, deltas):
    """Collapse repeated sources to their first (min-delta) row.

    Hubs deployed before 2026-06-10 recorded every shred arrival (many rows
    per source per FEC set); arrays are delta-sorted, so the first occurrence
    of a source is its earliest arrival.
    """
    if len(srcs) == len(set(srcs)):
        return srcs, deltas
    seen = set()
    ded_s, ded_d = [], []
    for s, d in zip(srcs, deltas):
        if s not in seen:
            seen.add(s)
            ded_s.append(s)
            ded_d.append(d)
    return ded_s, ded_d


def write_per_source_csv(path, overall, fec_sets):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["hub", "source", "samples", "win_count", "win_rate_pct",
                    "avg_rank", "avg_delta_ms", "p50_ms", "p90_ms", "p99_ms",
                    "min_ms", "max_ms", "beats_jito_pct", "fec_set_coverage_pct"])
        for (hub, s), st in sorted(overall.items(),
                                   key=lambda kv: (kv[0][0], -kv[1].wins)):
            w.writerow([hub, s, st.n, st.wins,
                        round(100 * st.wins / fec_sets[hub], 2),
                        round(st.sum_rank / st.n, 2),
                        ms(st.sum_d / st.n),
                        st.pct(0.5) and round(st.pct(0.5), 3),
                        st.pct(0.9) and round(st.pct(0.9), 3),
                        st.pct(0.99) and round(st.pct(0.99), 3),
                        ms(st.min_d), ms(st.max_d),
                        round(100 * st.neg / st.n, 2),
                        round(100 * st.n / fec_sets[hub], 2)])


def write_h2h_csv(path, h2h, h2h_n):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["hub", "source_a", "source_b", "both_present",
                    "a_first_count", "a_first_pct"])
        for (hub, a, b), n_ab in sorted(h2h.items()):
            n_total = h2h_n[(hub, a, b)]
            w.writerow([hub, a, b, n_total, n_ab,
                        round(100 * n_ab / n_total, 2) if n_total else None])


def merry_stats():
    """astra vs lucky vs jito (and the rest) on the merry-gar test hub."""
    hub = "merry-gar"
    conn = psycopg.connect(MERRY)
    overall = defaultdict(SrcStats)
    h2h = defaultdict(int)
    h2h_n = defaultdict(int)
    n_fec = 0
    cur = conn.cursor(name="merry")
    cur.itersize = 5000
    cur.execute("""
        SELECT slot, fec_set_index,
               array_agg(source   ORDER BY delta_ns, source),
               array_agg(delta_ns ORDER BY delta_ns, source)
        FROM shred_arrivals
        WHERE t > now() - interval '24 hours' AND delta_ns IS NOT NULL
        GROUP BY 1, 2
    """)
    for slot, fec, srcs, deltas in cur:
        srcs, deltas = dedup_sources(srcs, deltas)
        n_fec += 1
        for rank, (s, d) in enumerate(zip(srcs, deltas)):
            overall[(hub, s)].add(d, rank)
        for i, a in enumerate(srcs):
            for bsrc in srcs[i + 1:]:
                h2h[(hub, a, bsrc)] += 1
                h2h_n[(hub, a, bsrc)] += 1
                h2h_n[(hub, bsrc, a)] += 1
    conn.close()
    write_per_source_csv(f"{OUT_DIR}/merry_per_source_24h.csv",
                         overall, {hub: n_fec})
    write_h2h_csv(f"{OUT_DIR}/merry_h2h_24h.csv", h2h, h2h_n)
    print(f"merry hub: {n_fec} fec sets", flush=True)


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    if MERRY:
        merry_stats()
    if "--merry-only" in sys.argv:
        return
    leaders, epochs = fetch_leaders()

    conn = psycopg.connect(SM)
    now = dt.datetime.now(dt.timezone.utc)
    t_start = now - dt.timedelta(hours=WINDOW_H)

    overall = defaultdict(SrcStats)            # (hub, source)
    h2h = defaultdict(int)                     # (hub, a, b) -> a-before-b
    h2h_n = defaultdict(int)                   # (hub, a, b) -> both present
    fec_sets = defaultdict(int)                # hub -> number of fec sets
    lsplit = defaultdict(lambda: [0, 0, 0])    # (hub, leader_prov, source)
    lslots = defaultdict(set)                  # (hub, leader_prov) -> slots
    lfec = defaultdict(int)                    # (hub, leader_prov) -> fec sets
    own = defaultdict(lambda: [0, 0, 0])       # (hub, source, own/other)
    unknown_leader_slots = set()

    pub2prov = {}
    for prov, keys in PROVIDER_PUBKEYS.items():
        for k in keys:
            pub2prov[k] = prov

    # Stream hour-by-hour to keep server-side sorts small. A FEC set whose
    # arrivals straddle a chunk boundary is taken from the first chunk only
    # (prev_keys dedup).
    prev_keys = set()
    rows_seen = 0
    for h in range(WINDOW_H):
        c0 = t_start + dt.timedelta(hours=h)
        c1 = c0 + dt.timedelta(hours=1)
        cur = conn.cursor(name=f"cur{h}")
        cur.itersize = 5000
        cur.execute(
            """
            SELECT hub, slot, fec_set_index,
                   array_agg(source   ORDER BY delta_ns, source),
                   array_agg(delta_ns ORDER BY delta_ns, source)
            FROM shred_arrivals
            WHERE t >= %s AND t < %s AND delta_ns IS NOT NULL
            GROUP BY 1, 2, 3
            """,
            (c0, c1),
        )
        cur_keys = set()
        for hub, slot, fec, srcs, deltas in cur:
            key = (hub, slot, fec)
            cur_keys.add(key)
            if key in prev_keys:
                continue
            srcs, deltas = dedup_sources(srcs, deltas)
            rows_seen += 1
            fec_sets[hub] += 1
            leader = leaders.get(slot)
            lprov = pub2prov.get(leader) if leader else None
            if leader is None:
                unknown_leader_slots.add(slot)
            if lprov:
                lslots[(hub, lprov)].add(slot)
                lfec[(hub, lprov)] += 1
            for rank, (s, d) in enumerate(zip(srcs, deltas)):
                overall[(hub, s)].add(d, rank)
                if lprov:
                    st = lsplit[(hub, lprov, s)]
                    st[0] += 1
                    st[1] += d
                    if rank == 0:
                        st[2] += 1
                b = brand_of(s)
                if b in PROVIDER_PUBKEYS:
                    side = "own" if lprov == b else "other"
                    st = own[(hub, s, side)]
                    st[0] += 1
                    st[1] += d
                    if rank == 0:
                        st[2] += 1
            for i, a in enumerate(srcs):
                for bsrc in srcs[i + 1:]:
                    h2h[(hub, a, bsrc)] += 1
                    h2h_n[(hub, a, bsrc)] += 1
                    h2h_n[(hub, bsrc, a)] += 1
        prev_keys = cur_keys
        cur.close()
        print(f"hour {h + 1}/{WINDOW_H} done, fec sets so far: {rows_seen}",
              flush=True)

    write_per_source_csv(f"{OUT_DIR}/per_source_24h.csv", overall, fec_sets)
    write_h2h_csv(f"{OUT_DIR}/head_to_head_24h.csv", h2h, h2h_n)

    with open(f"{OUT_DIR}/leader_slots_24h.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["hub", "leader_provider", "slots_seen", "fec_sets",
                    "source", "samples", "wins", "win_rate_pct", "avg_delta_ms"])
        for (hub, lp, s), (n, sum_d, wins) in sorted(lsplit.items()):
            w.writerow([hub, lp, len(lslots[(hub, lp)]), lfec[(hub, lp)], s,
                        n, wins, round(100 * wins / lfec[(hub, lp)], 2),
                        ms(sum_d / n)])

    with open(f"{OUT_DIR}/own_vs_other_24h.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["hub", "source", "slots_kind", "samples", "wins",
                    "win_rate_of_samples_pct", "avg_delta_ms"])
        for (hub, s, side), (n, sum_d, wins) in sorted(own.items()):
            w.writerow([hub, s, side, n, wins,
                        round(100 * wins / n, 2), ms(sum_d / n)])

    with conn.cursor() as cur:
        cur.execute("""
            SELECT date_trunc('day', t)::date AS day, hub, source,
                   count(*), round(avg(delta_ns) / 1e6, 4),
                   round((100.0 * count(*) FILTER (WHERE delta_ns < 0))
                         / count(*), 2)
            FROM shred_arrivals
            WHERE t > now() - interval '7 days' AND delta_ns IS NOT NULL
            GROUP BY 1, 2, 3 ORDER BY 1, 2, 5
        """)
        with open(f"{OUT_DIR}/daily_trend_7d.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["day", "hub", "source", "samples", "avg_delta_ms",
                        "beats_jito_pct"])
            for row in cur:
                w.writerow(row)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT hub, src_ip, count(*), min(t), max(t)
            FROM shred_first_provider GROUP BY 1, 2 ORDER BY 1, 3 DESC
        """)
        fp = cur.fetchall()
    totals = defaultdict(int)
    for hub, ip, n, *_ in fp:
        totals[hub] += n
    with psycopg.connect(AS) as aconn, aconn.cursor() as cur:
        cur.execute("SELECT src_ip, name, tier FROM shred_providers")
        ipmap = {}
        for ip, name, tier in cur.fetchall():
            ipmap.setdefault(ip, []).append((name, tier))
    with open(f"{OUT_DIR}/first_provider.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["hub", "src_ip", "provider_names", "first_count",
                    "first_share_pct", "window_start", "window_end"])
        for hub, ip, n, t0, t1 in fp:
            names = ";".join(sorted({nm for nm, _ in ipmap.get(ip, [])})) \
                or "UNREGISTERED"
            w.writerow([hub, ip, names, n, round(100 * n / totals[hub], 2),
                        t0, t1])

    # IPs whose presence we were explicitly asked to track.
    interest = {
        "p2p": ["170.23.153.203", "31.172.68.134", "170.23.209.132",
                "103.88.233.89", "170.23.153.105", "103.88.233.87",
                "170.23.153.201"],
        "twinstake": ["155.2.223.11", "64.130.52.48"],
        "jupiter": ["88.216.222.171", "185.191.117.239", "64.130.41.46",
                    "64.130.57.50"],
    }
    seen_sources = {s for (_, s) in overall}
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT src_ip FROM shred_first_provider")
        fp_ips = {r[0] for r in cur.fetchall()}
    with open(f"{OUT_DIR}/ip_coverage.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["group", "ip", "registered_as", "tier",
                    "arrivals_data_24h", "first_provider_data", "note"])
        for grp, ips in interest.items():
            for ip in ips:
                regs = ipmap.get(ip, [])
                reg_names = ";".join(sorted({n for n, _ in regs}))
                tiers = ";".join(sorted({t for _, t in regs}))
                has_arr = any(nm in seen_sources for nm, _ in regs)
                note = ""
                if not regs:
                    note = ("NOT REGISTERED in shred_providers -> "
                            "provider-guard drops it; NO DATA")
                elif not has_arr and ip not in fp_ips:
                    note = "registered but no samples in window"
                w.writerow([grp, ip, reg_names or "-", tiers or "-",
                            "yes" if has_arr else "no",
                            "yes" if ip in fp_ips else "no", note])

    meta = {
        "generated_utc": now.isoformat(),
        "window_hours": WINDOW_H,
        "window": [t_start.isoformat(), now.isoformat()],
        "epochs": epochs,
        "fec_sets_per_hub": dict(fec_sets),
        "unknown_leader_slots": len(unknown_leader_slots),
        "provider_pubkeys": PROVIDER_PUBKEYS,
    }
    with open(f"{OUT_DIR}/meta.json", "w") as f:
        json.dump(meta, f, indent=2, default=str)
    print("DONE", json.dumps(meta["fec_sets_per_hub"]), flush=True)


if __name__ == "__main__":
    main()
