#!/usr/bin/env python3
"""Per-source stats over signature-valid arrivals only, plus xlsx.

Valid = the arrival's signature equals the jito baseline's signature for the
same (hub, slot, fec_set_index). Jito baseline rows are delta_ns = 0 from the
jito relayer source on each hub. Also tallies, per source, how many arrivals
carried a DIFFERENT signature than jito (potential repacked/forged FEC sets)
and how many carried none.

Signatures exist in shred_arrivals since the 2026-06-10 hub deploy.

Environment:
  SHRED_METRICS_URL  required
  OUT_DIR            optional  default ./data (CSVs land here)

Writes: $OUT_DIR/sig_valid_per_source_24h.csv, $OUT_DIR/sig_mismatch_24h.csv
"""

import csv
import os
from collections import defaultdict

import psycopg

SM = os.environ["SHRED_METRICS_URL"]
OUT_DIR = os.environ.get("OUT_DIR", "./data")

BIN_NS = 20_000
LO, HI = -100_000_000, 100_000_000


class St:
    __slots__ = ("n", "sum_d", "wins", "sum_rank", "neg", "hist", "mism",
                 "nosig")

    def __init__(self):
        self.n = 0
        self.sum_d = 0
        self.wins = 0
        self.sum_rank = 0
        self.neg = 0
        self.hist = defaultdict(int)
        self.mism = 0
        self.nosig = 0

    def add(self, d, rank):
        self.n += 1
        self.sum_d += d
        if rank == 0:
            self.wins += 1
        self.sum_rank += rank
        if d < 0:
            self.neg += 1
        b = (max(LO, min(HI - 1, d)) - LO) // BIN_NS
        self.hist[b] += 1

    def pct(self, q):
        if not self.n:
            return None
        target, c = q * self.n, 0
        for b in sorted(self.hist):
            c += self.hist[b]
            if c >= target:
                return round((LO + (b + 0.5) * BIN_NS) / 1e6, 3)
        return None


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    conn = psycopg.connect(SM)
    stats = defaultdict(St)        # (hub, source)
    fec_sets = defaultdict(int)    # hub -> groups with a jito signature
    cur = conn.cursor(name="sig")
    cur.itersize = 5000
    cur.execute("""
        WITH jito AS (
            SELECT hub, slot, fec_set_index, signature
            FROM shred_arrivals
            WHERE t > now() - interval '24 hours'
              AND delta_ns = 0
              AND source IN ('jito-fra', 'jito-ny')
              AND signature IS NOT NULL
        )
        SELECT a.hub, a.slot, a.fec_set_index, max(j.signature),
               array_agg(a.source    ORDER BY a.delta_ns, a.source),
               array_agg(a.delta_ns  ORDER BY a.delta_ns, a.source),
               array_agg(coalesce(a.signature, '')
                         ORDER BY a.delta_ns, a.source)
        FROM shred_arrivals a
        JOIN jito j USING (hub, slot, fec_set_index)
        WHERE a.t > now() - interval '24 hours' AND a.delta_ns IS NOT NULL
        GROUP BY 1, 2, 3
    """)
    for hub, slot, fec, jsig, srcs, deltas, sigs in cur:
        fec_sets[hub] += 1
        rank = 0
        for s, d, sig in zip(srcs, deltas, sigs):
            st = stats[(hub, s)]
            if sig == jsig:
                st.add(d, rank)
                rank += 1          # rank among VALID arrivals only
            elif sig == "":
                st.nosig += 1
            else:
                st.mism += 1
    conn.close()

    with open(f"{OUT_DIR}/sig_valid_per_source_24h.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["hub", "source", "valid_samples", "win_count",
                    "win_rate_pct", "avg_rank", "avg_delta_ms", "p50_ms",
                    "p90_ms", "p99_ms", "beats_jito_pct",
                    "valid_coverage_pct", "bad_sig_count", "no_sig_count",
                    "bad_sig_pct_of_arrivals"])
        for (hub, s), st in sorted(stats.items(),
                                   key=lambda kv: (kv[0][0], -kv[1].wins)):
            total_arr = st.n + st.mism + st.nosig
            if total_arr == 0:
                continue
            w.writerow([
                hub, s, st.n, st.wins,
                round(100 * st.wins / fec_sets[hub], 2) if fec_sets[hub] else None,
                round(st.sum_rank / st.n, 2) if st.n else None,
                round(st.sum_d / st.n / 1e6, 4) if st.n else None,
                st.pct(0.5), st.pct(0.9), st.pct(0.99),
                round(100 * st.neg / st.n, 2) if st.n else None,
                round(100 * st.n / fec_sets[hub], 2) if fec_sets[hub] else None,
                st.mism, st.nosig,
                round(100 * st.mism / total_arr, 2),
            ])

    with open(f"{OUT_DIR}/sig_mismatch_24h.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["hub", "source", "valid", "bad_signature", "no_signature"])
        for (hub, s), st in sorted(stats.items()):
            if st.mism or st.nosig:
                w.writerow([hub, s, st.n, st.mism, st.nosig])

    print("DONE", dict(fec_sets))


if __name__ == "__main__":
    main()
