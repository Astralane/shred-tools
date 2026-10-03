# postpack-health

Continuously answers one question: **is the postpack feed still arriving before the
shreds?** — and says so in Slack when it stops.

A searcher reported postpack arriving first only 76% of the time, with 48 transactions
more than 100 ms late. This service watches for that happening again.

## What it measures

For every transaction seen on both feeds, it compares two timestamps:

| | |
|---|---|
| `relay_arrivals.recv_ns` | when the postpack feed delivered the transaction |
| `fec_arrivals.last_shred_ns` | when the shred range carrying it became decodable |

The difference is the **lead**: positive means postpack won. Both timestamps are
stamped by the same `shred_indexer` host, so the subtraction involves one clock and no
skew correction.

> **Why not `relayer.validator_packet_events`?** It is the obvious candidate and it is
> wrong. Joined against shred timing it returns a uniform −5.5 s on both `ny` and
> `longhorn-lt` at a 0% win rate, because those rows are stamped on the relayer's
> machine rather than the indexer's. What looks like a five-second regression is clock
> skew between two hosts. `relay_arrivals` is the indexer's own subscription to the
> same `:30001` endpoints, which is why it can be subtracted safely.

## Alerting

A window breaches when **either** the win rate falls below 85% **or** more than 10% of
pairs are over 100 ms late. Recovery requires **both** the win rate at 90% or above
**and** the late rate at or below 8%, so a breach caused by tail lag cannot clear on
win-rate alone and flap on fractions of a percent. While degraded, repeats are throttled
to one every 15 minutes.

Those numbers come from measured baselines rather than taste:

| cluster | host | pairs | win rate | p50 | p90 | late >100 ms |
|---|---|---|---|---|---|---|
| terra | limburg | 1766 | 98.3% | +139 ms | +770 ms | 13 (0.7%) |
| terra | limburg | 1606 | 98.2% | +128 ms | +914 ms | 13 (0.8%) |
| new-longhorn | ny | 691 | 94.1% | +139 ms | +451 ms | 29 (4.2%) |
| new-longhorn | longhorn-lt | 862 | 94.3% | +98 ms | +472 ms | 36 (4.2%) |

The tail rule is a **rate**, not a count: a baseline window already carries 6–36 late
pairs, so any small absolute threshold fires continuously.

## Two ways this could lie, and what stops them

- **An empty window reading as healthy.** Postpack traffic is leader-slot shaped — a
  single key bursts for about a minute every ten — so short windows are often empty. The
  window is 15 minutes, and below `min_pairs` matched pairs the verdict is withheld
  instead of being drawn from a handful of samples. If the window stays empty for 30
  minutes, that itself raises an alert, because a dead pipeline and a healthy one both
  look like silence. That warning then repeats on the same cadence as a degradation
  alert rather than firing once and going quiet, and a notice is sent when pairs start
  matching again.
- **A failing query reading as healthy.** A ClickHouse error is surfaced as an error, not
  parsed as a window with no breaches.

## Which cluster

`shred_indexer.*` exists on two clusters and they are not equivalent:

| cluster | disk | hosts | state |
|---|---|---|---|
| terra (`100.95.71.49:18123`) | NVMe | `limburg` | live, streaming to the current minute |
| new-longhorn (`:18123`) | JBOD HDD | `longhorn-lt`, `ny` | archival batch copies; the `limburg` copy stopped 2026-08-23 |

Point this at **terra**. `relay_arrivals` is ordered by `(sig, recv_ns)`, so a time-range
filter cannot seek and has to scan the partitions it is given; on spinning disk that is
16–25 s per evaluation, and at a 60 s poll it never stops reading. On terra, with the
partition filter derived from the window bounds, the same query is ~1.5 s.

## Run

```sh
cp config.example.yaml config.yaml   # then edit it
cargo run --release -- --config-path config.yaml
```

It logs one line per evaluation at `info`:

```
postpack window: 862 pairs, win rate 94.3%, p50 97.9ms, p90 471.7ms, 36 late (4.2%)
```

## Scope

This measures **our** vantage point — the indexer's subscription on `limburg`. A searcher subscribing from their own datacentre sees a different number;
during the report that prompted this service, our internal view was 94–99% while theirs
was 76%. This catches our pipeline degrading. It is not a proxy for what a customer
experiences, and it will not reproduce their figure.

`relay_arrivals` carries `endpoint` and `host` but no `validator_pubkey`, so the win rate
is an aggregate per endpoint, not a per-validator breakdown.
