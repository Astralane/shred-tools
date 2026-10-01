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
pairs are over 100 ms late. Recovery is announced once the win rate reaches 90%, so a
feed hovering at the threshold does not flap. While degraded, repeats are throttled to
one every 15 minutes.

Those numbers come from measured baselines rather than taste:

| host | pairs | win rate | p50 | p90 | late >100 ms |
|---|---|---|---|---|---|
| ny | 357 | 98.3% | +186 ms | +618 ms | 6 (1.7%) |
| ny | 691 | 94.1% | +139 ms | +451 ms | 29 (4.2%) |
| longhorn-lt | 862 | 94.3% | +98 ms | +472 ms | 36 (4.2%) |
| longhorn-lt | 156 | 98.7% | +53 ms | +175 ms | 0 |

The tail rule is a **rate**, not a count: a baseline window already carries 6–36 late
pairs, so any small absolute threshold fires continuously.

## Two ways this could lie, and what stops them

- **An empty window reading as healthy.** Postpack traffic is leader-slot shaped — a
  single key bursts for about a minute every ten — so short windows are often empty. The
  window is 15 minutes, and below `min_pairs` matched pairs the verdict is withheld
  instead of being drawn from a handful of samples. If the window stays empty for 30
  minutes, that itself raises an alert, because a dead pipeline and a healthy one both
  look like silence.
- **A failing query reading as healthy.** A ClickHouse error is surfaced as an error, not
  parsed as a window with no breaches.

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

This measures **our** vantage point — the indexer's subscription on `ny` /
`longhorn-lt`. A searcher subscribing from their own datacentre sees a different number;
during the report that prompted this service, our internal view was 94–99% while theirs
was 76%. This catches our pipeline degrading. It is not a proxy for what a customer
experiences, and it will not reproduce their figure.

`relay_arrivals` carries `endpoint` and `host` but no `validator_pubkey`, so the win rate
is an aggregate per endpoint, not a per-validator breakdown.
