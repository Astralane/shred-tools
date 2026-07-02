# shreds-monitor — self-sufficient stack

One `docker compose up` brings up ClickHouse, a leader-schedule populator,
Prometheus, Grafana (auto-loading the **Providers monitoring** and
**Shreds monitor** dashboards), and the monitor itself.

## Quick start

```sh
cd shreds-monitor/deployment
cp monitor.example.yaml monitor.yaml     # edit providers + ports
docker compose up --build
```

Open Grafana at <http://localhost:3000> → **Providers monitoring**.

> The first build compiles the monitor (and solana-ledger/rocksdb) from source —
> several minutes. Later runs reuse the cache.

## Services

| Service           | Purpose                                                            | Port (host)      |
|-------------------|-------------------------------------------------------------------|------------------|
| `clickhouse`      | Analytics store; schema auto-applied from `clickhouse/init`        | 18123 / 19000    |
| `leader-schedule` | Fills `leader_schedule` + `validators` from RPC (once, then daily) | —                |
| `prometheus`      | Scrapes instance metrics for the **Shreds monitor** dashboard      | 19090            |
| `grafana`         | Provisioned ClickHouse + Prometheus datasources and dashboards     | 3000             |
| `shreds-monitor`  | Ingests shreds (host network), writes `fec_stats` + `slot_timings` | UDP per provider |

## Configuration

The monitor is configured entirely by `monitor.yaml` (bind-mounted into the
container). Each source gets a UDP port to receive on, a unique non-zero
`provider_id` (also seeded into the ClickHouse `providers` table for dashboard
names), and an optional `baseline` marker (delays are measured against it):

```yaml
clickhouse:
  url: "http://localhost:18123/default"
  baseline_provider_id: 1
shred_sources:
  - name: agave
    udp_port: 20000
    provider_id: 1
    baseline: true
  - name: jito
    udp_port: 20001
    provider_id: 2
```

`SOLANA_RPC_URL` (for the leader-schedule populator) can be overridden in the
shell before `docker compose up`; it defaults to mainnet-beta.

## Notes

- The monitor uses `network_mode: host` so it can bind arbitrary provider UDP
  ports without remapping. Linux only.
- Until the first leader-schedule population completes, signatures can't be
  verified, so rows carry `is_valid = false` and leader columns show pubkeys.
