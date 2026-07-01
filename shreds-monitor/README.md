# shreds-monitor

Receives raw Solana shreds over UDP from multiple providers, reassembles FEC
sets, verifies each set's signature against the slot's leader, and writes
per-provider timing and quality metrics to ClickHouse. Those tables feed the
**Providers monitoring** Grafana dashboard (see [`deployment/`](deployment/) for a
one-command stack).

Built on mainline agave crates (`solana-ledger`, `solana-sdk`) — no forks.

## Run

```sh
cargo run --release -- --config-path config.example.yaml
```

## Config

A single YAML file:

```yaml
clickhouse:
  url: "http://localhost:18123"  # database / user / password default to "default"
  baseline_provider_id: 1        # 0 -> anchor on the earliest-seen provider
  # shred_version: 50093         # optional; skip to accept any version
shred_sources:
  - name: agave
    udp_port: 20000
    provider_id: 1               # unique, non-zero; used as fec_stats.provider_id
    baseline: true               # delays are measured against this provider
  - name: jito
    udp_port: 20001
    provider_id: 2
```

See `config.example.yaml` for the full set of optional ClickHouse / leader-schedule
tuning fields.

## What it writes

| Table           | Contents                                                              |
|-----------------|----------------------------------------------------------------------|
| `fec_stats`     | Per (provider, slot, fec_set): signed first/decode/last delay vs baseline, invalid/missed/duplicated shreds, validity |
| `slot_timings`  | First-seen wall-clock time per slot (maps slots to time)             |
| `providers`     | `provider_id -> name`, upserted from config on startup               |

`leader_schedule` and `validators` are populated separately (the deployment stack
runs a small RPC-based populator); the monitor reads `leader_schedule` to resolve
each slot's leader for signature verification.
