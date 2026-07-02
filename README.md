# shred-tools

Tooling for measuring and monitoring Solana shred-source performance.

## Contents

- **[`shreds-monitor/`](shreds-monitor/)** — a live service that ingests raw
  shreds from multiple providers over UDP, reassembles FEC sets, verifies each
  set against the slot's leader, and writes per-provider timing/quality metrics
  to ClickHouse. Ships a one-command Docker stack (ClickHouse + Grafana +
  leader-schedule populator) that renders the **Providers monitoring** dashboard.
  Built on mainline agave crates.

- **[`perf-report/`](perf-report/)** — offline analysis scripts that pull
  competition data from the metrics DB, join it with the live leader schedule,
  and build xlsx reports (general, per-provider, per-leader, signature-valid).

- **[`shreds-example/`](shreds-example/)** - A small, standalone example client that reacts to Solana shreds forwarded by shreds-hub.

See each subdirectory's README for usage.
