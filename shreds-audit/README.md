# shred-audit

An independent tool that measures how fast each of your Solana shred providers
delivers — and cryptographically proves every shred they send is genuinely
signed by the slot leader.

You run it on your own machine, point your providers at it, and it produces a
`.zip` report comparing them.

## What it measures

For every FEC set that multiple providers deliver, `shred-audit` records — from a
single machine's clock, so comparisons are exact with no clock skew — who
delivered it first and whether the data was authentic. Per provider you get:

- **winrate** — how often it delivered a usable set first.
- **decode delay** — how far behind the fastest provider it was (0 = fastest).
- **bad-signature rate** — sets where it sent the leader's real data but with a
  broken proof (unusable, but not tampered with).
- **bad-data rate** — sets where it sent data the leader never signed. This is a
  trust signal, not a speed one.

Every timestamp is taken in the kernel with nanosecond precision the instant the
network card hands the packet up, and every shred's signature is verified against
the slot's real leader. A tampered or wrong-leader shred is caught.

## Install

Requires a recent Rust toolchain.

```sh
cargo build --release        # produces target/release/shred-audit
```

The first build is slow; later builds are fast.

## Run

```sh
./shred-audit --config config.yaml
```

It prints a status line every 10 seconds and writes a `.zip` report on exit
(Ctrl-C) and on each rotation.

**One-time host tuning (important):** Linux caps the socket buffer low by
default, and packets can be dropped before the tool sees them — which would look
like your provider's fault. Run this once:

```sh
sudo sysctl -w net.core.rmem_max=67108864
```

If the report shows any `udp_kernel_dropped`, the box was overloaded — reduce
load and re-capture before trusting the numbers.

## Flags

| flag | meaning |
|---|---|
| `--config <path>` | path to your YAML config (default `config.yaml`) |
| `--duration-secs <n>` | stop after `n` seconds (`0` = run until Ctrl-C) |
| `--dump-shreds` | also write one row per shred — very large, off by default |

## Configure

Give each provider its own UDP port (or identify it by source IP), and list
those ports under `listen_ports`. Minimal example:

```yaml
rpc_url: "https://api.mainnet-beta.solana.com"   # used only to fetch the leader schedule
listen_ports: [20001, 20002]                     # every UDP port to listen on

providers:
  - name: alpha
    port: 20001                # match anything arriving on this port
  - name: beta
    ips: ["203.0.113.7"]       # or match by source IP

output_dir: "./out"            # where reports are written (default ./out)
rotate_secs: 600               # start a new report every N seconds (0 = one report at exit)
```

A full annotated example is in `config.example.yaml`.

## The report

Each run writes `shred-audit-<timestamp>-<hostname>.zip` containing:

- **`manifest.json`** — run metadata plus a **`notes`** section listing any
  data-quality caveats. Always read the notes; they tell you if a capture was
  incomplete.
- **`fec_sets.parquet`** — one row per (provider, slot, FEC set) with timing,
  delivery counts, and validity — this is the data you compare providers on.
- **`shreds.parquet`** — one row per shred, only when `--dump-shreds` is set.

Load the Parquet files into whatever tooling you like to compare providers.
