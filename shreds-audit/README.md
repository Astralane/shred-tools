# shred-audit

Find out which of your Solana shred providers is fastest — and prove that every
shred they send you is genuinely signed by the slot leader.

You run `shred-audit` on your own machine and point one or more providers at it
(each on its own UDP port, or identified by source IP). It listens, times every
packet the instant it arrives, checks each shred's signature, and writes a `.zip`
report you can open in any Parquet tool to compare providers side by side.

Everything is measured from **one machine's clock**, so comparing two providers
is an exact subtraction — no baseline provider, no clock skew to correct for.

## What you get, per provider

- **winrate** — how often it delivered a usable set *first*.
- **decode delay** — how far behind the fastest provider it was (0 = it was the
  fastest).
- **bad-signature rate** — how often it sent the leader's real data but with a
  broken proof (unusable, but not tampered with).
- **bad-data rate** — how often it sent data the leader never signed. This is a
  trust signal, not a speed one — a provider should never do this.

The winner on any set is the one that made the data *usable* first (`decode_ns`),
not merely the first to send a byte — a provider can win the first-packet race
with a slow trickle and still lose the race that matters.

## Install

You need a recent [Rust toolchain](https://rustup.rs/). Nothing else — the build
brings its own `protoc`.

```sh
cargo build --release        # produces target/release/shred-audit
```

The first build is slow (it compiles a large Solana dependency); later builds are
quick. You can also just run `make release`.

## Run

```sh
./target/release/shred-audit --config config.yaml
```

It opens a live dashboard comparing your providers, and writes a report archive
on exit (Ctrl-C), on a timer, and whenever it rotates.

### One-time host tuning (please do this)

Linux ships with a tiny network receive buffer. Under a burst the kernel throws
packets away *before this tool can see them* — and that would look like your
provider dropped them. Raise the limit once:

```sh
sudo sysctl -w net.core.rmem_max=67108864
```

shred-audit counts any packets the kernel dropped on it (`udp_kernel_dropped` in
the report) and never blames a provider for them. **If that number isn't zero,
your machine was overloaded — reduce load and re-capture before trusting the
results.**

### Flags

| flag | what it does |
|---|---|
| `--config <path>` | your YAML config (default `config.yaml`) |
| `--duration-secs <n>` | stop after `n` seconds (`0` = run until Ctrl-C) |
| `--no-tui` | turn off the live dashboard, print a status line instead |
| `--dump-shreds` | also record every individual shred — **very large**, off by default |
| `--live` | keep refreshing `out/live.zip` for an external live viewer to poll |

## Configure

Copy the example and edit it:

```sh
cp config.example.yaml config.yaml
```

The example file is fully commented — walk through it top to bottom. The core of
it is: give each provider its own UDP port (or identify it by source IP), and
list every port under `listen_ports`.

```yaml
rpc_url: "https://api.mainnet-beta.solana.com"   # only used to look up who the leader is
listen_ports: [20001, 20002]                     # every UDP port to listen on

providers:
  - name: alpha
    port: 20001                # match anything arriving on this port
  - name: beta
    ips: ["203.0.113.7"]       # or match by the source IP it sends from

output_dir: "./out"            # where reports are written
rotate_secs: 600               # start a fresh report every N seconds (0 = one at exit)
```

Each provider must set `port`, `ips`, or both — the tool refuses to start on a
config that could silently drop traffic, so you find mistakes immediately rather
than in the numbers.

> **Keep your `config.yaml` private.** It can contain gRPC auth tokens. Only
> `config.example.yaml` is meant to be shared; `config.yaml` is gitignored.

### Optional: also compare against a gRPC feed

If you add a `grpc_sources` block to your config, the tool additionally compares
your shred stream against one or more Geyser/Yellowstone gRPC feeds by
transaction arrival time, and reports which source delivered each transaction
first. If you don't add that block, nothing changes — this is entirely opt-in.
See the commented `grpc_sources` section in `config.example.yaml`.

## The report

Each run writes `shred-audit-<timestamp>-<hostname>.zip` containing:

- **`manifest.json`** — details about the run, plus a **`notes`** section listing
  any data-quality caveats. **Always read the notes** — they tell you if a
  capture was incomplete before you draw conclusions from it.
- **`fec_sets.parquet`** — one row per (provider, slot, FEC set) with timing,
  delivery counts, and validity. This is the table you compare providers on.
- **`shreds.parquet`** — one row per shred, only present if you passed
  `--dump-shreds`.

Load the Parquet files into whatever you like — DuckDB, pandas, Polars, a
spreadsheet importer — and compare. The columns that matter most:

| column | meaning |
|---|---|
| `provider`, `slot`, `fec_set_index` | which provider, which set |
| `decode_ns` | when the set became usable (lower = faster; this is the race) |
| `first_ns`, `last_ns` | when this provider's first / last good shred arrived |
| `is_valid` | the set fully decoded and every shred checked out |
| `invalid_sig` | sent the leader's real data behind a broken proof |
| `invalid_data` | sent data the leader never signed (**a red flag**) |
| `missed` | shreds the set expected but this provider didn't deliver |

Timing only ever counts *good* shreds — a provider can't look fast by spraying
garbage early, because invalid or duplicate shreds never move its timestamps.

### What "invalid" really means

The tool is careful to separate honest mistakes from tampering, because these
reports are evidence you might hand back to a provider:

- **broken proof** (`invalid_sig`) — the block data is provably the leader's; only
  the signature proof is malformed. Unusable, but nothing was altered.
- **altered data** (`invalid_data`) — the data differs from the copy the leader
  actually signed. This is the serious one.
- **can't tell** (`invalid_unknown`) — no provider gave a leader-signed copy of
  that exact shred to compare against, so it's never lumped in with the above.

Genuine Solana network pings that ride the same socket are recognised and
excluded — they are never counted against a provider.

---

Requires Linux (it uses kernel packet timestamping). Built on the agave Solana
crates.
