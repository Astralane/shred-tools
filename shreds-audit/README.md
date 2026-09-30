# shred-audit

Find out which of your Solana shred providers is fastest — and prove that every
shred they send you is genuinely signed by the slot leader.

You run `shred-audit` on your own machine and point one or more providers at it
(each on its own UDP port, or identified by source IP). It listens, times every
packet the instant it arrives, checks each shred's signature, and writes a `.zip`
report you can open in any Parquet tool to compare providers side by side.

Everything is measured from **one machine's clock**, so comparing two providers
is an exact subtraction — no baseline provider, no clock skew to correct for.

<details>
<summary><b>⚠️ Never built a Rust project? Start here — clean machine to a finished report, about ten minutes.</b></summary>

Nothing below assumes you have used Rust before. Copy each block in order. The
one prerequisite is a Linux machine your providers send shreds to (or can start
sending to).

**1. Install Rust**

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
```

The second line puts `cargo` on the `PATH` of this shell; shells you open later
pick it up on their own.

**2. Get the code and build it**

```sh
git clone https://github.com/Astralane/shred-tools.git
cd shred-tools/shreds-audit
cargo build --release
```

The first build takes 5–15 minutes — it compiles the Solana crates from source.
It is finished when you see a line starting with `Finished`. Warnings scrolling
past are normal; only a line starting with `error:` means it failed.

**3. Write the config**

```sh
cp config.example.yaml config.yaml
```

Open `config.yaml` in any editor. Two things need to match your setup:

- `listen_ports` — the UDP ports you receive on, one per provider.
- `providers` — one entry per provider: a name you choose, and the port that
  provider sends to.

Everything else in the example is optional and set to its default, except
`grpc_sources`: its entries point at placeholder URLs, so delete them unless you
have those feeds. For two providers on ports 20001 and 20002 this is the whole
file:

```yaml
rpc_url: "https://api.mainnet-beta.solana.com"
listen_ports: [20001, 20002]
providers:
  - name: alpha
    port: 20001
  - name: beta
    port: 20002
```

Then give each provider this machine's public IP and the port that belongs to
them, and allow those UDP ports in your firewall or security group.

**4. Let the kernel keep the packets**

```sh
sudo sysctl -w net.core.rmem_max=67108864
```

Once per machine. Skip it and the kernel quietly drops packets under load — the
report flags this, but a flagged run is one you have to repeat. Details in
[One-time host tuning](#one-time-host-tuning-please-do-this).

**5. Run it for two minutes**

```sh
./target/release/shred-audit --config config.yaml --duration-secs 120
```

A dashboard comes up and the tool stops on its own after 120 seconds. Two
minutes is enough data to compare providers; a longer `--duration-secs` narrows
the numbers further.

Glance at the dashboard in the first few seconds — every provider should be
showing packets. One sitting at zero isn't reaching you: check its port and the
firewall rather than waiting out the run.

**6. Send the file**

The run leaves one archive in `out/`:

```sh
ls out/*.zip
```

Send that `shred-audit-<timestamp>-<hostname>.zip` to the Astralane team — it is
the whole result. Inside are the timing tables and a `manifest.json` describing
the run: hostname, provider names and IPs, and the `rpc_url` you configured. No
auth tokens, and your `config.yaml` is not included.

</details>

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

A recent [Rust toolchain](https://rustup.rs/) and nothing else — the build brings
its own `protoc`. `cargo build --release` (or `make release`) puts the binary at
`target/release/shred-audit`. The first build compiles a large Solana dependency
and is slow; later ones are quick.

## Run

```sh
./target/release/shred-audit --config config.yaml
```

A live dashboard comparing your providers. A report archive is written on exit —
Ctrl-C, or `--duration-secs` running out — and on every rotation.

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
| `--dump-txns` | also record every transaction arrival per source — **very large**, off by default |
| `--live` | keep refreshing `out/live.zip` for an external live viewer to poll |

## Configure

Copy the example and edit it:

```sh
cp config.example.yaml config.yaml
```

The example file lists every setting with its default value. The core of
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
See the `grpc_sources` section in `config.example.yaml`.

Each source can use either the standard post-execution transaction subscription
or the separate pre-execution `SubscribeDeshred` API:

```yaml
grpc_sources:
  - name: regular-geyser
    url: "https://grpc.example.com:443"
    mode: transactions       # default when omitted
    commitment: processed   # processed | confirmed | finalized

  - name: early-deshred
    url: "https://deshred.example.com:443"
    mode: deshred
```

`SubscribeDeshred` reports transactions reconstructed from shreds before
execution, so it has no commitment or transaction-status metadata. Do not set
`commitment` on a `deshred` source. Both modes are matched to locally
reconstructed transactions by `transaction.signatures[0]`.

The two modes stay distinguishable everywhere they are reported. In the manifest
and the dashboard each source carries a `kind` — `shreds` for the local shred
reconstruction, `grpc` for a post-execution subscription, `grpc-deshred` for a
pre-execution one — and the ping rows use the same vocabulary, so a deshred
endpoint is never reported as a plain gRPC one. Keep `kind` in view when reading
winrates: a pre-execution feed is timed before execution and a post-execution
subscription after it, so the two are not racing on equal terms.

#### Is any of it real? — the onchain audit

Winning a race proves a source was *fast*, not that it was *right*. A feed that
drops half a block, repeats itself, or invents transactions outright wins exactly
the same races as one that relays the leader's block faithfully — and a
pre-execution `deshred` feed carries no proof of anything at all.

So whenever the comparison is running, shred-audit also audits it against the
chain. Every `onchain_sample_secs` (default 5) it picks one recent slot, asks an
RPC node for that block's signatures with `getBlock`, and diffs them against what
every source delivered for that slot:

| | |
|---|---|
| `onchain_missed` | in the block, this source never delivered it |
| `onchain_corrupted` | this source delivered it, the block does not contain it |
| `onchain_duplicated` | delivered more than once for the same slot |
| `onchain_bad_pct` | all three, over the transactions in the sampled blocks |

`onchain_bad_pct` is the **`bad sigs`** column in the dashboard, and every count
lands in the manifest. It is the only number here that says whether a source's
transactions exist.

Two things to know before you read it:

- **It samples.** One slot every 5 s against ~2.5 produced a second — roughly one
  slot in twelve. The rates converge over a capture of any length; the raw counts
  are over sampled slots only, so compare sources on the percentage, never on
  `onchain_bad`.
- **Each sample is a `getBlock` call.** A public endpoint will rate-limit it.
  Point `onchain_rpc_url` at your own node (or raise `onchain_sample_secs`), and
  check `onchain_rpc_errors` in the manifest before trusting a run.

A source is only scored on a slot it delivered *something* for; slots it was
absent for are counted separately (`onchain_slots_absent`) and never scored, so a
feed that was merely disconnected is never reported as one that corrupted a
block. A `finalized` subscription lags past the sampling window and will show up
this way — use `processed` if you want it audited.

Some `onchain_corrupted` is expected and honest: a pre-execution deshred feed
reports transactions that may never land, and a `processed` subscription can
deliver from a fork that lost. Read it next to `onchain_missed` before calling it
fabrication. Set `onchain_verify: false` to turn the whole thing off.

Sources that rotate filters (next section) deliver a filtered subset of every
block by design, so they are left out of this audit and judged by the filter
audit instead.

#### Do the filters work? — filter rotation

Every gRPC source rotates through **filter bundles** by default: one bundle per
window of 30–60 s (random), each on a fresh subscription, cycling through every
bundle in shuffled order. A bundle is several named filters in one request —
exactly what a client sends — so the server also has to tag each update with
the names of every filter it matched. The built-in bundles cover every filter
field (`vote`, `account_include`, `account_exclude`, `account_required`, and
`failed` on post-execution feeds), include through **lookup-table** addresses
(USDC / wSOL mints), overlapping filters, filters combined with AND, an
unfiltered `all`, and a `never` filter on a key nobody uses, which must stay
silent.

Each window is checked two ways:

| check | coverage | what it catches |
|---|---|---|
| **tags** — each filter re-evaluated on the delivered transaction vs the update's `filters` list | every delivery | `tag_false_positive` (tagged, filter rejects it), `tag_missing` (filter accepts it, no tag), `vote_flag_mismatch` (server `is_vote` vs agave's simple-vote rule) |
| **chain** — every slot with `slot % sample_every_slots == 0` (default 10) that the window fully covered, vs the confirmed block from a full `getBlock` | sampled slots | `missed` (block txs the filter accepts, not delivered with its tag), `extra_in_block` (delivered with the tag, filter rejects it), `extra_not_in_block` (not in the confirmed block: forks, stale, fabricated) |

A slot counts only if the subscription was live for all of it (a couple of slots
after subscribing, and before the last slot the window delivered), so switching
filters never reads as a miss. One `getBlock` per sampled slot is shared by all
sources — about one call every 4 s.

`missed`, `extra_in_block` and the tag errors should be zero; the violation
table keeps up to 25 example signatures per window, filter and kind, each with
the clause that matched or failed (including whether a key came from a lookup
table). Latency is also recorded per window: the race against every other
source, the signed difference to the fastest **shred** provider for the same
transaction, and receive time minus the server's `created_at`.

Point the audit at a provider with a key rather than a public node — switching
provider is one field:

```yaml
rpc:
  provider: shyft          # generic | shyft | helius | triton
  token: env:SHYFT_API_KEY # key read from the environment; never logged or stored
```

Set `rotate: false` on a source to keep one unfiltered subscription for the whole
run (a stable latency baseline next to the rotating ones), or
`filter_rotation.enabled: false` to turn rotation off everywhere.

#### Running it forever: Postgres + Grafana

```sh
docker compose up -d                                  # postgres + grafana on 127.0.0.1
SHYFT_API_KEY=... cargo run --release -- --config config.yaml --export postgres --no-tui
```

Grafana (http://127.0.0.1:3000) is provisioned with two dashboards — `shred-audit`
(providers, race) and **`shred-audit — filter audit`** (correctness per filter,
violations, windows, getBlock health, latency per window and bundle) — and with
the alert rules in `grafana/provisioning/alerting/filter-audit.yml`:

| alert | severity | fires when |
|---|---|---|
| Filter false positive | page | any `extra_in_block` in 15 m |
| Filter tag errors | page | any tag false positive / missing tag in 15 m |
| No ground truth | page | no successful `getBlock` in 10 m |
| Rotation stalled | page | no window from a source in 5 m (or none at all) |
| Missed rate | warn | > 0.5 % of a filter's matches missed over 30 m |
| Not in confirmed block | warn | > 2 % of a source's deliveries over 30 m |
| Windows failing | warn | > 3 windows ended in an error in 15 m |
| Vote flag / tagging protocol | warn | server `is_vote` or `filters` lists disagree |
| Slower than shreds | warn | p50 vs fastest shred provider > 50 ms (placeholder) |

No contact point is provisioned — add yours in Grafana (Alerting → Contact
points). The tables are documented in `src/filters/schema.sql`; all rows are
per-window or per-slot deltas, so `SUM` them over a range.

## The report

Each run writes `shred-audit-<timestamp>-<hostname>.zip` containing:

- **`manifest.json`** — details about the run, plus a **`notes`** section listing
  any data-quality caveats. **Always read the notes** — they tell you if a
  capture was incomplete before you draw conclusions from it.
- **`fec_sets.parquet`** — one row per (provider, slot, FEC set) with timing,
  delivery counts, and validity. This is the table you compare providers on.
- **`shreds.parquet`** — one row per shred, only present if you passed
  `--dump-shreds`.
- **`transactions.parquet`** — one row per (transaction, source), only present if
  you passed `--dump-txns`. See below.

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

### `transactions.parquet` — every arrival, not just the summary

The manifest's `txn_compare` block says *who won, on average*. This table is the
raw material behind it: one row per **(transaction, source)** — when that source
first had that transaction on this machine, and whatever the source itself said
about it.

Pass `--dump-txns` to write it. It needs the transaction comparison configured
(`grpc_sources`), and it is as large as `--dump-shreds`:
every source repeats every transaction the cluster produces, votes included.

| column | meaning |
|---|---|
| `signature` | base58 `signatures[0]` — the join key across sources |
| `slot` | the slot this source attributed it to |
| `source` | source name from your config |
| `source_mode` | `shreds`, `grpc`, or `grpc-deshred` — the same vocabulary as the manifest |
| `first_rx_unix_ns` | when it first arrived **here**; this is the number the race is decided on |
| `server_created_at_ns` | when the sending server says it produced the message |
| `duplicate_count` | times this source sent it *again* after the first (0 = clean) |
| `is_vote` | a consensus vote rather than user traffic |
| `message_size` | size of the transaction in that source's own encoding |
| `connection_id` | which connection of that source delivered it, counting from 0 |

A null in the last four columns means **this kind of feed does not carry that
value** — never zero, and never "we lost it":

| | `server_created_at_ns` | `is_vote` | `message_size` | `connection_id` |
|---|---|---|---|---|
| `shreds` | — | yes | — | — |
| `grpc` | yes | yes | yes | yes |
| `grpc-deshred` | yes | yes | yes | yes |

The shred path has no server to timestamp anything, no connection to number, and
its transactions were rebuilt locally rather than received as messages.

Four things to know before you read it:

- **`server_created_at_ns` is another machine's clock.** Everything else in this
  tool is one host's `CLOCK_REALTIME`, which is why provider deltas are exact
  subtractions. Subtract this column from `first_rx_unix_ns` and you get network
  delay *plus that server's clock offset*, which nothing here measures. It is the
  source's claim about when it had the transaction, not a measured latency.
- **`is_vote` means the same thing on every row.** The gRPC feeds report it; the
  shred path derives it, using agave's simple-vote shape (one
  instruction into the vote program, fewer than three signatures). Votes are
  roughly half of mainnet traffic (~53 %), so `WHERE is_vote = false` is usually
  the first thing you write.
- **`message_size` is per-encoding.** Protobuf on the gRPC feeds. Compare it
  over time within one source, not between two.
- **A row appears when its race settles**, ~64 slots (≈25 s) behind the tip. The
  tail of a capture window therefore lands in the *next* archive, and a source
  re-delivering a transaction after that starts a fresh row at
  `duplicate_count = 0`.

```sql
-- median gap between what a feed claims and when it actually landed, votes out
SELECT source, source_mode,
       median(first_rx_unix_ns - server_created_at_ns) / 1000 AS us_behind_claim
FROM 'transactions.parquet'
WHERE is_vote = false AND server_created_at_ns IS NOT NULL
GROUP BY 1, 2;
```

---

Requires Linux (it uses kernel packet timestamping). Built on the agave Solana
crates.
