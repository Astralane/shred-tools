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

### DoubleZero Edge multicast

A provider can be a multicast group instead of a unicast sender, which is how you
compare **DoubleZero Edge against internet turbine on a validator**. Add a
`multicast` entry for the group port and make each transport a provider:

```yaml
listen_ports: [7733, 20001]
providers:
  - name: doublezero
    port: 7733
  - name: turbine
    port: 20001            # a mirror of the validator's TVU port; see below
multicast:
  - port: 7733             # DoubleZero uses 7733 for every group
    cluster: mainnet       # mainnet | testnet -> that cluster's leader + root groups
```

shred-audit receives the groups exactly the way the validator does: bind
`0.0.0.0:7733` with `SO_REUSEADDR`, then join each group once the DoubleZero
daemon has installed its host route.

**You can run this next to a live validator.** For multicast the kernel fans each
datagram out to *every* socket bound to the port, so shred-audit takes its own
copy and the validator loses nothing — unlike a unicast port, where a second
socket would compete for the stream. Nothing is mirrored and no hop is added, so
the DoubleZero timestamp is the kernel's at driver handoff on the DoubleZero
interface, directly comparable with a turbine timestamp taken the same way. The
delta stays an exact subtraction.

| field | meaning |
|---|---|
| `port` | UDP port of the groups; must be in `listen_ports`. Defaults to 7733 |
| `cluster` | `mainnet` / `testnet` — shorthand for that cluster's leader + turbine-root groups |
| `groups` | explicit group addresses; combines with `cluster` |
| `interface` | IPv4 of the interface to join on. `0.0.0.0` (default) lets the DoubleZero host route choose it |
| `require_route` | join only while a `/32` host route to the group exists (default `true`) |

`require_route` is on by default because a join without that route succeeds
against whatever the default route names and then receives nothing at all — which
would read as DoubleZero delivering nothing rather than as a tunnel that is down.
Membership is re-checked every 60 s, so a tunnel that flaps mid-capture is
followed rather than lost. Set it to `false` only if your deployment installs no
such route.

**Turbine is the harder leg.** Agave owns the TVU port, so it cannot be bound
twice — and `SO_REUSEPORT` would make the kernel *split* the stream with the
validator rather than duplicate it. Mirror that port to a spare one and point the
`turbine` provider there:

```sh
tc qdisc add dev eth0 clsact
tc filter add dev eth0 ingress protocol ip flower ip_proto udp dst_port <TVU_PORT> \
   action mirred egress mirror dev <spare-veth>
```

`mirred ... mirror` clones, so the validator's own path is untouched. A mirror
costs the turbine leg a small extra hop that the multicast leg does not pay, so it
biases turbine *slower*; measure it once and subtract, or mirror both legs the
same way if you want the bias to cancel exactly. Mirror drops are invisible to
this tool — check `tc -s filter show` too, or they read as turbine packet loss.

**Read `multicast` in `manifest.json` before comparing anything.** A group that
was not joined for the whole window did not lose races, it was in none of them;
`joined_ns` says for how long it was in, `joins`/`leaves` how often the tunnel
flapped, and the `notes` say so in words when the window is not trustworthy.

> **Keep your `config.yaml` private.** It can contain gRPC auth tokens. Only
> `config.example.yaml` is meant to be shared; `config.yaml` is gitignored.

### Optional: also compare against a gRPC feed

If you add a `grpc_sources` block to your config, the tool additionally compares
your shred stream against one or more Geyser/Yellowstone gRPC feeds by
transaction arrival time, and reports which source delivered each transaction
first. If you don't add that block, nothing changes — this is entirely opt-in.
See the commented `grpc_sources` section in `config.example.yaml`.

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

## The report

Each run writes `shred-audit-<timestamp>-<hostname>.zip` containing:

- **`manifest.json`** — details about the run, plus a **`notes`** section listing
  any data-quality caveats. **Always read the notes** — they tell you if a
  capture was incomplete before you draw conclusions from it. With a `multicast`
  block there is also a **`multicast`** section: per group, whether it was joined,
  `joined_ns` (how much of the window it was in), and every join/leave transition.
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
