# shred-audit

Independently measure how fast each of your Solana shred providers delivers, and
prove every shred they send is genuinely signed by the slot leader.

`shred-audit` is a receiver you run on your own machine. Point one or more
providers at it (each on its own UDP port, or identified by source IP), and it:

1. **Timestamps every datagram at the kernel**, with nanosecond precision, the
   moment the NIC hands it up — before it sits in any queue (`SO_TIMESTAMPNS`).
2. **Verifies every shred's signature** against the slot's leader. Each shred's
   merkle root is recomputed from its own inclusion proof, and the leader's
   ed25519 signature over that root is checked. A tampered, misfiled, or
   wrong-leader shred is caught.
3. **Aggregates per FEC set** — for each (provider, slot, fec_set_index) it
   records when the first shred arrived, when the set became *decodable*, and
   when the last shred arrived.
4. **Writes a `.zip`** (Parquet + a JSON manifest) you can archive, share, or
   load into your own tooling to compare providers.

Because every timestamp is an absolute value from **one machine's clock**,
comparing two providers on the same FEC set is an exact subtraction — there is
no baseline provider and no clock skew to correct for.

## How the comparison works

For any FEC set that two providers both delivered validly, the one with the
smaller `decode_ns` gave you a usable set first. Aggregate that across a
leader's slots and you get, per provider:

- **winrate** — fraction of contested sets it decoded first,
- **decode delay** — microseconds behind the fastest provider (0 = it was
  fastest),
- **bad-signature rate** — fraction of sets where it sent a shred carrying the
  leader's genuine data behind a broken merkle proof (unusable, but not altered),
- **bad-data rate** — fraction of sets where it sent block data the leader never
  signed. This one is not a performance number; it is a trust number.

`decode_ns` (set becomes reconstructable) is the edge that matters to a
consumer, not `first_ns` (first byte arrives) — a provider can win the
first-packet race with a slow trickle yet lose the decode race to a tight burst.
Both are recorded so you can look at either.

## Build

Requires a recent Rust toolchain.

```sh
cargo build --release        # -> target/release/shred-audit
cargo test --release         # 21 tests: registry, aggregation, invalid split, real-signature path
```

The first build is slow (it compiles the Solana ledger crate); later builds are
fast.

## Run

```sh
./shred-audit --config config.yaml
```

Flags:

| flag | meaning |
|---|---|
| `--config <path>` | YAML config (default `config.yaml`) |
| `--dump-shreds` | also write `shreds.parquet` — one row per shred. Large (~1 GB / 10 min at 100 kpps); off by default |
| `--duration-secs <n>` | stop after `n` seconds (0 = run until Ctrl-C) |

It prints a status line every 10 s (received / unmatched / dropped / kernel-drop /
truncated, parsed / bad-sig / no-leader / malformed / ping / unsupported, ed25519
verifies, pending sets) and writes an archive on rotation, on Ctrl-C, and at exit.

### Host tuning (do this, or your own losses look like the provider's)

The tool asks for a 64 MiB socket receive buffer. Linux **silently clamps** that
to `net.core.rmem_max` — 208 KiB on a stock box — and `setsockopt` still returns
success. A burst then overruns the queue and the kernel discards datagrams before
the tool ever sees them, which would show up as shreds the provider never sent.

shred-audit reads the granted size back and warns at startup, and counts every
kernel-dropped datagram via `SO_RXQ_OVFL` (`udp_kernel_dropped`, plus a manifest
note). Those are **our** losses and are never charged to a provider — but they do
mean incomplete coverage. Fix the host:

```sh
sudo sysctl -w net.core.rmem_max=67108864
```

If `udp_kernel_dropped` is non-zero, raise it (and reduce load on the box) and
re-capture before drawing conclusions about who missed what.

## Configure

```yaml
rpc_url: "https://api.mainnet-beta.solana.com"   # only used to fetch the leader schedule
listen_ports: [20001, 20002, 20003]              # every UDP port to bind
bind_ip: "0.0.0.0"                               # optional, default 0.0.0.0

providers:
  # A provider is matched most-specific-first: (ip,port) -> port -> ip.
  - name: alpha
    port: 20001                # match anything arriving on this port
  - name: beta
    ips: ["203.0.113.7"]       # match by source IP across all listen_ports
  - name: gamma
    port: 20003
    ips: ["198.51.100.4"]      # match only this IP on this port (most specific)

output_dir: "./out"            # default ./out
rotate_secs: 600               # new archive every N seconds; 0 = single archive at exit
verify_threads: 0              # 0 = (physical cores - 1)
fec_max_wait_slots: 10         # finalize a set once the tip is this many slots past it
shred_version: null            # optional: drop shreds whose version != this
```

Every provider must declare `port`, `ips`, or both — one with neither can never
match and is rejected at startup. A provider pinned to a port not in
`listen_ports` is also rejected (this is the `src_port = 0` class of silent
black-hole, caught loudly instead).

## Output archive

`shred-audit-<UTC-timestamp>-<hostname>.zip` containing:

### `manifest.json`

Metadata for the run: tool version, hostname, start/end unix-ns, `clock_source`,
`timestamp_semantics`, the provider list, RPC URL, leader-schedule epoch, slot
range, row counts, raw counters, and **`notes`** — human-readable data-quality
caveats (dropped batches, datagrams with no kernel timestamp, unmatched sources,
schedule gaps). Always read the notes; they tell you when a capture is
incomplete.

### `fec_sets.parquet` — one row per (provider, slot, fec_set_index)

| column | type | meaning |
|---|---|---|
| `provider` | string | provider name |
| `slot` | u64 | Solana slot |
| `fec_set_index` | u32 | FEC set index within the slot |
| `leader` | string? | base58 leader pubkey (null if schedule gap) |
| `first_ns` | i64 | absolute unix ns of this provider's first **accepted** shred |
| `decode_ns` | i64? | absolute unix ns the set became decodable (null if never) |
| `last_ns` | i64 | absolute unix ns of this provider's last accepted shred |
| `n_data`, `n_code` | u32 | data / coding shreds delivered |
| `expected_total` | u32? | data+coding expected (from a coding shred header) |
| `missed` | u32 | expected minus delivered |
| `invalid` | u32 | shreds that failed the merkle/signature check (= the three below) |
| `invalid_sig` | u32 | failed, but the block data is provably the leader's — only the merkle proof is broken |
| `invalid_data` | u32 | failed, **and** the block data differs from the leader-signed copy: altered content |
| `invalid_unknown` | u32 | failed, and no leader-authenticated copy was seen, so it could not be classified |
| `duplicated` | u32 | byte-identical retransmits from this provider |
| `sig_unverifiable` | u32 | shreds with no known leader (couldn't judge) |
| `is_valid` | bool | no invalid/unverifiable shreds **and** the set decoded |
| `last_in_slot` | bool | this set closes the slot |

Timing columns reflect **accepted shreds only** — an invalid or duplicate shred
never moves `first_ns`/`last_ns`, so a provider can't look fast by spraying
garbage early.

### `shreds.parquet` (only with `--dump-shreds`) — one row per shred

`provider, slot, fec_set_index, shred_index, is_code, rx_unix_ns, sig_ok (bool?),
merkle_ok (bool), leader, data_hash`. `sig_ok = null` means the leader was unknown,
which is distinct from `false` (verification failed). `data_hash` is SHA-256 of the
shred's block data (headers + payload, excluding the merkle proof), which lets the
bad-signature / bad-data split be re-derived offline.

## Verdict semantics (important)

Three outcomes per shred, kept strictly separate:

- **valid** — merkle root reconstructs *and* the leader's signature verifies.
- **invalid** — merkle fails or the signature is wrong. Counted, timing ignored.
  Always split three ways, because "your proof is broken" and "you changed the
  block" are very different accusations:
  - **invalid_sig** — the shred's block data is *byte-identical* to a copy whose
    leader signature verified, so the data is genuinely the leader's; only the
    merkle proof / signature path is broken. The shred is still unusable (agave
    rejects it too), but nothing was altered.
  - **invalid_data** — the block data differs from the leader-signed copy. The
    provider did not merely mangle a proof, it shipped content the leader never
    signed. Treat as substitution until proven otherwise.
  - **invalid_unknown** — no provider delivered a leader-authenticated copy of that
    exact shred, so there is no ground truth to compare against. Never folded into
    either of the other two.

  The comparison is cryptographic, not reputational: a copy is trusted as ground
  truth only because *the leader's signature over it verifies*. A dishonest
  provider cannot poison it without forging the leader's key.
- **unverifiable** — well-formed but the slot's leader is unknown (schedule gap).
  Counted separately, timing ignored. **Never** reported as invalid.
- **unsupported** — the shred's variant is one this build cannot parse (a legacy
  shred, or one from a Solana release newer than this binary). Counted under
  `shreds_unsupported_variant`, dropped, and **never** reported as invalid. If
  this number is large, the tool is out of date — not your provider.

A fourth thing shares the wire but is not a shred at all: **Solana pings**.
Validators ping peers over the same socket that carries shreds (the liveness
handshake before repair is served), so a provider forwarding that socket relays
them to you. They are recognised by shape *and* by verifying the sender's
signature, counted under `non_shred_pings`, and excluded from every shred count.
They are **not** a provider defect and are never reported as malformed or as a
bad signature — which matters, because these archives are evidence you hand to a
provider. A datagram whose slot is nowhere near the loaded leader schedule is
likewise rejected before it can invent a phantom FEC set at a nonsense slot.

## Layout

```
src/
  main.rs      wiring, rotation, live status, clean shutdown
  config.rs    YAML config + validation
  registry.rs  provider resolution (ip,port -> port -> ip)
  rx.rs        recvmmsg + SO_TIMESTAMPNS receive threads
  leader.rs    leader schedule via JSON-RPC, cached per epoch
  verify.rs    per-shred merkle + ed25519 verification (batch + fallback)
  agg.rs       FEC-set aggregation -> output rows
  out.rs       Parquet + manifest + zip writer
```
