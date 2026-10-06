# postpack-mock-validator

Stands in for the post-pack providers `postpack-relay` ingests from, so the relay can be
exercised without a real leader or provider behind it. There is one binary per provider,
each speaking that provider's wire protocol; they share one library
(`src/lib.rs`) with the protos and the transaction feed.

| binary | provider | direction | protocol |
|---|---|---|---|
| `mock-rakurai` | Rakurai validator with the Astralane patch | mock → relay ingress | `AstralaneRelayer/StartRelayStream` + `BlockEngineRelayer/StartExpiringPacketStream` |
| `mock-bam` | Jito BAM block engine | relay → mock | `bam_preconf.PreconfStream/Subscribe` |
| `mock-gravity` | Gravity (gattaca solana-relay) | relay → mock | `auth.AuthService` + `searcher.SearcherService` |

Every binary takes the same feed flags. Transactions are derived from `--seed` alone, so
two instances with one seed emit byte-identical transactions, which is what makes the
relay's dedup observable (also across providers).

| flag | default | |
|---|---|---|
| `--rate` | 4 | batches per second |
| `--batch-size` | 8 | transactions per batch |
| `--repeat` | off | send every batch twice |
| `--seed` | 0 | derives transactions and the default identity |

## mock-rakurai

```sh
cargo run --bin mock-rakurai -- --url http://127.0.0.1:12350 --rate 4
```

Runs the same auth handshake and pushes the same streams as
`core/src/tpu_relay/tpu_relay_stage.rs` (Rakurai fork). It opens both ingresses on one
authenticated connection and pushes every batch down both, with the same packets in
each, so the relay sees every transaction arrive twice: astralane carries them as
bundles and block-engine as packet batches.

The identity is derived from `--seed`, so the pubkey to allowlist in
`validator_clients.relay_auth_keys` is stable across runs; it is logged on startup.
`--keypair-path` uses a real Solana keypair file instead. The relay only credits this
path when that identity is registered with a Rakurai client; otherwise it drops the
traffic as `unknown_provider`.

## mock-bam

```sh
cargo run --bin mock-bam -- --listen 127.0.0.1:12360
```

Point the relay at it with `"bam_preconf_ingress": {"endpoints": ["http://127.0.0.1:12360"], ...}`.
The subscription's `x-bam-pubkey` and signature over `signed_slot` are verified; `--allow
<PUBKEY>` (repeatable) restricts who may subscribe. Each feed batch is one bundle:
`sequence_id` is the batch index, `bundle_position` the transaction's place in it.
`--slot` stamps every update; the default 0 makes the relay use its own current slot and
resolve the leader from its schedule. A subscriber that falls behind is closed with
`RESOURCE_EXHAUSTED`, which the relay counts as a `lag` reconnect.

## mock-gravity

```sh
cargo run --bin mock-gravity -- --listen 127.0.0.1:12370
```

Point the relay at it with `"gravity_relay_client": {"endpoints": ["http://127.0.0.1:12370"]}`.
It runs the searcher challenge/token handshake (any correctly signed key is accepted) and
requires the issued bearer token on both streams. Every batch goes down
`SubscribeMempool` as one `PendingTxNotification` and down `SubscribeBundles` as one
`JitoBundle` with its signatures zeroed, as Gravity emits them. Both streams also carry
an empty message every second, which the relay counts as a heartbeat. `identity_address`
is `--identity`, or the same seed-derived identity `mock-rakurai` uses.
