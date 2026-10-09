# postpack-tools

Local tooling for exercising the post-pack pipeline (`postpack-relay`, the admin server's
post-pack endpoints and `/postpack-pay`) without real providers, leaders or RPCs.

| binary | purpose |
|---|---|
| `mock-rakurai` | Rakurai validator with the Astralane patch; pushes the feed into the relay's ingress |
| `mock-bam` | Jito BAM block engine; serves `bam_preconf.PreconfStream/Subscribe` to the relay |
| `mock-gravity` | Gravity (gattaca solana-relay); serves auth + searcher streams to the relay |
| `preconf-admin-mock` | in-memory stand-in for the preconf admin API (`/admin/postpack*`) and `/postpack-pay` |
| `fake-postpack-tx` | prints fake, locally signed tipped transactions as the JSON body for `/postpack-pay` |

## Mock validators

The three mocks stand in for the post-pack providers `postpack-relay` ingests from, so
the relay can be exercised without a real leader or provider behind it. There is one
binary per provider, each speaking that provider's wire protocol; they share one library
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

### mock-rakurai

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

### mock-bam

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

### mock-gravity

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

## preconf-admin-mock

```sh
cargo run --bin preconf-admin-mock -- 127.0.0.1:28090
```

An in-memory mock of the txn-router admin-server post-pack API and the public server's
`/postpack-pay`, with the same routes, bodies, validation and error messages. Swagger UI is
served at `/swagger-ui`. It is seeded with `demo-key-tier1` (paying, 48 hours of usage on
two relay instances), `demo-key-trial` (tier override), `demo-key-off` and `demo-key-new`
(an API key that is not a post-pack client yet).

| route | |
|---|---|
| `GET /health` | liveness |
| `GET`, `PUT /admin/postpack` | list clients, set tier / tier override / comment |
| `GET`, `DELETE /admin/postpack/{key}` | read a client, switch it off (balance and history are kept) |
| `POST /admin/postpack/{key}/credits` | `admin_change` or `manual_payment`, never below 0 |
| `PUT /admin/postpack/{key}/credits-enabled` | `true`, `false` or `null` (follow the settings) |
| `GET /admin/postpack/{key}/usage` | balance and the last 24 hours |
| `GET /admin/postpack/{key}/usage/hourly?hours=` | per hour, zero-filled |
| `GET /admin/postpack/{key}/usage/daily?days=` | per day and relay instance |
| `GET /admin/postpack/{key}/credits/history?before_id=&limit=` | balance changes other than usage |
| `GET`, `PUT /admin/postpack/credits/settings` | `credits_enabled`, prices per message type and unit |
| `GET /admin/postpack/credits/price?lamports=` | quote, 503 when the SOL price is older than 30 minutes |
| `POST /postpack-pay?api-key=` | register tip transactions |

The mock treats every accepted `/postpack-pay` transaction as landed and, once the key is a
post-pack client, credits its tip within 5 seconds at the current SOL price, whether or not
credits are enabled. Usage is only what was seeded, since no relay charges the mock. Pipe
`fake-postpack-tx` output into `http://127.0.0.1:28090/postpack-pay?api-key=demo-key-tier1`
to exercise it.

| setting | default | |
|---|---|---|
| first argument / `PRECONF_ADMIN_MOCK_ADDR` | `0.0.0.0:28090` | listen address |
| `PRECONF_ADMIN_MOCK_TIP_ACCOUNTS` | the four `ast…` tip wallets | comma-separated tip accounts |
| `PRECONF_ADMIN_MOCK_SOL_USD_PRICE_CENTS` | 12000 | SOL price at startup, fresh for 30 minutes |
| `PRECONF_ADMIN_MOCK_SOL_PRICE_URL` | CoinGecko simple price | refreshed every 5 minutes |

## fake-postpack-tx

```sh
cargo run --release --bin fake-postpack-tx -- --count 2 \
  | curl -sS -X POST 'http://127.0.0.1:8891/postpack-pay?api-key=KEY' \
         -H 'content-type: application/json' --data-binary @-
```

Prints a JSON array of `{"transaction": <base64>, "slot": <slot>}` entries. Each
transaction is signed by a fresh keypair over a random blockhash, so it never lands
on-chain but passes sanitize, signature verification and tip detection. It holds a
compute-unit limit, a compute-unit price, a memo and a system transfer to the tip wallet.

| flag | default | |
|---|---|---|
| `--tip-address` | `astgCFnATW3DGN7Pnfj51w3BdVy2TGeMoK5eV7bt4x1` | must be one of the server's tip addresses |
| `--tip-lamports` | 10000 | must be > 0 for the server to accept it |
| `--lamports-per-cu` | 0 | priority fee, converted to micro-lamports |
| `--memo` | `fake-postpack` | suffixed with `-<index>` per transaction |
| `--slot` | 1 | repeatable; cycled across entries, each > 0 |
| `--count` | number of `--slot` values | transactions to emit |
