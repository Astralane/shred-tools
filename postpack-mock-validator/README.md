# postpack-mock-validator

Stands in for a validator running the Astralane patch, so `postpack-relay` can be
exercised without a leader pointed at it. It runs the same auth handshake and pushes the
same streams as `core/src/pre_confirms/pre_confirms_stage.rs` (Jito fork) and
`core/src/tpu_relay/tpu_relay_stage.rs` (Rakurai fork).

```sh
cargo run -- --url http://127.0.0.1:12350 --rate 4
```

It always opens both ingresses on one authenticated connection —
`AstralaneRelayer/StartRelayStream` (what both patches speak) and
`BlockEngineRelayer/StartExpiringPacketStream` — and pushes every batch down both, with
the same packets in each, so the relay sees every transaction arrive twice. astralane
carries them as bundles and block-engine as packet batches, the message each side is
built around.

The identity is derived from `--seed`, so the pubkey to allowlist in
`validator_clients.relay_auth_keys` is stable across runs; it is logged on startup.
`--keypair-path` uses a real Solana keypair file instead. Transactions are derived from
`--seed` alone, identity included, so two instances with one seed emit byte-identical
transactions, which is what makes the relay's dedup observable.

| flag | default | |
|---|---|---|
| `--rate` | 4 | batches per second |
| `--batch-size` | 8 | transactions per batch |
| `--repeat` | off | send every batch twice |
