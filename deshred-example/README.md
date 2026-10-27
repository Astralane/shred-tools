# deshred-example

A minimal Rust client for Astralane's **Decoded Shreds** service
([docs](https://astralane.gitbook.io/docs/low-latency/shreds/decoded-shreds)).

The service does shred reconstruction for you. Instead of raw UDP shreds, it
streams already decoded, filtered transactions over gRPC (`Geyser.SubscribeDeshred`,
defined in [`proto/geyser.proto`](../proto/geyser.proto)). This example:

1. connects to an endpoint and authenticates with your shreds API key (`x-token` header),
2. sends one `SubscribeDeshredRequest` with account / vote filters,
3. logs every decoded transaction: slot, signature, account and instruction
   counts, priority fee, and the latency from the server's `created_at` stamp,
4. answers server pings to keep the stream alive, and reconnects if it drops.

Runs until Ctrl-C.

## Endpoints

Plaintext gRPC on port `12000`:

| Region               | URL                                 |
| -------------------- | ----------------------------------- |
| Fra (Terraswitch)    | `http://fr1.shreds.astralane.io:12000` |
| Fra (Cherry Servers) | `http://fr2.shreds.astralane.io:12000` |
| NY (Terraswitch)     | `http://ny1.shreds.astralane.io:12000` |

Access is included for active tier-2 shreds users.

## Run

```sh
# All non-vote transactions touching the Jupiter v6 program
cargo run --release -- \
  --url http://fr1.shreds.astralane.io:12000 \
  --x-token <YOUR_SHREDS_API_KEY> \
  --account-include JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4 \
  --vote false
```

Sample output:

```
INFO deshred_example] subscribed to http://fr1.shreds.astralane.io:12000
INFO deshred_example] slot=377412345 sig=5xK...9Qm vote=false accounts=24 ixs=5 priority_fee=120000 latency=0.84ms filters=["example"]
```

`--url` and `--x-token` can also be set with `DESHRED_URL` / `DESHRED_X_TOKEN`.
`RUST_LOG=debug` raises log verbosity.

## Filters

All flags map 1:1 onto `SubscribeRequestFilterDeshredTransactions`. Account
flags are repeatable and take base58 pubkeys.

| Flag                 | Meaning                                                            |
| -------------------- | ------------------------------------------------------------------ |
| `--account-include`  | Deliver txs that reference **any** of these accounts.              |
| `--account-exclude`  | Drop txs that reference any of these accounts.                     |
| `--account-required` | Deliver only txs that reference **all** of these accounts.         |
| `--vote`             | `true` = only vote txs, `false` = no vote txs; omit for both.      |

Account matching covers addresses loaded through lookup tables. They come back
in `loaded_writable_addresses` / `loaded_readonly_addresses`.

## Notes

- Transactions are delivered **before execution**, so there is no status / meta.
  You can't tell from the stream whether a transaction succeeded.
- `transaction.signatures[0]` is the transaction signature. `slot` is on the
  wrapping `SubscribeUpdateDeshredTransaction`.
- Using another language? Generate a client from `proto/geyser.proto` and
  `proto/solana-storage.proto` with your gRPC toolchain. The service is a single
  bidirectional stream, so the flow matches this example.
