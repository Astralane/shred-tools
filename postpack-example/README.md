# postpack-example

A minimal Rust client for Astralane's **post-pack** relay. It streams transactions
that validators have packed into their blocks, as soon as the relay receives them
(`preconf.PreconfService/SubscribePreconfs`, defined in
[`proto/preconf.proto`](../proto/preconf.proto)).

This example:

1. connects to a relay and authenticates with your API key (`x-token` header),
2. sends one `SubscribePreconfsRequest` with account / program filters,
3. logs every transaction: slot, signature and size in bytes,
4. reconnects if the stream drops.

Runs until Ctrl-C.

## Run

```sh
# Transactions that call Jupiter v6, except ones that touch a wallet you ignore
cargo run --release -- \
  --url <POSTPACK_URL> \
  --x-token <YOUR_API_KEY> \
  --program JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4 \
  --exclude-account <WALLET_PUBKEY>
```

Sample output:

```
INFO postpack_example] subscribed to <POSTPACK_URL>
INFO postpack_example] slot=377412345 sig=5xK...9Qm bytes=612
```

`--url` and `--x-token` can also be set with `POSTPACK_URL` / `POSTPACK_X_TOKEN`.
`RUST_LOG=debug` raises log verbosity.

## Filters

Flags map 1:1 onto `SubscribePreconfsRequest`. They are repeatable and take base58
pubkeys.

| Flag                | Field                  | Meaning                                                                 |
| ------------------- | ---------------------- | ----------------------------------------------------------------------- |
| `--account`         | `accounts_of_interest` | Deliver txs that **write** to any of these accounts.                    |
| `--program`         | `programs_of_interest` | Deliver txs that reference any of these programs.                       |
| `--exclude-account` | `excluded_accounts`    | Drop a delivered tx if it references any of these accounts in any role. |

A transaction is delivered when it matches at least one account or program and
references none of the excluded accounts. Matching covers addresses loaded through
lookup tables the relay has cached.

To drop everything a wallet signs or pays for, pass it to `--exclude-account`.
Signers are always static keys, so this works whatever lookup tables the
transaction uses. It also drops transactions that only reference that wallet
without its signature.

Vote transactions are never delivered.

## Errors

| Status               | Cause                                                       |
| -------------------- | ----------------------------------------------------------- |
| `UNAUTHENTICATED`    | no `x-token` header                                         |
| `PERMISSION_DENIED`  | the API key has no active post-pack access                  |
| `RESOURCE_EXHAUSTED` | out of credits, or the key's connection limit is reached    |
| `INVALID_ARGUMENT`   | a filter entry is not a 32-byte pubkey                      |

Access can also end mid-stream, for example when credits run out. The stream then
closes with one of these statuses.

## Notes

- `Preconf.data` is a raw serialized `VersionedTransaction`. Its first signature
  is bytes `1..65`.
- Each delivered transaction is charged in credits.
- The relay only sends `Preconf` messages and only serves `PreconfService`.
  `ValidatorSlotStart` and `PreconfAuthService` stay in the proto for compatibility
  with other preconf providers.
- Using another language? Generate a client from `proto/preconf.proto` with your
  gRPC toolchain. It is a single server-streaming call, so the flow matches this
  example.
