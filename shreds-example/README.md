# shreds-example

A small, standalone example client that reacts to Solana shreds forwarded by
shreds-hub.

It subscribes to the raw shred stream (plain UDP, one serialized shred per
datagram), deshreds + decodes each slot into transactions, and watches for any
transaction that touches a configurable "trigger" wallet. The moment one is
seen it:

1. builds a simple SOL transfer that tips an Astralane tip account,
2. submits it to iris (`sendTransaction`) as fast as possible,
3. registers it via the `/shred-pay` endpoint and prints when accepted,
4. waits for it to land and logs the slot distance between the trigger
   transaction and our landed transaction.

Runs until Ctrl-C.

## Layout

| File            | Responsibility                                                      |
| --------------- | ------------------------------------------------------------------- |
| `src/main.rs`   | CLI wiring, background tasks, and the main decode/dispatch loop.     |
| `src/config.rs` | Command-line arguments.                                             |
| `src/receiver.rs` | Blocking UDP receive loop (one datagram == one raw shred).        |
| `src/decode.rs` | Per-slot shred accumulation, deshredding, and trigger detection.    |
| `src/tip.rs`    | Self-contained tip-transaction builder.                            |
| `src/trigger.rs`| The race: submit to iris, register via shred-pay, report landing.   |

## Run

```sh
cargo run --release -- \
  --watch-wallet <BASE58_PUBKEY> \
  --keypair-path /path/to/funded-keypair.json \
  --api-key <YOUR_API_KEY> \
  --shred-port 20000 \
  --iris-url http://127.0.0.1:8888/iris2 \
  --shred-pay-url http://127.0.0.1:8888/shred-pay \
  --rpc-url https://api.mainnet-beta.solana.com
```

See `--help` for all options and defaults.

> **Note:** for this client to receive anything, shreds-hub must be forwarding
> shreds to this host on `--shred-port` (configured in its listener set).

## Shred decoding dependencies

`solana-ledger` and `solana-entry` are pinned to the same agave fork that
shreds-hub uses (`nuel77/agave`, branch `xdp-read-dev`) so the wire shred
format decodes correctly. Keep them in sync with shreds-hub.
