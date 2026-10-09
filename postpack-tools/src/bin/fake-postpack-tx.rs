//! Build fake, locally-signed tipped transactions for /postpack-pay (or
//! /shred-pay) without touching an RPC. The blockhash is random: it never
//! lands on-chain, but public-server only runs sanitize + signature verify +
//! tip detection, so a fresh keypair + Hash::new_unique() is enough.
//!
//! Output is the JSON array accepted by the endpoint, ready to pipe to curl:
//!
//!   cargo run --release --bin fake-postpack-tx -- --count 2 \
//!     | curl -sS -X POST 'http://127.0.0.1:8891/postpack-pay?api-key=KEY' \
//!            -H 'content-type: application/json' --data-binary @-

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use clap::Parser;
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_hash::Hash;
use solana_instruction::{AccountMeta, Instruction};
use solana_keypair::Keypair;
use solana_pubkey::{Pubkey, pubkey};
use solana_signer::Signer;
use solana_transaction::Transaction;
use solana_transaction::versioned::VersionedTransaction;

const MEMO_PROGRAM: Pubkey = pubkey!("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");
const MICRO_LAMPORTS_PER_LAMPORTS: u64 = 1_000_000;

#[derive(Parser, Debug)]
#[command(about = "Emit fake signed tipped txs (JSON array) for /postpack-pay")]
struct Args {
    /// Tip wallet the transfer targets. Must match an entry in the server's
    /// `incoming_tip_addresses`.
    #[arg(long, default_value = "astgCFnATW3DGN7Pnfj51w3BdVy2TGeMoK5eV7bt4x1")]
    tip_address: String,

    /// Lamports to tip (must be > 0 for the server to accept).
    #[arg(long, default_value_t = 10_000)]
    tip_lamports: u64,

    /// Priority fee (lamports per CU; converted to micro-lamports internally).
    #[arg(long, default_value_t = 0)]
    lamports_per_cu: u64,

    /// Memo payload (differentiates txs so signatures don't collide).
    #[arg(long, default_value = "fake-postpack")]
    memo: String,

    /// Shred slot stamped on each entry. Repeat the flag to assign distinct
    /// slots across entries (cycled if fewer slots than --count). Each must
    /// be > 0. Default: 1.
    #[arg(long = "slot", num_args = 1..)]
    slots: Option<Vec<i64>>,

    /// Number of fake txs to emit. Defaults to the number of --slot values,
    /// or 1 if --slot is omitted.
    #[arg(long)]
    count: Option<usize>,
}

fn create_signed_tipped_transaction(
    signer: &Keypair,
    block_hash: Hash,
    data: &str,
    tip: u64,
    tip_to: &Pubkey,
    lamport_per_cu: u64,
) -> Transaction {
    let payer = signer.pubkey();
    let mut instructions = vec![
        ComputeBudgetInstruction::set_compute_unit_limit(30_000),
        ComputeBudgetInstruction::set_compute_unit_price(
            lamport_per_cu * MICRO_LAMPORTS_PER_LAMPORTS,
        ),
        Instruction {
            accounts: vec![AccountMeta::new(payer, true)],
            program_id: MEMO_PROGRAM,
            data: data.as_bytes().to_vec(),
        },
    ];
    if tip > 0 {
        instructions.push(solana_system_interface::instruction::transfer(
            &payer, tip_to, tip,
        ));
    }
    Transaction::new_signed_with_payer(&instructions, Some(&payer), &[signer], block_hash)
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let tip_to: Pubkey = args
        .tip_address
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid --tip-address: {}", args.tip_address))?;

    let slots = args.slots.unwrap_or_else(|| vec![1]);
    if let Some(&bad) = slots.iter().find(|s| **s <= 0) {
        anyhow::bail!("--slot values must be > 0 (got {bad})");
    }
    let count = args.count.unwrap_or(slots.len());

    let entries: Vec<serde_json::Value> = (0..count)
        .map(|i| {
            let kp = Keypair::new();
            let memo = format!("{}-{}", args.memo, i);
            let tx = create_signed_tipped_transaction(
                &kp,
                Hash::new_unique(),
                &memo,
                args.tip_lamports,
                &tip_to,
                args.lamports_per_cu,
            );
            let bytes =
                wincode::serialize(&VersionedTransaction::from(tx)).expect("wincode serialize");
            serde_json::json!({
                "transaction": BASE64_STANDARD.encode(bytes),
                "slot": slots[i % slots.len()],
            })
        })
        .collect();

    println!("{}", serde_json::to_string(&entries)?);
    Ok(())
}
