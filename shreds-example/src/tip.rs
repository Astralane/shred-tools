//! Self-contained tip-transaction builder.
//!
//! Mirrors `common::utils::create_signed_tipped_transaction` from the
//! txn-router workspace so this example has no internal-crate dependencies.

use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::transaction::Transaction;

/// SPL Memo program.
const MEMO_PROGRAM: Pubkey = Pubkey::from_str_const("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");
const MICRO_LAMPORTS_PER_LAMPORTS: u64 = 1_000_000;

/// Build a signed transaction that sets a compute budget, writes a memo, and
/// (when `tip > 0`) transfers `tip` lamports to `tip_to`.
pub fn create_signed_tipped_transaction(
    signer: &Keypair,
    block_hash: Hash,
    data: &str,
    tip: u64,
    tip_to: &Pubkey,
    lamport_per_cu: u64,
) -> Transaction {
    let instructions = create_tipped_instruction(data, tip, tip_to, lamport_per_cu, signer.pubkey());
    Transaction::new_signed_with_payer(&instructions, Some(&signer.pubkey()), &[signer], block_hash)
}

fn create_tipped_instruction(
    data: &str,
    tip: u64,
    tip_to: &Pubkey,
    lamport_per_cu: u64,
    signer: Pubkey,
) -> Vec<Instruction> {
    let mut instructions = vec![
        ComputeBudgetInstruction::set_compute_unit_limit(30_000),
        ComputeBudgetInstruction::set_compute_unit_price(lamport_per_cu * MICRO_LAMPORTS_PER_LAMPORTS),
        Instruction {
            accounts: vec![AccountMeta::new(signer, true)],
            program_id: MEMO_PROGRAM,
            data: data.as_bytes().to_vec(),
        },
    ];

    if tip > 0 {
        instructions.push(solana_system_interface::instruction::transfer(
            &signer, tip_to, tip,
        ));
    }
    instructions
}
