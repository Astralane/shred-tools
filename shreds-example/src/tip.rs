use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::transaction::Transaction;

const MEMO_PROGRAM: Pubkey = Pubkey::from_str_const("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");
const MICRO_LAMPORTS_PER_LAMPORTS: u64 = 1_000_000;

pub fn create_signed_tipped_transaction(
    signer: &Keypair,
    block_hash: Hash,
    memo: &str,
    tip: u64,
    tip_to: &Pubkey,
    lamport_per_cu: u64,
) -> Transaction {
    let payer = signer.pubkey();
    let mut instructions = vec![
        ComputeBudgetInstruction::set_compute_unit_limit(30_000),
        ComputeBudgetInstruction::set_compute_unit_price(lamport_per_cu * MICRO_LAMPORTS_PER_LAMPORTS),
        Instruction {
            accounts: vec![AccountMeta::new(payer, true)],
            program_id: MEMO_PROGRAM,
            data: memo.as_bytes().to_vec(),
        },
    ];
    if tip > 0 {
        instructions.push(solana_system_interface::instruction::transfer(&payer, tip_to, tip));
    }
    Transaction::new_signed_with_payer(&instructions, Some(&payer), &[signer], block_hash)
}
