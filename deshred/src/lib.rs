mod deshredder;
#[cfg(feature = "verify")]
mod verify;

pub use deshredder::{CompletedDataSet, Deshredder, RecoveryStats, ShredInsertionResult};
pub use solana_entry::entry::Entry;
pub use solana_message::v0::LoadedAddresses;
pub use solana_transaction::versioned::VersionedTransaction;
#[cfg(feature = "verify")]
pub use verify::{LeaderSchedule, ShredVerifier, Verdict};

pub fn first_signature(tx: &VersionedTransaction) -> Option<[u8; 64]> {
    tx.signatures.first()?.as_ref().try_into().ok()
}
