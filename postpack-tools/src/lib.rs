//! Shared pieces of the post-pack provider mocks: the protos, the deterministic
//! transaction feed and identity handling. Each binary only adds its provider's wire
//! protocol on top.

pub mod proto;

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use clap::Args;
use prost_types::Timestamp;
use solana_keypair::{Keypair, read_keypair_file};
use solana_pubkey::Pubkey;
use solana_sdk::hash::Hash;
use solana_sdk::transaction::Transaction;
use solana_signer::Signer;
use tokio::sync::broadcast;
use tracing::info;
use tracing_subscriber::EnvFilter;

/// Flags shaping the transaction feed, shared by every mock.
#[derive(Args, Clone, Debug)]
pub struct FeedArgs {
    /// Batches per second.
    #[arg(long, default_value_t = 4)]
    pub rate: u64,
    /// Transactions per batch.
    #[arg(long, default_value_t = 8)]
    pub batch_size: u64,
    /// Send every batch twice.
    #[arg(long)]
    pub repeat: bool,
    /// Derives the transactions (and the default identity), so two instances with one
    /// seed emit byte-identical transactions.
    #[arg(long, default_value_t = 0)]
    pub seed: u64,
}

/// One tick of the feed: `batch_size` signed transfers.
#[derive(Debug)]
pub struct Batch {
    pub index: u64,
    pub transactions: Vec<Bytes>,
}

/// A clock-driven stream of batches; every subscriber sees the same batches.
#[derive(Clone)]
pub struct Feed {
    sender: broadcast::Sender<Arc<Batch>>,
}

impl Feed {
    const CAPACITY: usize = 1024;

    /// Starts ticking at `args.rate`, whether or not anyone is subscribed.
    pub fn spawn(args: &FeedArgs) -> Self {
        let (sender, _) = broadcast::channel(Self::CAPACITY);
        let feed = Self { sender };
        let args = args.clone();
        let sender = feed.sender.clone();
        tokio::spawn(async move {
            let payer = Keypair::new_from_array(tag(b"payer", args.seed, 0));
            let rate = args.rate.max(1);
            let mut ticker = tokio::time::interval(Duration::from_millis(1_000 / rate));
            let mut index = 0u64;
            loop {
                ticker.tick().await;
                // With --repeat every batch is sent twice.
                let range = if args.repeat { index / 2 } else { index };
                let base = range * args.batch_size;
                let transactions = (0..args.batch_size)
                    .map(|i| transfer(&payer, args.seed, base + i))
                    .collect();
                // No subscribers is fine: the batch is dropped, as a provider would.
                let _ = sender.send(Arc::new(Batch {
                    index,
                    transactions,
                }));
                index += 1;
                if index.is_multiple_of(rate) {
                    info!(
                        batches = index,
                        subscribers = sender.receiver_count(),
                        "produced"
                    );
                }
            }
        });
        feed
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Batch>> {
        self.sender.subscribe()
    }
}

#[derive(Debug)]
pub struct KeypairError {
    path: String,
    message: String,
}

impl fmt::Display for KeypairError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path, self.message)
    }
}

impl std::error::Error for KeypairError {}

/// The keypair at `path`, or one derived from `seed` so its pubkey is stable across runs.
pub fn identity(path: Option<&str>, seed: u64) -> Result<Keypair, KeypairError> {
    match path {
        Some(path) => read_keypair_file(path).map_err(|error| KeypairError {
            path: path.to_owned(),
            message: error.to_string(),
        }),
        None => Ok(Keypair::new_from_array(tag(b"identity", seed, 0))),
    }
}

pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
}

pub fn now() -> Timestamp {
    Timestamp::from(SystemTime::now())
}

/// 32 bytes unique to `(domain, seed, index)`.
pub fn tag(domain: &[u8], seed: u64, index: u64) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&seed.to_le_bytes());
    bytes[8..16].copy_from_slice(&index.to_le_bytes());
    for (slot, byte) in bytes[16..].iter_mut().zip(domain.iter().cycle()) {
        *slot = *byte;
    }
    bytes
}

/// A copy of a wire transaction with every signature zeroed, as Gravity emits bundles.
pub fn redact_signatures(transaction: &[u8]) -> Bytes {
    let mut redacted = transaction.to_vec();
    let (count, offset) = short_u16(&redacted);
    let end = (offset + count * 64).min(redacted.len());
    redacted[offset..end].fill(0);
    Bytes::from(redacted)
}

/// Decodes the compact-u16 signature count: `(value, bytes consumed)`.
fn short_u16(bytes: &[u8]) -> (usize, usize) {
    let mut value = 0usize;
    for (i, byte) in bytes.iter().take(3).enumerate() {
        value |= usize::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            return (value, i + 1);
        }
    }
    (value, bytes.len().min(3))
}

fn transfer(payer: &Keypair, seed: u64, index: u64) -> Bytes {
    let recipient = Pubkey::new_from_array(tag(b"recipient", seed, index));
    let blockhash = Hash::new_from_array(tag(b"blockhash", seed, index));
    let instruction =
        solana_system_interface::instruction::transfer(&payer.pubkey(), &recipient, 1);
    let transaction = Transaction::new_signed_with_payer(
        &[instruction],
        Some(&payer.pubkey()),
        &[payer],
        blockhash,
    );
    Bytes::from(bincode::serialize(&transaction).expect("serialize transaction"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redaction_zeroes_only_the_signatures() {
        let payer = Keypair::new_from_array(tag(b"payer", 0, 0));
        let transaction = transfer(&payer, 0, 0);
        let redacted = redact_signatures(&transaction);
        assert_eq!(redacted.len(), transaction.len());
        assert_eq!(redacted[0], 1);
        assert!(redacted[1..65].iter().all(|byte| *byte == 0));
        assert_eq!(redacted[65..], transaction[65..]);
    }

    #[test]
    fn transactions_are_deterministic_per_seed() {
        let payer = Keypair::new_from_array(tag(b"payer", 7, 0));
        assert_eq!(transfer(&payer, 7, 3), transfer(&payer, 7, 3));
        assert_ne!(transfer(&payer, 7, 3), transfer(&payer, 7, 4));
    }
}
