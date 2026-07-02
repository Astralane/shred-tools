//! Command-line arguments.

use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(about = "Shred-triggered tip bench: watch a wallet in shreds, race a tip tx to iris")]
pub struct Args {
    /// UDP port to bind for receiving raw shreds forwarded by shreds-hub.
    #[arg(long, default_value_t = 20000)]
    pub shred_port: u16,

    /// Wallet to watch for in shred transactions (base58 pubkey). Any tx that
    /// references this account triggers a tip submission.
    #[arg(long)]
    pub watch_wallet: String,

    /// Astralane tip account the transfer tips (must be an accepted tip wallet).
    #[arg(long, default_value = "ASTZHptaMgYVMX6DAocDr1vVXLran5PpfKfQtVTSWkfE")]
    pub tip_address: String,

    /// Lamports to transfer/tip to the tip account.
    #[arg(long, default_value_t = 100_000)]
    pub tip_lamports: u64,

    /// Priority fee (micro-lamports per compute unit).
    #[arg(long, default_value_t = 0)]
    pub lamports_per_cu: u64,

    /// iris2 endpoint (query-param sendTransaction, text/plain base64 body).
    #[arg(long, default_value = "https://fr.gateway.astralane.io/iris2")]
    pub iris_url: String,

    /// shred-pay endpoint.
    #[arg(long, default_value = "https://edge.astralane.io/shred-pay")]
    pub shred_pay_url: String,

    /// API key (query param `api-key`) used for both iris and shred-pay.
    #[arg(long)]
    pub api_key: String,

    /// RPC url used to fetch recent blockhashes and confirm landed txs.
    #[arg(long, default_value = "https://api.mainnet-beta.solana.com")]
    pub rpc_url: String,

    /// Fee-payer keypair path (must be funded).
    #[arg(long)]
    pub keypair_path: String,

    /// Drop per-slot shred state after a slot has been idle this many seconds.
    #[arg(long, default_value_t = 20)]
    pub slot_ttl_secs: u64,
}
