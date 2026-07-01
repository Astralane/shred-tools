//! Per-trigger handler: race a tip transfer to iris, register it via shred-pay,
//! and report where it lands relative to the trigger slot.

use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use log::{error, info, warn};
use reqwest::Client;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature};
use tokio::sync::RwLock;

use crate::config::Args;
use crate::tip::create_signed_tipped_transaction;

/// Handles shared across every per-trigger handler task.
pub struct Shared {
    pub args: Args,
    pub keypair: Arc<Keypair>,
    pub tip_to: Pubkey,
    pub http: Client,
    pub rpc: Arc<RpcClient>,
    pub blockhash: Arc<RwLock<Hash>>,
}

/// Race a tip transfer to iris, register it via shred-pay, and report landing.
pub async fn handle_trigger(shared: Arc<Shared>, trigger_slot: u64, trigger_sig: String) {
    let t0 = Instant::now();
    let args = &shared.args;

    let blockhash = *shared.blockhash.read().await;
    let tx = create_signed_tipped_transaction(
        &shared.keypair,
        blockhash,
        "shred-trigger-bench",
        args.tip_lamports,
        &shared.tip_to,
        args.lamports_per_cu,
    );
    let our_sig: Signature = tx.signatures[0];
    let encoded = match bincode::serialize(&tx) {
        Ok(bytes) => BASE64_STANDARD.encode(bytes),
        Err(e) => {
            error!("[{trigger_sig}] failed to serialize tx: {e}");
            return;
        }
    };

    info!("TRIGGER slot={trigger_slot} from {trigger_sig} -> sending tip tx {our_sig}");

    if submit_to_iris(&shared, &encoded, &our_sig).await.is_err() {
        return;
    }
    register_via_shred_pay(&shared, &encoded, trigger_slot, &our_sig).await;

    // Wait for our tx to land and report the slot distance from the trigger.
    match wait_for_landing(&shared.rpc, &our_sig, Duration::from_secs(60)).await {
        Some(landed_slot) => {
            let distance = landed_slot as i64 - trigger_slot as i64;
            info!(
                "[{our_sig}] LANDED slot={landed_slot} | trigger_slot={trigger_slot} | distance={distance} slots | {:?} end-to-end",
                t0.elapsed()
            );
        }
        None => warn!("[{our_sig}] not landed within timeout (trigger_slot={trigger_slot})"),
    }
}

/// Submit to iris (iris2 query-param style) ASAP.
async fn submit_to_iris(shared: &Shared, encoded: &str, our_sig: &Signature) -> Result<(), ()> {
    let args = &shared.args;
    let url = format!("{}?api-key={}&method=sendTransaction", args.iris_url, args.api_key);
    match shared
        .http
        .post(&url)
        .header("Content-Type", "text/plain")
        .body(encoded.to_owned())
        .send()
        .await
    {
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            info!("[{our_sig}] tx SENT to iris ({status}): {body}");
            Ok(())
        }
        Err(e) => {
            error!("[{our_sig}] iris send failed: {e}");
            Err(())
        }
    }
}

/// Register via shred-pay and log whether our signature was accepted.
async fn register_via_shred_pay(
    shared: &Shared,
    encoded: &str,
    trigger_slot: u64,
    our_sig: &Signature,
) {
    let args = &shared.args;
    let url = format!("{}?api-key={}", args.shred_pay_url, args.api_key);
    let body = serde_json::json!([{
        "transaction": encoded,
        "slot": trigger_slot,
    }]);
    info!("[{our_sig}] REGISTERING via shred-pay");
    match shared.http.post(&url).json(&body).send().await {
        Ok(resp) => {
            let status = resp.status();
            let json: serde_json::Value = resp.json().await.unwrap_or_default();
            let accepted = json
                .get("accepted")
                .and_then(|a| a.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str())
                        .any(|s| s == our_sig.to_string())
                })
                .unwrap_or(false);
            if accepted {
                info!("[{our_sig}] ACCEPTED by shred-pay ({status})");
            } else {
                warn!("[{our_sig}] NOT accepted by shred-pay ({status}): {json}");
            }
        }
        Err(e) => error!("[{our_sig}] shred-pay register failed: {e}"),
    }
}

/// Poll signature status until the tx lands (returns the landed slot) or times out.
async fn wait_for_landing(rpc: &RpcClient, sig: &Signature, timeout: Duration) -> Option<u64> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(resp) = rpc.get_signature_statuses(&[*sig]).await {
            if let Some(Some(status)) = resp.value.into_iter().next() {
                if status.err.is_none() {
                    return Some(status.slot);
                }
                return None; // failed on chain
            }
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    None
}
