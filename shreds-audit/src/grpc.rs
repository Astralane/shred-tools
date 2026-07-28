//! Geyser/Yellowstone gRPC transaction subscription.
//!
//! Reports each transaction's first signature to the shared registry, stamped
//! with a CLOCK_REALTIME arrival time taken the instant the decoded message is
//! pulled off the stream. Same absolute-nanosecond clock domain as the kernel
//! shred timestamps, so a shred-vs-gRPC delta is an exact subtraction (see `sigreg`).

use std::sync::{Arc, Mutex};

use anyhow::Result;
use bytes::Bytes;
use futures::{channel::mpsc, sink::SinkExt, stream::StreamExt};
use tokio_util::sync::CancellationToken;
use tonic::{
    metadata::AsciiMetadataValue,
    service::interceptor::InterceptedService,
    transport::{Channel, ClientTlsConfig, Endpoint},
    Request, Status,
};

use crate::config::{GrpcMode, GrpcSourceCfg};
use crate::out::now_unix_ns;
use crate::proto::geyser::{
    geyser_client::GeyserClient, subscribe_update::UpdateOneof as TransactionUpdate,
    subscribe_update_deshred::UpdateOneof as DeshredUpdate, CommitmentLevel,
    SubscribeDeshredRequest, SubscribeRequest, SubscribeRequestFilterDeshredTransactions,
    SubscribeRequestFilterTransactions, SubscribeRequestPing, SubscribeUpdateDeshredTransaction,
    SubscribeUpdateTransaction,
};
use crate::sigreg::SigRegistry;

/// Injects the `x-token` auth header (if configured) on every request.
#[derive(Clone)]
struct XToken(Option<AsciiMetadataValue>);

impl tonic::service::Interceptor for XToken {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        if let Some(t) = self.0.clone() {
            req.metadata_mut().insert("x-token", t);
        }
        Ok(req)
    }
}

async fn connect(cfg: &GrpcSourceCfg) -> Result<GeyserClient<InterceptedService<Channel, XToken>>> {
    let mut endpoint = Endpoint::from_shared(Bytes::from(cfg.url.clone()))?;
    // TLS only for https endpoints; a plaintext http:// endpoint (e.g. a
    // node on a private network) must connect without it.
    if cfg.url.starts_with("https://") {
        endpoint = endpoint.tls_config(ClientTlsConfig::new().with_native_roots())?;
    }
    let channel = endpoint.connect().await?;
    let token = match &cfg.x_token {
        Some(t) => Some(AsciiMetadataValue::try_from(t.as_str())?),
        None => None,
    };
    Ok(GeyserClient::with_interceptor(channel, XToken(token))
        .max_decoding_message_size(64 * 1024 * 1024))
}

fn commitment_of(s: &str) -> CommitmentLevel {
    match s.to_lowercase().as_str() {
        "confirmed" => CommitmentLevel::Confirmed,
        "finalized" => CommitmentLevel::Finalized,
        _ => CommitmentLevel::Processed,
    }
}

/// Run one gRPC source until cancelled or the stream ends. Reconnects are the
/// caller's concern; this returns on any stream error so a supervisor can retry.
pub async fn run_source(
    sid: usize,
    cfg: GrpcSourceCfg,
    reg: Arc<Mutex<SigRegistry>>,
    cancel: CancellationToken,
) -> Result<()> {
    match cfg.mode {
        GrpcMode::Transactions => run_transactions_source(sid, cfg, reg, cancel).await,
        GrpcMode::Deshred => run_deshred_source(sid, cfg, reg, cancel).await,
    }
}

async fn run_transactions_source(
    sid: usize,
    cfg: GrpcSourceCfg,
    reg: Arc<Mutex<SigRegistry>>,
    cancel: CancellationToken,
) -> Result<()> {
    let mut client = connect(&cfg).await?;
    let (mut sub_tx, mut stream) = {
        let (tx, rx) = mpsc::unbounded::<SubscribeRequest>();
        let resp = client.subscribe(rx).await?;
        (tx, resp.into_inner())
    };

    sub_tx.send(transactions_request(&cfg)).await?;

    loop {
        if cancel.is_cancelled() {
            break;
        }
        tokio::select! {
            _ = cancel.cancelled() => break,
            msg = stream.next() => {
                let msg = match msg {
                    Some(Ok(m)) => m,
                    Some(Err(_)) | None => break,
                };
                match msg.update_oneof {
                    Some(TransactionUpdate::Transaction(tx_msg)) => {
                        let ns = now_unix_ns();
                        if let Some((sig, slot)) = transaction_hit(&tx_msg) {
                            reg.lock().unwrap().record_first(sid, sig, ns, slot);
                        }
                    }
                    Some(TransactionUpdate::Ping(_)) => {
                        let _ = sub_tx.send(transactions_ping_request(1)).await;
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

async fn run_deshred_source(
    sid: usize,
    cfg: GrpcSourceCfg,
    reg: Arc<Mutex<SigRegistry>>,
    cancel: CancellationToken,
) -> Result<()> {
    let mut client = connect(&cfg).await?;
    let (mut sub_tx, mut stream) = {
        let (tx, rx) = mpsc::unbounded::<SubscribeDeshredRequest>();
        let resp = client.subscribe_deshred(rx).await?;
        (tx, resp.into_inner())
    };

    sub_tx.send(deshred_request()).await?;

    loop {
        if cancel.is_cancelled() {
            break;
        }
        tokio::select! {
            _ = cancel.cancelled() => break,
            msg = stream.next() => {
                let msg = match msg {
                    Some(Ok(m)) => m,
                    Some(Err(_)) | None => break,
                };
                match msg.update_oneof {
                    Some(DeshredUpdate::DeshredTransaction(tx_msg)) => {
                        let ns = now_unix_ns();
                        if let Some((sig, slot)) = deshred_hit(&tx_msg) {
                            reg.lock().unwrap().record_first(sid, sig, ns, slot);
                        }
                    }
                    Some(DeshredUpdate::Ping(_)) => {
                        let _ = sub_tx.send(deshred_ping_request(1)).await;
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

fn transactions_request(cfg: &GrpcSourceCfg) -> SubscribeRequest {
    let mut transactions = std::collections::HashMap::new();
    transactions.insert(
        "all".to_string(),
        SubscribeRequestFilterTransactions {
            account_include: vec![],
            account_exclude: vec![],
            account_required: vec![],
            // Include votes: the shred path reconstructs every transaction, votes
            // included, so excluding them here would make every vote a gRPC miss.
            vote: None,
            failed: None,
            signature: None,
        },
    );
    SubscribeRequest {
        transactions,
        commitment: Some(commitment_of(cfg.effective_commitment()) as i32),
        ..Default::default()
    }
}

fn deshred_request() -> SubscribeDeshredRequest {
    let mut deshred_transactions = std::collections::HashMap::new();
    deshred_transactions.insert(
        "all".to_string(),
        SubscribeRequestFilterDeshredTransactions {
            // Include votes so the feed matches the local shred reconstruction.
            vote: None,
            account_include: vec![],
            account_exclude: vec![],
            account_required: vec![],
        },
    );
    SubscribeDeshredRequest {
        deshred_transactions,
        ..Default::default()
    }
}

fn transactions_ping_request(id: i32) -> SubscribeRequest {
    SubscribeRequest {
        ping: Some(SubscribeRequestPing { id }),
        ..Default::default()
    }
}

fn deshred_ping_request(id: i32) -> SubscribeDeshredRequest {
    SubscribeDeshredRequest {
        ping: Some(SubscribeRequestPing { id }),
        ..Default::default()
    }
}

/// Pull `transaction.signatures[0]` (64 bytes), deliberately ignoring the
/// redundant info-level signature so both subscription modes use the same key.
fn transaction_hit(tx_msg: &SubscribeUpdateTransaction) -> Option<([u8; 64], u64)> {
    let tx = tx_msg.transaction.as_ref()?.transaction.as_ref()?;
    first_signature(tx.signatures.first()?).map(|sig| (sig, tx_msg.slot))
}

fn deshred_hit(tx_msg: &SubscribeUpdateDeshredTransaction) -> Option<([u8; 64], u64)> {
    let tx = tx_msg.transaction.as_ref()?.transaction.as_ref()?;
    first_signature(tx.signatures.first()?).map(|sig| (sig, tx_msg.slot))
}

fn first_signature(sig: &[u8]) -> Option<[u8; 64]> {
    if sig.len() != 64 {
        return None;
    }
    let mut arr = [0u8; 64];
    arr.copy_from_slice(sig);
    Some(arr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{
        geyser::{SubscribeUpdateDeshredTransactionInfo, SubscribeUpdateTransactionInfo},
        solana::storage::confirmed_block::Transaction,
    };

    fn source(mode: GrpcMode, commitment: Option<&str>) -> GrpcSourceCfg {
        GrpcSourceCfg {
            name: "test".into(),
            url: "http://localhost:10000".into(),
            x_token: None,
            mode,
            commitment: commitment.map(str::to_string),
        }
    }

    fn transaction(sig: Vec<u8>) -> Transaction {
        Transaction {
            signatures: vec![sig],
            ..Default::default()
        }
    }

    #[test]
    fn builds_all_transactions_request_with_default_commitment() {
        let request = transactions_request(&source(GrpcMode::Transactions, None));
        assert_eq!(request.commitment, Some(CommitmentLevel::Processed as i32));
        let filter = request.transactions.get("all").unwrap();
        assert_eq!(filter.vote, None);
        assert_eq!(filter.failed, None);
    }

    #[test]
    fn builds_all_deshred_request_without_commitment() {
        let request = deshred_request();
        let filter = request.deshred_transactions.get("all").unwrap();
        assert_eq!(filter.vote, None);
        assert!(request.slots.is_empty());
    }

    #[test]
    fn builds_mode_specific_ping_requests() {
        assert_eq!(transactions_ping_request(7).ping.unwrap().id, 7);
        assert_eq!(deshred_ping_request(9).ping.unwrap().id, 9);
    }

    #[test]
    fn transaction_update_uses_nested_signature_and_wrapper_slot() {
        let nested = vec![7; 64];
        let msg = SubscribeUpdateTransaction {
            transaction: Some(SubscribeUpdateTransactionInfo {
                signature: vec![9; 64],
                transaction: Some(transaction(nested)),
                ..Default::default()
            }),
            slot: 42,
        };
        assert_eq!(transaction_hit(&msg), Some(([7; 64], 42)));
    }

    #[test]
    fn deshred_update_uses_nested_signature_and_wrapper_slot() {
        let nested = vec![5; 64];
        let msg = SubscribeUpdateDeshredTransaction {
            transaction: Some(SubscribeUpdateDeshredTransactionInfo {
                signature: vec![8; 64],
                transaction: Some(transaction(nested)),
                ..Default::default()
            }),
            slot: 84,
        };
        assert_eq!(deshred_hit(&msg), Some(([5; 64], 84)));
    }

    #[test]
    fn malformed_or_absent_nested_signatures_are_ignored() {
        let malformed = SubscribeUpdateDeshredTransaction {
            transaction: Some(SubscribeUpdateDeshredTransactionInfo {
                transaction: Some(transaction(vec![1; 63])),
                ..Default::default()
            }),
            slot: 1,
        };
        assert_eq!(deshred_hit(&malformed), None);
        assert_eq!(
            transaction_hit(&SubscribeUpdateTransaction::default()),
            None
        );
    }
}
