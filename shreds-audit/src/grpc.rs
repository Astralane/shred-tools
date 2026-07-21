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

use crate::config::GrpcSourceCfg;
use crate::out::now_unix_ns;
use crate::proto::geyser::{
    geyser_client::GeyserClient, subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterTransactions, SubscribeRequestPing,
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
    let mut client = connect(&cfg).await?;
    let (mut sub_tx, mut stream) = {
        let (tx, rx) = mpsc::unbounded::<SubscribeRequest>();
        let resp = client.subscribe(rx).await?;
        (tx, resp.into_inner())
    };

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
    sub_tx
        .send(SubscribeRequest {
            transactions,
            commitment: Some(commitment_of(&cfg.commitment) as i32),
            ..Default::default()
        })
        .await?;

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
                    Some(UpdateOneof::Transaction(tx_msg)) => {
                        let ns = now_unix_ns();
                        let slot = tx_msg.slot;
                        if let Some(sig) = first_signature(&tx_msg) {
                            reg.lock().unwrap().record_first(sid, sig, ns, slot);
                        }
                    }
                    Some(UpdateOneof::Ping(_)) => {
                        let _ = sub_tx
                            .send(SubscribeRequest {
                                ping: Some(SubscribeRequestPing { id: 1 }),
                                ..Default::default()
                            })
                            .await;
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

/// Pull `signatures[0]` (64 bytes) from a transaction update.
fn first_signature(
    tx_msg: &crate::proto::geyser::SubscribeUpdateTransaction,
) -> Option<[u8; 64]> {
    let tx = tx_msg.transaction.as_ref()?.transaction.as_ref()?;
    let sig = tx.signatures.first()?;
    if sig.len() != 64 {
        return None;
    }
    let mut arr = [0u8; 64];
    arr.copy_from_slice(sig);
    Some(arr)
}
