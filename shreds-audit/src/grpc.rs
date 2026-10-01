use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use futures::{channel::mpsc, sink::SinkExt, stream::StreamExt};
use prost::Message as _;
use tokio_util::sync::CancellationToken;
use tonic::{
    metadata::AsciiMetadataValue,
    service::interceptor::InterceptedService,
    transport::{Channel, ClientTlsConfig, Endpoint},
    Request, Status, Streaming,
};

use crate::config::{GrpcMode, GrpcSourceCfg};
use crate::filters::{
    audit::{FilterAudit, WindowKey},
    bundles::Bundle,
    spec::{Key, TxView},
};
use crate::out::now_unix_ns;
use crate::sigreg::{SigRegistry, TxnMeta};
use shreds_proto::yellowstone::geyser::{
    geyser_client::GeyserClient, subscribe_update::UpdateOneof as TransactionUpdate,
    subscribe_update_deshred::UpdateOneof as DeshredUpdate, CommitmentLevel,
    SubscribeDeshredRequest, SubscribeRequest, SubscribeRequestFilterDeshredTransactions,
    SubscribeRequestFilterTransactions, SubscribeRequestPing, SubscribeUpdateDeshredTransaction,
    SubscribeUpdateTransaction,
};
use shreds_proto::yellowstone::solana::storage::confirmed_block::Transaction;

#[derive(Clone)]
struct XToken(Option<AsciiMetadataValue>);

impl tonic::service::Interceptor for XToken {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        if let Some(t) = &self.0 {
            req.metadata_mut().insert("x-token", t.clone());
        }
        Ok(req)
    }
}

type Client = GeyserClient<InterceptedService<Channel, XToken>>;

async fn connect(cfg: &GrpcSourceCfg) -> Result<Client> {
    let mut endpoint = Endpoint::from_shared(Bytes::from(cfg.url.clone()))?
        .connect_timeout(std::time::Duration::from_secs(5));
    if cfg.url.starts_with("https://") {
        endpoint = endpoint.tls_config(ClientTlsConfig::new().with_native_roots())?;
    }
    let channel = endpoint.connect().await?;
    let token = cfg
        .x_token
        .as_deref()
        .map(AsciiMetadataValue::try_from)
        .transpose()?;
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

pub struct Subscription {
    pub bundle: Arc<Bundle>,
    pub window: Option<(Arc<FilterAudit>, WindowKey)>,
    pub deadline: Option<tokio::time::Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum End {
    Rotated,
    Cancelled,
    StreamEnded,
}

impl End {
    pub fn label(self) -> &'static str {
        match self {
            Self::Rotated => "rotated",
            Self::Cancelled => "shutdown",
            Self::StreamEnded => "stream ended",
        }
    }
}

async fn until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending().await,
    }
}

pub async fn run_source(
    sid: usize,
    cfg: GrpcSourceCfg,
    sub: Subscription,
    reg: Arc<Mutex<SigRegistry>>,
    cancel: CancellationToken,
    connection_id: u32,
) -> Result<End> {
    let mut client = tokio::select! {
        _ = cancel.cancelled() => return Ok(End::Cancelled),
        _ = until(sub.deadline) => return Ok(End::Rotated),
        res = connect(&cfg) => res.context("connect")?,
    };
    match cfg.mode {
        GrpcMode::Transactions => {
            let (mut sub_tx, stream) = tokio::select! {
                _ = cancel.cancelled() => return Ok(End::Cancelled),
                _ = until(sub.deadline) => return Ok(End::Rotated),
                res = async {
                    let (tx, rx) = mpsc::unbounded::<SubscribeRequest>();
                    let resp = client.subscribe(rx).await?;
                    Ok::<_, anyhow::Error>((tx, resp.into_inner()))
                } => res.context("subscribe")?,
            };
            sub_tx.send(transactions_request(&cfg, &sub.bundle)).await?;
            pump(stream, &sub, &cancel, |msg| {
                let created = created_at_ns(msg.created_at.as_ref());
                match msg.update_oneof {
                    Some(TransactionUpdate::Transaction(tx_msg)) => {
                        let ns = now_unix_ns();
                        let Some((sig, slot, meta)) = transaction_hit(&tx_msg, created, connection_id) else {
                            return;
                        };
                        reg.lock().unwrap().record_first(sid, sig, ns, slot, meta);
                        if let Some((audit, key)) = &sub.window {
                            if let Some(view) = transaction_view(&tx_msg) {
                                audit.on_delivery(*key, slot, &view, &msg.filters, meta.is_vote, ns, created);
                            }
                        }
                    }
                    Some(TransactionUpdate::Ping(_)) => {
                        let _ = sub_tx.unbounded_send(transactions_ping_request(1));
                    }
                    _ => {}
                }
            })
            .await
        }
        GrpcMode::Deshred => {
            let (mut sub_tx, stream) = tokio::select! {
                _ = cancel.cancelled() => return Ok(End::Cancelled),
                _ = until(sub.deadline) => return Ok(End::Rotated),
                res = async {
                    let (tx, rx) = mpsc::unbounded::<SubscribeDeshredRequest>();
                    let resp = client.subscribe_deshred(rx).await?;
                    Ok::<_, anyhow::Error>((tx, resp.into_inner()))
                } => res.context("subscribe")?,
            };
            sub_tx.send(deshred_request(&sub.bundle)).await?;
            pump(stream, &sub, &cancel, |msg| {
                let created = created_at_ns(msg.created_at.as_ref());
                match msg.update_oneof {
                    Some(DeshredUpdate::DeshredTransaction(tx_msg)) => {
                        let ns = now_unix_ns();
                        let Some((sig, slot, meta)) = deshred_hit(&tx_msg, created, connection_id) else {
                            return;
                        };
                        reg.lock().unwrap().record_first(sid, sig, ns, slot, meta);
                        if let Some((audit, key)) = &sub.window {
                            if let Some(view) = deshred_view(&tx_msg) {
                                audit.on_delivery(*key, slot, &view, &msg.filters, meta.is_vote, ns, created);
                            }
                        }
                    }
                    Some(DeshredUpdate::Ping(_)) => {
                        let _ = sub_tx.unbounded_send(deshred_ping_request(1));
                    }
                    _ => {}
                }
            })
            .await
        }
    }
}

/// Drives an open subscription until cancel, the rotation deadline, or the stream ends.
async fn pump<M>(
    mut stream: Streaming<M>,
    sub: &Subscription,
    cancel: &CancellationToken,
    mut handle: impl FnMut(M),
) -> Result<End> {
    if let Some((audit, key)) = &sub.window {
        audit.subscribed(*key);
    }
    let mut seen_first = false;
    loop {
        let msg = tokio::select! {
            _ = cancel.cancelled() => return Ok(End::Cancelled),
            _ = until(sub.deadline) => return Ok(End::Rotated),
            msg = stream.next() => msg,
        };
        let msg = match msg {
            Some(Ok(m)) => m,
            Some(Err(status)) => bail!("stream error: {}", status.message()),
            None => return Ok(End::StreamEnded),
        };
        if !seen_first {
            seen_first = true;
            if let Some((audit, key)) = &sub.window {
                audit.first_message(*key);
            }
        }
        handle(msg);
    }
}

fn transactions_request(cfg: &GrpcSourceCfg, bundle: &Bundle) -> SubscribeRequest {
    let transactions = bundle
        .filters
        .iter()
        .map(|f| {
            (
                f.name.clone(),
                SubscribeRequestFilterTransactions {
                    account_include: f.cfg.account_include.clone(),
                    account_exclude: f.cfg.account_exclude.clone(),
                    account_required: f.cfg.account_required.clone(),
                    vote: f.cfg.vote,
                    failed: f.cfg.failed,
                    signature: None,
                },
            )
        })
        .collect();
    SubscribeRequest {
        transactions,
        commitment: Some(commitment_of(cfg.effective_commitment()) as i32),
        ..Default::default()
    }
}

fn deshred_request(bundle: &Bundle) -> SubscribeDeshredRequest {
    let deshred_transactions = bundle
        .filters
        .iter()
        .map(|f| {
            (
                f.name.clone(),
                SubscribeRequestFilterDeshredTransactions {
                    vote: f.cfg.vote,
                    account_include: f.cfg.account_include.clone(),
                    account_exclude: f.cfg.account_exclude.clone(),
                    account_required: f.cfg.account_required.clone(),
                },
            )
        })
        .collect();
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

fn created_at_ns(ts: Option<&prost_types::Timestamp>) -> Option<i64> {
    ts.map(|t| t.seconds * 1_000_000_000 + t.nanos as i64)
}

/// Keys on the nested `transaction.signatures[0]`, not the wrapper's `signature` field.
fn hit(
    tx: &Transaction,
    is_vote: bool,
    slot: u64,
    created_at_ns: Option<i64>,
    connection_id: u32,
) -> Option<([u8; 64], u64, TxnMeta)> {
    let sig: [u8; 64] = tx.signatures.first()?.as_slice().try_into().ok()?;
    let meta = TxnMeta {
        server_created_at_ns: created_at_ns,
        is_vote: Some(is_vote),
        version: None,
        message_size: Some(tx.encoded_len() as u32),
        connection_id: Some(connection_id),
    };
    Some((sig, slot, meta))
}

fn transaction_hit(
    tx_msg: &SubscribeUpdateTransaction,
    created_at_ns: Option<i64>,
    connection_id: u32,
) -> Option<([u8; 64], u64, TxnMeta)> {
    let info = tx_msg.transaction.as_ref()?;
    let tx = info.transaction.as_ref()?;
    hit(tx, info.is_vote, tx_msg.slot, created_at_ns, connection_id)
}

fn deshred_hit(
    tx_msg: &SubscribeUpdateDeshredTransaction,
    created_at_ns: Option<i64>,
    connection_id: u32,
) -> Option<([u8; 64], u64, TxnMeta)> {
    let info = tx_msg.transaction.as_ref()?;
    let tx = info.transaction.as_ref()?;
    hit(tx, info.is_vote, tx_msg.slot, created_at_ns, connection_id)
}

fn proto_view(
    tx: &Transaction,
    loaded_writable: &[Vec<u8>],
    loaded_readonly: &[Vec<u8>],
    failed: Option<bool>,
) -> Option<TxView> {
    let signature: [u8; 64] = tx.signatures.first()?.as_slice().try_into().ok()?;
    let msg = tx.message.as_ref()?;
    let num_static_keys = msg.account_keys.len();
    let keys = msg
        .account_keys
        .iter()
        .chain(loaded_writable)
        .chain(loaded_readonly)
        .map(|k| Key::try_from(k.as_slice()).ok())
        .collect::<Option<Vec<_>>>()?;
    let first_program = msg
        .instructions
        .first()
        .and_then(|ix| keys[..num_static_keys].get(ix.program_id_index as usize).copied());
    Some(TxView {
        signature,
        num_signatures: tx.signatures.len(),
        legacy: !msg.versioned,
        keys,
        num_static_keys,
        num_instructions: msg.instructions.len(),
        first_program,
        failed,
    })
}

fn transaction_view(tx_msg: &SubscribeUpdateTransaction) -> Option<TxView> {
    let info = tx_msg.transaction.as_ref()?;
    let tx = info.transaction.as_ref()?;
    match &info.meta {
        Some(m) => proto_view(
            tx,
            &m.loaded_writable_addresses,
            &m.loaded_readonly_addresses,
            Some(m.err.is_some()),
        ),
        None => proto_view(tx, &[], &[], None),
    }
}

fn deshred_view(tx_msg: &SubscribeUpdateDeshredTransaction) -> Option<TxView> {
    let info = tx_msg.transaction.as_ref()?;
    let tx = info.transaction.as_ref()?;
    proto_view(
        tx,
        &info.loaded_writable_addresses,
        &info.loaded_readonly_addresses,
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filters::bundles;
    use shreds_proto::yellowstone::{
        geyser::{SubscribeUpdateDeshredTransactionInfo, SubscribeUpdateTransactionInfo},
        solana::storage::confirmed_block::{
            CompiledInstruction, Message, TransactionError, TransactionStatusMeta,
        },
    };

    fn source(mode: GrpcMode, commitment: Option<&str>) -> GrpcSourceCfg {
        GrpcSourceCfg {
            name: "test".into(),
            url: "http://localhost:10000".into(),
            x_token: None,
            mode,
            commitment: commitment.map(str::to_string),
            rotate: true,
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
        let request = transactions_request(&source(GrpcMode::Transactions, None), &Bundle::all());
        assert_eq!(request.commitment, Some(CommitmentLevel::Processed as i32));
        let filter = request.transactions.get("all").unwrap();
        assert_eq!(filter.vote, None);
        assert_eq!(filter.failed, None);
    }

    #[test]
    fn builds_all_deshred_request_without_commitment() {
        let request = deshred_request(&Bundle::all());
        let filter = request.deshred_transactions.get("all").unwrap();
        assert_eq!(filter.vote, None);
        assert!(request.ping.is_none());
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
                is_vote: true,
                transaction: Some(transaction(nested)),
                ..Default::default()
            }),
            slot: 42,
        };
        let (sig, slot, meta) = transaction_hit(&msg, Some(1_700), 4).unwrap();
        assert_eq!((sig, slot), ([7; 64], 42));
        assert_eq!(meta.is_vote, Some(true));
        assert_eq!(meta.server_created_at_ns, Some(1_700));
        assert_eq!(meta.connection_id, Some(4));
        assert_eq!(meta.message_size, Some(66));
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
        let (sig, slot, meta) = deshred_hit(&msg, None, 0).unwrap();
        assert_eq!((sig, slot), ([5; 64], 84));
        assert_eq!(meta.is_vote, Some(false));
        assert_eq!(meta.server_created_at_ns, None);
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
        assert!(deshred_hit(&malformed, None, 0).is_none());
        assert!(transaction_hit(&SubscribeUpdateTransaction::default(), None, 0).is_none());
    }

    #[test]
    fn a_missing_server_timestamp_is_not_the_epoch() {
        assert_eq!(created_at_ns(None), None);
        assert_eq!(
            created_at_ns(Some(&prost_types::Timestamp { seconds: 2, nanos: 5 })),
            Some(2_000_000_005)
        );
    }

    #[test]
    fn a_bundle_becomes_one_named_filter_per_entry() {
        let cfg = bundles::defaults()
            .into_iter()
            .find(|b| b.name == "combo_never")
            .unwrap();
        let bundle = Bundle::new(&cfg).unwrap();
        let req = deshred_request(&bundle);
        assert_eq!(req.deshred_transactions.len(), 2);
        let jup = &req.deshred_transactions["jup_non_vote"];
        assert_eq!(jup.vote, Some(false));
        assert_eq!(jup.account_include.len(), 1);
        let req = transactions_request(&source(GrpcMode::Transactions, Some("confirmed")), &bundle);
        assert_eq!(req.commitment, Some(CommitmentLevel::Confirmed as i32));
        assert!(req.transactions.contains_key("never"));
    }

    #[test]
    fn views_include_lookup_table_addresses_and_execution_result() {
        let tx = Transaction {
            signatures: vec![vec![3; 64]],
            message: Some(Message {
                account_keys: vec![vec![1; 32], vec![2; 32]],
                instructions: vec![CompiledInstruction {
                    program_id_index: 1,
                    ..Default::default()
                }],
                versioned: true,
                ..Default::default()
            }),
        };
        let msg = SubscribeUpdateTransaction {
            transaction: Some(SubscribeUpdateTransactionInfo {
                transaction: Some(tx.clone()),
                meta: Some(TransactionStatusMeta {
                    err: Some(TransactionError::default()),
                    loaded_writable_addresses: vec![vec![4; 32]],
                    loaded_readonly_addresses: vec![vec![5; 32]],
                    ..Default::default()
                }),
                ..Default::default()
            }),
            slot: 1,
        };
        let v = transaction_view(&msg).unwrap();
        assert_eq!(v.keys.len(), 4);
        assert_eq!(v.num_static_keys, 2);
        assert_eq!(v.first_program, Some([2; 32]));
        assert_eq!(v.failed, Some(true));
        assert!(!v.legacy);

        let d = SubscribeUpdateDeshredTransaction {
            transaction: Some(SubscribeUpdateDeshredTransactionInfo {
                transaction: Some(tx),
                loaded_writable_addresses: vec![vec![4; 32]],
                ..Default::default()
            }),
            slot: 1,
        };
        let v = deshred_view(&d).unwrap();
        assert_eq!(v.keys.len(), 3);
        assert_eq!(v.failed, None, "pre-execution: no result to know");
    }
}
