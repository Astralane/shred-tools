//! Minimal client for Astralane's Decoded Shreds (`SubscribeDeshred`) gRPC service.
//!
//! Subscribes with a single account filter, prints every decoded transaction as
//! it arrives together with the server-to-client latency, answers server pings,
//! and reconnects with a short backoff if the stream drops.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::Parser;
use futures::{SinkExt, StreamExt, channel::mpsc};
use log::{info, warn};
use shreds_proto::geyser::{
    SubscribeDeshredRequest, SubscribeRequestFilterDeshredTransactions, SubscribeRequestPing,
    SubscribeUpdateDeshredTransaction, geyser_client::GeyserClient,
    subscribe_update_deshred::UpdateOneof,
};
use tonic::{
    Request, Status,
    metadata::AsciiMetadataValue,
    service::interceptor::InterceptedService,
    transport::{Channel, Endpoint},
};

#[derive(Parser, Debug)]
#[command(about = "Stream decoded transactions from Astralane's Decoded Shreds service")]
struct Args {
    /// Plaintext gRPC endpoint, e.g. http://fr1.shreds.astralane.io:12000.
    #[arg(long, env = "DESHRED_URL", default_value = "http://fr1.shreds.astralane.io:12000")]
    url: String,

    /// Shreds API key, sent as the `x-token` header.
    #[arg(long, env = "DESHRED_X_TOKEN")]
    x_token: String,

    /// Deliver transactions that reference any of these accounts (repeatable).
    #[arg(long = "account-include")]
    account_include: Vec<String>,

    /// Drop transactions that reference any of these accounts (repeatable).
    #[arg(long = "account-exclude")]
    account_exclude: Vec<String>,

    /// Deliver only transactions that reference all of these accounts (repeatable).
    #[arg(long = "account-required")]
    account_required: Vec<String>,

    /// Vote filter: `true` = only votes, `false` = no votes; omit for both.
    #[arg(long)]
    vote: Option<bool>,
}

#[derive(Clone)]
struct XToken(AsciiMetadataValue);

impl tonic::service::Interceptor for XToken {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        req.metadata_mut().insert("x-token", self.0.clone());
        Ok(req)
    }
}

type Client = GeyserClient<InterceptedService<Channel, XToken>>;

async fn connect(args: &Args) -> Result<Client> {
    let channel = Endpoint::from_shared(args.url.clone())?
        .connect_timeout(Duration::from_secs(5))
        .tcp_nodelay(true)
        .connect()
        .await?;
    let token = AsciiMetadataValue::try_from(args.x_token.as_str()).context("invalid x-token")?;
    Ok(GeyserClient::with_interceptor(channel, XToken(token))
        .max_decoding_message_size(64 * 1024 * 1024))
}

fn subscribe_request(args: &Args) -> SubscribeDeshredRequest {
    let filter = SubscribeRequestFilterDeshredTransactions {
        vote: args.vote,
        account_include: args.account_include.clone(),
        account_exclude: args.account_exclude.clone(),
        account_required: args.account_required.clone(),
    };
    SubscribeDeshredRequest {
        // The key is echoed back in `SubscribeUpdateDeshred.filters` for matching updates.
        deshred_transactions: HashMap::from([("example".to_string(), filter)]),
        ping: None,
    }
}

/// Opens one subscription and prints updates until the stream ends or errors.
async fn run(args: &Args) -> Result<()> {
    let mut client = connect(args).await.context("connect")?;
    let (mut sub_tx, sub_rx) = mpsc::unbounded();
    sub_tx.send(subscribe_request(args)).await?;
    let mut stream = client.subscribe_deshred(sub_rx).await.context("subscribe")?.into_inner();
    info!("subscribed to {}", args.url);

    while let Some(msg) = stream.next().await {
        let msg = msg.context("stream error")?;
        match msg.update_oneof {
            Some(UpdateOneof::DeshredTransaction(tx)) => {
                print_transaction(&tx, msg.created_at.as_ref(), &msg.filters)
            }
            // The server pings idle streams; reply so the connection is kept alive.
            Some(UpdateOneof::Ping(_)) => {
                let _ = sub_tx.unbounded_send(SubscribeDeshredRequest {
                    ping: Some(SubscribeRequestPing { id: 1 }),
                    ..Default::default()
                });
            }
            Some(UpdateOneof::Slot(slot)) => info!("slot {} status {:?}", slot.slot, slot.status()),
            Some(UpdateOneof::Pong(_)) | None => {}
        }
    }
    bail!("stream ended")
}

fn print_transaction(
    update: &SubscribeUpdateDeshredTransaction,
    created_at: Option<&prost_types::Timestamp>,
    filters: &[String],
) {
    let Some(info) = &update.transaction else { return };
    let Some(tx) = &info.transaction else { return };
    let Some(signature) = tx.signatures.first() else { return };

    let (accounts, instructions, priority_fee) = tx
        .message
        .as_ref()
        .map(|m| {
            let accounts = m.account_keys.len()
                + info.loaded_writable_addresses.len()
                + info.loaded_readonly_addresses.len();
            let fee = m.config.as_ref().and_then(|c| c.priority_fee);
            (accounts, m.instructions.len(), fee)
        })
        .unwrap_or_default();

    info!(
        "slot={} sig={} vote={} accounts={} ixs={} priority_fee={} latency={} filters={:?}",
        update.slot,
        bs58::encode(signature).into_string(),
        info.is_vote,
        accounts,
        instructions,
        priority_fee.map_or("-".into(), |f| f.to_string()),
        latency(created_at).map_or("-".into(), |d| format!("{:.2}ms", d.as_secs_f64() * 1e3)),
        filters,
    );
}

/// Time between the server stamping the update and this client receiving it.
fn latency(created_at: Option<&prost_types::Timestamp>) -> Option<Duration> {
    let ts = created_at?;
    let created = Duration::new(ts.seconds.try_into().ok()?, ts.nanos.try_into().ok()?);
    SystemTime::now().duration_since(UNIX_EPOCH).ok()?.checked_sub(created)
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                warn!("ctrl-c received, shutting down");
                return Ok(());
            }
            res = run(&args) => {
                if let Err(e) = res {
                    warn!("{e:#}; reconnecting in 1s");
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
