//! A Jito BAM block engine's preconfirmation stream. The relay is the client here: it
//! calls `bam_preconf.PreconfStream/Subscribe` with `x-bam-pubkey` and its signature over
//! `signed_slot`, and receives one `PreconfUpdate` per transaction. Each feed batch is
//! one bundle: `sequence_id` is the batch index, `bundle_position` the transaction's
//! place in it.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use clap::Parser;
use solana_pubkey::Pubkey;
use solana_sdk::signature::Signature;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use postpack_mock_validator::proto::jito::bam_preconf::preconf_stream_server::{
    PreconfStream, PreconfStreamServer,
};
use postpack_mock_validator::proto::jito::bam_preconf::{PreconfSubscribeRequest, PreconfUpdate};
use postpack_mock_validator::{Feed, FeedArgs};

#[derive(Parser)]
#[command(name = "mock-bam")]
struct Cli {
    /// Where the relay's `bam_preconf_ingress.endpoints` point.
    #[arg(long, default_value = "127.0.0.1:12360")]
    listen: SocketAddr,
    /// Slot stamped on every update; 0 makes the relay use its own current slot.
    #[arg(long, default_value_t = 0)]
    slot: u64,
    /// Pubkeys allowed to subscribe (repeatable); any correctly signed pubkey when absent.
    #[arg(long = "allow")]
    allowed: Vec<Pubkey>,
    #[command(flatten)]
    feed: FeedArgs,
}

struct Bam {
    feed: Feed,
    slot: u64,
    allowed: Arc<HashSet<Pubkey>>,
}

impl Bam {
    fn authenticate(&self, request: &Request<PreconfSubscribeRequest>) -> Result<Pubkey, Status> {
        let pubkey = request
            .metadata()
            .get("x-bam-pubkey")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| Pubkey::from_str(value).ok())
            .ok_or_else(|| Status::unauthenticated("missing or invalid x-bam-pubkey"))?;
        let message = request.get_ref();
        let signature = Signature::try_from(message.signature.as_ref())
            .map_err(|_| Status::unauthenticated("malformed signature"))?;
        if !signature.verify(pubkey.as_ref(), &message.signed_slot.to_le_bytes()) {
            return Err(Status::unauthenticated("signature does not match x-bam-pubkey"));
        }
        if !self.allowed.is_empty() && !self.allowed.contains(&pubkey) {
            return Err(Status::permission_denied("pubkey is not allowed"));
        }
        Ok(pubkey)
    }
}

#[tonic::async_trait]
impl PreconfStream for Bam {
    type SubscribeStream = ReceiverStream<Result<PreconfUpdate, Status>>;

    async fn subscribe(
        &self,
        request: Request<PreconfSubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let pubkey = self.authenticate(&request)?;
        let peer = request.remote_addr();
        info!(%pubkey, ?peer, signed_slot = request.get_ref().signed_slot, "subscribed");

        let mut batches = self.feed.subscribe();
        let slot = self.slot;
        let (sender, receiver) = mpsc::channel(1024);
        tokio::spawn(async move {
            loop {
                let batch = match batches.recv().await {
                    Ok(batch) => batch,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        // What BAM does to a slow subscriber: the relay reconnects as "lag".
                        warn!(%pubkey, skipped, "subscriber lagged, closing");
                        let _ = sender
                            .send(Err(Status::resource_exhausted("subscriber lagged")))
                            .await;
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                };
                for (position, transaction) in batch.transactions.iter().enumerate() {
                    let update = PreconfUpdate {
                        slot,
                        sequence_id: batch.index as u32,
                        bundle_position: position as u32,
                        is_revert_on_error: false,
                        transaction: transaction.clone(),
                    };
                    if sender.send(Ok(update)).await.is_err() {
                        info!(%pubkey, ?peer, "unsubscribed");
                        return;
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    postpack_mock_validator::init_tracing();
    let cli = Cli::parse();

    let bam = Bam {
        feed: Feed::spawn(&cli.feed),
        slot: cli.slot,
        allowed: Arc::new(cli.allowed.into_iter().collect()),
    };
    info!(listen = %cli.listen, "serving bam_preconf.PreconfStream");
    Server::builder()
        .add_service(PreconfStreamServer::new(bam))
        .serve_with_shutdown(cli.listen, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
