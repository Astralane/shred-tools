//! A Rakurai validator running the Astralane patch (`core/src/tpu_relay/tpu_relay_stage.rs`):
//! it authenticates against the relay and pushes every batch down both
//! `AstralaneRelayer/StartRelayStream` (as a bundle) and
//! `BlockEngineRelayer/StartExpiringPacketStream` (as a packet batch).

use std::str::FromStr;
use std::time::Duration;

use clap::Parser;
use solana_keypair::Keypair;
use solana_signer::Signer;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};
use tracing::{info, warn};

use postpack_mock_validator::proto::jito::astralane_relayer::astralane_relayer_client::AstralaneRelayerClient;
use postpack_mock_validator::proto::jito::astralane_relayer::{
    ExpiringBundleBatch, RelayUpdate, relay_update,
};
use postpack_mock_validator::proto::jito::auth::auth_service_client::AuthServiceClient;
use postpack_mock_validator::proto::jito::auth::{
    GenerateAuthChallengeRequest, GenerateAuthTokensRequest, Role,
};
use postpack_mock_validator::proto::jito::block_engine::block_engine_relayer_client::BlockEngineRelayerClient;
use postpack_mock_validator::proto::jito::block_engine::{
    ExpiringPacketBatch, PacketBatchUpdate, StartExpiringPacketStreamResponse, packet_batch_update,
};
use postpack_mock_validator::proto::jito::bundle::{Bundle, BundleUuid};
use postpack_mock_validator::proto::jito::packet::{Meta, Packet, PacketBatch};
use postpack_mock_validator::proto::jito::shared::Header;
use postpack_mock_validator::{Batch, Feed, FeedArgs};

#[derive(Parser)]
#[command(name = "mock-rakurai")]
struct Cli {
    /// The relay's validator ingress.
    #[arg(long, default_value = "http://127.0.0.1:12350")]
    url: String,
    /// Auth identity; derived from --seed when absent.
    #[arg(long)]
    keypair_path: Option<String>,
    #[command(flatten)]
    feed: FeedArgs,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    postpack_mock_validator::init_tracing();
    let cli = Cli::parse();

    let keypair = postpack_mock_validator::identity(cli.keypair_path.as_deref(), cli.feed.seed)?;
    info!(identity = %keypair.pubkey(), url = %cli.url, "authenticating");

    let channel = Endpoint::from_str(&cli.url)?
        .tcp_nodelay(true)
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await?;
    let access_token = authenticate(channel.clone(), &keypair).await?;
    let bearer: MetadataValue<_> = format!("Bearer {access_token}").parse()?;
    let authorize = move |mut request: Request<()>| -> Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("authorization", bearer.clone());
        Ok(request)
    };

    let mut astralane_client =
        AstralaneRelayerClient::with_interceptor(channel.clone(), authorize.clone());
    let (astralane, receiver) = mpsc::channel(256);
    watch_heartbeats(
        "astralane",
        astralane_client
            .start_relay_stream(ReceiverStream::new(receiver))
            .await?
            .into_inner(),
    );

    let mut block_engine_client = BlockEngineRelayerClient::with_interceptor(channel, authorize);
    let (block_engine, receiver) = mpsc::channel(256);
    watch_heartbeats(
        "block_engine",
        block_engine_client
            .start_expiring_packet_stream(ReceiverStream::new(receiver))
            .await?
            .into_inner(),
    );

    info!("streaming to astralane and block_engine");

    let mut batches = Feed::spawn(&cli.feed).subscribe();
    loop {
        let batch = match batches.recv().await {
            Ok(batch) => batch,
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                warn!(skipped, "relay is not keeping up, batches skipped");
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => anyhow::bail!("feed stopped"),
        };
        send_batch(&astralane, &block_engine, &batch).await?;
    }
}

async fn authenticate(channel: Channel, keypair: &Keypair) -> anyhow::Result<String> {
    let mut auth = AuthServiceClient::new(channel);
    let challenge = auth
        .generate_auth_challenge(GenerateAuthChallengeRequest {
            role: Role::Relayer.into(),
            pubkey: keypair.pubkey().to_bytes().to_vec(),
        })
        .await?
        .into_inner()
        .challenge;

    let challenge = format!("{}-{}", keypair.pubkey(), challenge);
    let signed_challenge = keypair.sign_message(challenge.as_bytes()).as_ref().to_vec();
    let tokens = auth
        .generate_auth_tokens(GenerateAuthTokensRequest {
            challenge,
            client_pubkey: keypair.pubkey().as_ref().to_vec(),
            signed_challenge,
        })
        .await?
        .into_inner();

    tokens
        .access_token
        .map(|token| token.value)
        .ok_or_else(|| anyhow::anyhow!("relay issued no access token"))
}

async fn send_batch(
    astralane: &mpsc::Sender<RelayUpdate>,
    block_engine: &mpsc::Sender<PacketBatchUpdate>,
    batch: &Batch,
) -> anyhow::Result<()> {
    let packets: Vec<Packet> = batch.transactions.iter().cloned().map(packet).collect();
    let closed = || anyhow::anyhow!("relay stream closed");
    astralane
        .send(RelayUpdate {
            msg: Some(relay_update::Msg::Bundles(ExpiringBundleBatch {
                header: Some(header()),
                bundles: vec![BundleUuid {
                    bundle: Some(Bundle {
                        header: Some(header()),
                        packets: packets.clone(),
                    }),
                    uuid: format!("mock-{}", batch.index),
                }],
                expiry_ms: 0,
            })),
        })
        .await
        .map_err(|_| closed())?;
    block_engine
        .send(PacketBatchUpdate {
            msg: Some(packet_batch_update::Msg::Batches(ExpiringPacketBatch {
                header: Some(header()),
                batch: Some(PacketBatch { packets }),
                expiry_ms: 0,
            })),
        })
        .await
        .map_err(|_| closed())?;
    Ok(())
}

fn header() -> Header {
    Header {
        ts: Some(postpack_mock_validator::now()),
    }
}

fn packet(data: bytes::Bytes) -> Packet {
    Packet {
        meta: Some(Meta {
            size: data.len() as u64,
            ..Meta::default()
        }),
        data,
    }
}

fn watch_heartbeats(
    stream: &'static str,
    mut responses: tonic::Streaming<StartExpiringPacketStreamResponse>,
) {
    tokio::spawn(async move {
        while let Ok(Some(response)) = responses.message().await {
            if let Some(heartbeat) = response.heartbeat {
                tracing::debug!(stream, count = heartbeat.count, "relay heartbeat");
            }
        }
        warn!(stream, "relay closed the response stream");
    });
}
