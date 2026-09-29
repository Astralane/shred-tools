use std::str::FromStr;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use clap::Parser;
use prost_types::Timestamp;
use solana_keypair::{Keypair, read_keypair_file};
use solana_sdk::hash::Hash;
use solana_sdk::transaction::Transaction;
use solana_signer::Signer;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[allow(dead_code)]
mod proto {
    pub mod astralane_relayer {
        tonic::include_proto!("astralane_relayer");
    }
    pub mod auth {
        tonic::include_proto!("auth");
    }
    pub mod block_engine {
        tonic::include_proto!("block_engine");
    }
    pub mod bundle {
        tonic::include_proto!("bundle");
    }
    pub mod packet {
        tonic::include_proto!("packet");
    }
    pub mod shared {
        tonic::include_proto!("shared");
    }
}

use proto::astralane_relayer::astralane_relayer_client::AstralaneRelayerClient;
use proto::astralane_relayer::{ExpiringBundleBatch, RelayUpdate, relay_update::Msg};
use proto::auth::auth_service_client::AuthServiceClient;
use proto::auth::{GenerateAuthChallengeRequest, GenerateAuthTokensRequest, Role};
use proto::block_engine::block_engine_relayer_client::BlockEngineRelayerClient;
use proto::block_engine::{
    ExpiringPacketBatch, PacketBatchUpdate, StartExpiringPacketStreamResponse, packet_batch_update,
};
use proto::bundle::{Bundle, BundleUuid};
use proto::packet::{Meta, Packet, PacketBatch};
use proto::shared::Header;

#[derive(Parser)]
#[command(name = "postpack-mock-validator")]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:12350")]
    url: String,
    #[arg(long)]
    keypair_path: Option<String>,
    #[arg(long, default_value_t = 4)]
    rate: u64,
    #[arg(long, default_value_t = 8)]
    batch_size: u64,
    #[arg(long)]
    repeat: bool,
    #[arg(long, default_value_t = 0)]
    seed: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let cli = Cli::parse();

    let keypair = match &cli.keypair_path {
        Some(path) => read_keypair_file(path).map_err(|e| anyhow::anyhow!("{path}: {e}"))?,
        None => Keypair::new_from_array(tag(b"identity", cli.seed, 0)),
    };
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

    let payer = Keypair::new_from_array(tag(b"payer", cli.seed, 0));
    let rate = cli.rate.max(1);
    let mut ticker = tokio::time::interval(Duration::from_millis(1_000 / rate));
    let mut batch_index = 0u64;
    loop {
        ticker.tick().await;
        // With --repeat every batch is sent twice.
        let range = if cli.repeat {
            batch_index / 2
        } else {
            batch_index
        };
        let base = range * cli.batch_size;
        let packets: Vec<Packet> = (0..cli.batch_size)
            .map(|i| packet(&payer, cli.seed, base + i))
            .collect();

        if send_batch(&astralane, &block_engine, &packets, batch_index)
            .await
            .is_err()
        {
            anyhow::bail!("relay stream closed");
        }
        batch_index += 1;
        if batch_index % rate == 0 {
            info!(batches = batch_index, "pushed");
        }
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
    packets: &[Packet],
    index: u64,
) -> anyhow::Result<()> {
    astralane
        .send(RelayUpdate {
            msg: Some(Msg::Bundles(ExpiringBundleBatch {
                header: Some(header()),
                bundles: vec![BundleUuid {
                    bundle: Some(Bundle {
                        header: Some(header()),
                        packets: packets.to_vec(),
                    }),
                    uuid: format!("mock-{index}"),
                }],
                expiry_ms: 0,
            })),
        })
        .await?;
    block_engine
        .send(PacketBatchUpdate {
            msg: Some(packet_batch_update::Msg::Batches(ExpiringPacketBatch {
                header: Some(header()),
                batch: Some(PacketBatch {
                    packets: packets.to_vec(),
                }),
                expiry_ms: 0,
            })),
        })
        .await?;
    Ok(())
}

fn header() -> Header {
    Header {
        ts: Some(Timestamp::from(SystemTime::now())),
    }
}

fn packet(payer: &Keypair, seed: u64, index: u64) -> Packet {
    let recipient = solana_pubkey::Pubkey::new_from_array(tag(b"recipient", seed, index));
    let blockhash = Hash::new_from_array(tag(b"blockhash", seed, index));
    let instruction =
        solana_system_interface::instruction::transfer(&payer.pubkey(), &recipient, 1);
    let transaction = Transaction::new_signed_with_payer(
        &[instruction],
        Some(&payer.pubkey()),
        &[payer],
        blockhash,
    );
    let data = Bytes::from(bincode::serialize(&transaction).expect("serialize transaction"));
    Packet {
        meta: Some(Meta {
            size: data.len() as u64,
            ..Meta::default()
        }),
        data,
    }
}

fn tag(domain: &[u8], seed: u64, index: u64) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&seed.to_le_bytes());
    bytes[8..16].copy_from_slice(&index.to_le_bytes());
    for (slot, byte) in bytes[16..].iter_mut().zip(domain.iter().cycle()) {
        *slot = *byte;
    }
    bytes
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
