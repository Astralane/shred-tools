//! A Gravity (gattaca solana-relay) relay. The relay is the client here: it authenticates
//! as a searcher through `auth.AuthService`, then opens `searcher.SearcherService`'s
//! `SubscribeMempool` and `SubscribeBundles` with the issued bearer token. Every feed
//! batch goes down both: as one `PendingTxNotification` and as one `JitoBundle` with its
//! signatures zeroed, as Gravity emits them. Both streams also carry an empty heartbeat
//! message every second.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use clap::Parser;
use solana_pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_signer::Signer;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use postpack_tools::proto::gravity::auth::auth_service_server::{AuthService, AuthServiceServer};
use postpack_tools::proto::gravity::auth::{
    GenerateAuthChallengeRequest, GenerateAuthChallengeResponse, GenerateAuthTokensRequest,
    GenerateAuthTokensResponse, Token,
};
use postpack_tools::proto::gravity::bundle::JitoBundle;
use postpack_tools::proto::gravity::packet::{Meta, Packet};
use postpack_tools::proto::gravity::searcher::searcher_service_server::{
    SearcherService, SearcherServiceServer,
};
use postpack_tools::proto::gravity::searcher::{
    BundleSubscription, MempoolSubscription, PendingTxNotification, SendBackrunBundleRequest,
    SendBackrunBundleResponse, SendBundleRequest, SendBundleResponse,
};
use postpack_tools::proto::gravity::shared::Header;
use postpack_tools::{Batch, Feed, FeedArgs};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
const TOKEN_TTL: Duration = Duration::from_secs(30 * 60);

#[derive(Parser)]
#[command(name = "mock-gravity")]
struct Cli {
    /// Where the relay's `gravity_relay_client.endpoints` point.
    #[arg(long, default_value = "127.0.0.1:12370")]
    listen: SocketAddr,
    /// Validator identity stamped as `identity_address`; derived from --seed when absent.
    #[arg(long)]
    identity: Option<Pubkey>,
    #[command(flatten)]
    feed: FeedArgs,
}

#[derive(Default)]
struct Sessions {
    nonce: AtomicU64,
    /// Outstanding challenge per pubkey.
    challenges: Mutex<HashMap<Pubkey, String>>,
    tokens: Mutex<HashSet<String>>,
}

impl Sessions {
    fn fresh(&self) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("{nanos:x}{:x}", self.nonce.fetch_add(1, Ordering::Relaxed))
    }

    fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let token = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or_else(|| Status::unauthenticated("missing bearer token"))?;
        if self.tokens.lock().unwrap().contains(token) {
            Ok(())
        } else {
            Err(Status::unauthenticated("unknown access token"))
        }
    }
}

#[derive(Clone)]
struct Auth(Arc<Sessions>);

#[tonic::async_trait]
impl AuthService for Auth {
    async fn generate_auth_challenge(
        &self,
        request: Request<GenerateAuthChallengeRequest>,
    ) -> Result<Response<GenerateAuthChallengeResponse>, Status> {
        let pubkey = Pubkey::try_from(request.get_ref().pubkey.as_slice())
            .map_err(|_| Status::invalid_argument("invalid pubkey"))?;
        let challenge = self.0.fresh();
        self.0
            .challenges
            .lock()
            .unwrap()
            .insert(pubkey, challenge.clone());
        Ok(Response::new(GenerateAuthChallengeResponse { challenge }))
    }

    async fn generate_auth_tokens(
        &self,
        request: Request<GenerateAuthTokensRequest>,
    ) -> Result<Response<GenerateAuthTokensResponse>, Status> {
        let request = request.into_inner();
        let pubkey = Pubkey::try_from(request.client_pubkey.as_slice())
            .map_err(|_| Status::invalid_argument("invalid client_pubkey"))?;
        let issued = self
            .0
            .challenges
            .lock()
            .unwrap()
            .remove(&pubkey)
            .ok_or_else(|| Status::permission_denied("no outstanding challenge"))?;
        // Clients sign "{pubkey}-{challenge}".
        if request.challenge != format!("{pubkey}-{issued}") {
            return Err(Status::permission_denied("challenge mismatch"));
        }
        let signature = Signature::try_from(request.signed_challenge.as_slice())
            .map_err(|_| Status::invalid_argument("malformed signed_challenge"))?;
        if !signature.verify(pubkey.as_ref(), request.challenge.as_bytes()) {
            return Err(Status::permission_denied("bad signature"));
        }

        let expires_at_utc = Some(prost_types::Timestamp::from(SystemTime::now() + TOKEN_TTL));
        let access = self.0.fresh();
        self.0.tokens.lock().unwrap().insert(access.clone());
        info!(%pubkey, "searcher authenticated");
        Ok(Response::new(GenerateAuthTokensResponse {
            access_token: Some(Token {
                value: access,
                expires_at_utc,
            }),
            refresh_token: Some(Token {
                value: self.0.fresh(),
                expires_at_utc,
            }),
        }))
    }
}

struct Searcher {
    sessions: Arc<Sessions>,
    feed: Feed,
    identity: Bytes,
    seed: u64,
}

#[tonic::async_trait]
impl SearcherService for Searcher {
    async fn send_bundle(
        &self,
        _request: Request<SendBundleRequest>,
    ) -> Result<Response<SendBundleResponse>, Status> {
        Err(Status::unimplemented("mock-gravity only streams"))
    }

    async fn send_backrun_bundle(
        &self,
        _request: Request<SendBackrunBundleRequest>,
    ) -> Result<Response<SendBackrunBundleResponse>, Status> {
        Err(Status::unimplemented("mock-gravity only streams"))
    }

    type SubscribeMempoolStream = ReceiverStream<Result<PendingTxNotification, Status>>;

    async fn subscribe_mempool(
        &self,
        request: Request<MempoolSubscription>,
    ) -> Result<Response<Self::SubscribeMempoolStream>, Status> {
        self.sessions.authorize(&request)?;
        info!(peer = ?request.remote_addr(), "mempool subscribed");
        let identity = self.identity.clone();
        Ok(Response::new(subscribe(
            &self.feed,
            "mempool",
            move |batch| PendingTxNotification {
                transactions: batch.transactions.iter().cloned().map(packet).collect(),
                identity_address: identity.clone(),
            },
        )))
    }

    type SubscribeBundlesStream = ReceiverStream<Result<JitoBundle, Status>>;

    async fn subscribe_bundles(
        &self,
        request: Request<BundleSubscription>,
    ) -> Result<Response<Self::SubscribeBundlesStream>, Status> {
        self.sessions.authorize(&request)?;
        info!(peer = ?request.remote_addr(), "bundles subscribed");
        let identity = self.identity.clone();
        let seed = self.seed;
        Ok(Response::new(subscribe(
            &self.feed,
            "bundles",
            move |batch| JitoBundle {
                header: Some(Header {
                    ts: Some(postpack_tools::now()),
                }),
                identity_address: identity.clone(),
                bundle_id: Bytes::copy_from_slice(&postpack_tools::tag(
                    b"bundle",
                    seed,
                    batch.index,
                )),
                packets: batch
                    .transactions
                    .iter()
                    .map(|transaction| packet(postpack_tools::redact_signatures(transaction)))
                    .collect(),
            },
        )))
    }
}

/// Streams one `T` per feed batch, plus `T::default()` (Gravity's heartbeat) every second.
fn subscribe<T, F>(feed: &Feed, stream: &'static str, map: F) -> ReceiverStream<Result<T, Status>>
where
    T: Default + Send + 'static,
    F: Fn(&Batch) -> T + Send + 'static,
{
    let mut batches = feed.subscribe();
    let (sender, receiver) = mpsc::channel(1024);
    tokio::spawn(async move {
        let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
        loop {
            let message = tokio::select! {
                _ = heartbeat.tick() => Ok(T::default()),
                batch = batches.recv() => match batch {
                    Ok(batch) => Ok(map(&batch)),
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(stream, skipped, "subscriber lagged, closing");
                        Err(Status::resource_exhausted("subscriber lagged"))
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
            };
            let failed = message.is_err();
            if sender.send(message).await.is_err() {
                info!(stream, "unsubscribed");
                break;
            }
            if failed {
                break;
            }
        }
    });
    ReceiverStream::new(receiver)
}

fn packet(data: Bytes) -> Packet {
    Packet {
        meta: Some(Meta {
            size: data.len() as u64,
            ..Meta::default()
        }),
        data,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    postpack_tools::init_tracing();
    let cli = Cli::parse();

    let identity = match cli.identity {
        Some(identity) => identity,
        None => postpack_tools::identity(None, cli.feed.seed)?.pubkey(),
    };
    let sessions = Arc::new(Sessions::default());
    let searcher = Searcher {
        sessions: Arc::clone(&sessions),
        feed: Feed::spawn(&cli.feed),
        identity: Bytes::copy_from_slice(identity.as_ref()),
        seed: cli.feed.seed,
    };
    info!(listen = %cli.listen, %identity, "serving gravity AuthService and SearcherService");
    Server::builder()
        .add_service(AuthServiceServer::new(Auth(sessions)))
        .add_service(SearcherServiceServer::new(searcher))
        .serve_with_shutdown(cli.listen, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
