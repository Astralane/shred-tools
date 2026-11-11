use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use log::{info, warn};
use shreds_proto::preconf::{
    Preconf, SubscribePreconfsRequest, preconf_service_client::PreconfServiceClient,
    preconf_stream_message::Message,
};
use tonic::{
    Request, Status,
    metadata::AsciiMetadataValue,
    service::interceptor::InterceptedService,
    transport::{Channel, Endpoint},
};

#[derive(Parser, Debug)]
#[command(about = "Stream post-pack transactions from Astralane's post-pack relay")]
struct Args {
    #[arg(
        long,
        env = "POSTPACK_URL",
        help = "Plaintext gRPC endpoint of the post-pack relay"
    )]
    url: String,

    #[arg(
        long,
        env = "POSTPACK_X_TOKEN",
        help = "Post-pack API key, sent as the `x-token` header"
    )]
    x_token: String,

    #[arg(
        long = "account",
        value_parser = pubkey,
        help = "Deliver transactions that write to this account (repeatable)"
    )]
    accounts: Vec<[u8; 32]>,

    #[arg(
        long = "program",
        value_parser = pubkey,
        help = "Deliver transactions that reference this program (repeatable)"
    )]
    programs: Vec<[u8; 32]>,

    #[arg(
        long = "exclude-account",
        value_parser = pubkey,
        help = "Drop delivered transactions that reference this account (repeatable)"
    )]
    excluded_accounts: Vec<[u8; 32]>,
}

fn pubkey(value: &str) -> Result<[u8; 32]> {
    bs58::decode(value)
        .into_vec()?
        .try_into()
        .map_err(|_| anyhow!("expected a 32-byte base58 pubkey"))
}

fn bytes(keys: &[[u8; 32]]) -> Vec<Vec<u8>> {
    keys.iter().map(|key| key.to_vec()).collect()
}

#[derive(Clone)]
struct XToken(AsciiMetadataValue);

impl tonic::service::Interceptor for XToken {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        req.metadata_mut().insert("x-token", self.0.clone());
        Ok(req)
    }
}

type Client = PreconfServiceClient<InterceptedService<Channel, XToken>>;

async fn connect(args: &Args) -> Result<Client> {
    let channel = Endpoint::from_shared(args.url.clone())?
        .connect_timeout(Duration::from_secs(5))
        .tcp_nodelay(true)
        .connect()
        .await?;
    let token = AsciiMetadataValue::try_from(args.x_token.as_str()).context("invalid x-token")?;
    Ok(PreconfServiceClient::with_interceptor(
        channel,
        XToken(token),
    ))
}

async fn run(args: &Args) -> Result<()> {
    let mut client = connect(args).await.context("connect")?;
    let request = SubscribePreconfsRequest {
        accounts_of_interest: bytes(&args.accounts),
        programs_of_interest: bytes(&args.programs),
        excluded_accounts: bytes(&args.excluded_accounts),
    };
    let mut stream = client
        .subscribe_preconfs(request)
        .await
        .context("subscribe")?
        .into_inner();
    info!("subscribed to {}", args.url);

    while let Some(message) = stream.message().await.context("stream error")? {
        if let Some(Message::Preconf(preconf)) = message.message {
            print_transaction(&preconf);
        }
    }
    bail!("stream ended")
}

fn print_transaction(preconf: &Preconf) {
    let signature = preconf.data.get(1..65).map_or("-".into(), |signature| {
        bs58::encode(signature).into_string()
    });
    info!(
        "slot={} sig={} bytes={}",
        preconf.slot,
        signature,
        preconf.data.len()
    );
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
