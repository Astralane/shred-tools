mod config;
mod decode;
mod receiver;
mod tip;
mod trigger;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use log::{info, warn};
use reqwest::Client;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{EncodableKey, Keypair};
use tokio::sync::{mpsc, RwLock};

use config::Args;
use decode::ingest_shred;
use receiver::run_receiver;
use trigger::{handle_trigger, Shared};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();

    let watch_wallet: Pubkey = args
        .watch_wallet
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid --watch-wallet pubkey: {}", args.watch_wallet))?;
    let tip_to: Pubkey = args
        .tip_address
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid --tip-address pubkey: {}", args.tip_address))?;
    let keypair = Keypair::read_from_file(&args.keypair_path)
        .map_err(|e| anyhow::anyhow!("failed to read keypair: {e}"))?;

    let rpc = RpcClient::new_with_commitment(args.rpc_url.clone(), CommitmentConfig::confirmed());
    // Fetch up front so the first trigger doesn't wait on RPC.
    let blockhash = RwLock::new(rpc.get_latest_blockhash().await?);

    info!(
        "shreds-example started | watch={} | tip {} lamports -> {} | shred udp :{} | iris={} | shred-pay={}",
        watch_wallet, args.tip_lamports, tip_to, args.shred_port, args.iris_url, args.shred_pay_url
    );

    let shred_port = args.shred_port;
    let ttl = Duration::from_secs(args.slot_ttl_secs);
    let shared = Arc::new(Shared {
        args,
        keypair,
        tip_to,
        http: Client::new(),
        rpc,
        blockhash,
    });

    let running = Arc::new(AtomicBool::new(true));

    {
        let running = running.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            warn!("ctrl-c received, shutting down");
            running.store(false, Ordering::SeqCst);
        });
    }

    {
        let shared = shared.clone();
        let running = running.clone();
        tokio::spawn(async move {
            while running.load(Ordering::SeqCst) {
                if let Ok(bh) = shared.rpc.get_latest_blockhash().await {
                    *shared.blockhash.write().await = bh;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    let (tx, mut rx) = mpsc::unbounded_channel();
    let recv_handle = {
        let running = running.clone();
        std::thread::spawn(move || run_receiver(shred_port, tx, running))
    };

    let mut slots = HashMap::new();
    let mut seen_triggers = HashSet::new();
    let mut last_prune = Instant::now();
    let mut last_sent = Instant::now();

    while running.load(Ordering::SeqCst) {
        match tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
            Ok(Some(packet)) => {
                for (trigger_slot, trigger_sig) in
                    ingest_shred(&mut slots, packet, &watch_wallet, &mut seen_triggers)
                {
                    // At most one tip tx per second.
                    if last_sent.elapsed() >= Duration::from_secs(1) {
                        last_sent = Instant::now();
                        tokio::spawn(handle_trigger(shared.clone(), trigger_slot, trigger_sig));
                    }
                }
            }
            Ok(None) => break,
            Err(_) => {}
        }

        if last_prune.elapsed() >= Duration::from_secs(2) {
            let now = Instant::now();
            slots.retain(|_, s| now.duration_since(s.last_received) < ttl);
            if seen_triggers.len() > 100_000 {
                seen_triggers.clear();
            }
            last_prune = now;
        }
    }

    let _ = recv_handle.join();
    info!("shreds-example stopped");
    Ok(())
}
