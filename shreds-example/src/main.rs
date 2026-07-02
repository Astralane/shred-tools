//! Example shred-triggered tip bench client.
//!
//! Subscribes to the raw shred stream forwarded by shreds-hub (plain UDP, one
//! serialized Solana shred per datagram), deshreds + decodes each slot into
//! transactions, and watches for any transaction that touches a configurable
//! "trigger" wallet (`--watch-wallet`). The moment one is seen it:
//!   1. builds a simple SOL transfer that tips an Astralane tip account,
//!   2. submits it to iris (`sendTransaction`) as fast as possible,
//!   3. registers it via the `/shred-pay` endpoint and prints when accepted,
//!   4. waits for it to land and logs the slot distance between the trigger
//!      transaction and our landed transaction.
//!
//! Runs until Ctrl-C.
//!
//! NOTE: for this client to receive anything, shreds-hub must be forwarding
//! shreds to this host:`--shred-port` (its listener set, via the DB or a
//! `named_listeners` config entry).

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
use decode::{ingest_shred, SlotState};
use receiver::{run_receiver, ShredPacket};
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
    let keypair = Arc::new(
        Keypair::read_from_file(&args.keypair_path)
            .map_err(|e| anyhow::anyhow!("failed to read keypair: {e}"))?,
    );

    let rpc = Arc::new(RpcClient::new_with_commitment(
        args.rpc_url.clone(),
        CommitmentConfig::confirmed(),
    ));

    // Prime the blockhash cache up front so the first trigger is instant.
    let initial_blockhash = rpc.get_latest_blockhash().await?;
    let blockhash = Arc::new(RwLock::new(initial_blockhash));

    info!(
        "shreds-example started | watch={} | tip {} lamports -> {} | shred udp :{} | iris={} | shred-pay={}",
        watch_wallet, args.tip_lamports, tip_to, args.shred_port, args.iris_url, args.shred_pay_url
    );

    let shared = Arc::new(Shared {
        args: args.clone(),
        keypair,
        tip_to,
        http: Client::new(),
        rpc: rpc.clone(),
        blockhash: blockhash.clone(),
    });

    let running = Arc::new(AtomicBool::new(true));

    // Ctrl-C -> stop.
    {
        let running = running.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            warn!("ctrl-c received, shutting down");
            running.store(false, Ordering::SeqCst);
        });
    }

    // Background blockhash refresher.
    {
        let rpc = rpc.clone();
        let blockhash = blockhash.clone();
        let running = running.clone();
        tokio::spawn(async move {
            while running.load(Ordering::SeqCst) {
                if let Ok(bh) = rpc.get_latest_blockhash().await {
                    *blockhash.write().await = bh;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    // UDP receiver runs on a dedicated blocking thread and feeds the processor.
    let (tx, mut rx) = mpsc::unbounded_channel::<ShredPacket>();
    let recv_handle = {
        let running = running.clone();
        let port = args.shred_port;
        std::thread::spawn(move || run_receiver(port, tx, running))
    };

    // Processor: decode shreds, detect triggers, spawn a handler per new trigger.
    let mut slots: HashMap<u64, SlotState> = HashMap::new();
    let mut seen_triggers: HashSet<String> = HashSet::new();
    let mut last_prune = Instant::now();
    let mut last_sent = Instant::now();
    let ttl = Duration::from_secs(args.slot_ttl_secs);

    while running.load(Ordering::SeqCst) {
        match tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
            Ok(Some(packet)) => {
                for (trigger_slot, trigger_sig) in
                    ingest_shred(&mut slots, &packet, &watch_wallet, &mut seen_triggers)
                {
                    if last_sent.elapsed() >= Duration::from_secs(1) {
                        last_sent = Instant::now();
                        let shared = shared.clone();
                        tokio::spawn(async move {
                            handle_trigger(shared, trigger_slot, trigger_sig).await;
                        });
                    }
                }
            }
            Ok(None) => break, // receiver thread gone
            Err(_) => {}       // idle tick
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

    running.store(false, Ordering::SeqCst);
    let _ = recv_handle.join();
    info!("shreds-example stopped");
    Ok(())
}
