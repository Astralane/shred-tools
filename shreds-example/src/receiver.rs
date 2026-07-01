//! Blocking UDP receive loop: one datagram == one raw serialized Solana shred.

use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::{error, info, warn};
use tokio::sync::mpsc;

/// A raw shred datagram received off the UDP socket.
pub struct ShredPacket {
    pub data: Vec<u8>,
    pub received_at: Instant,
}

/// Bind `port` and forward every datagram to `tx` until `running` clears.
pub fn run_receiver(port: u16, tx: mpsc::UnboundedSender<ShredPacket>, running: Arc<AtomicBool>) {
    let socket = match UdpSocket::bind(("0.0.0.0", port)) {
        Ok(s) => s,
        Err(e) => {
            error!("failed to bind UDP :{port}: {e}");
            running.store(false, Ordering::SeqCst);
            return;
        }
    };
    socket
        .set_read_timeout(Some(Duration::from_millis(100)))
        .expect("set_read_timeout");

    let mut buf = [0u8; 1280];
    let mut received_counter = 0u64;
    while running.load(Ordering::SeqCst) {
        match socket.recv_from(&mut buf) {
            Ok((n, _)) => {
                let packet = ShredPacket {
                    data: buf[..n].to_vec(),
                    received_at: Instant::now(),
                };
                if tx.send(packet).is_err() {
                    break; // processor gone
                }
                received_counter += 1;
                if received_counter % 50_000 == 0 {
                    info!("received {} packets", received_counter);
                }
            }
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(e) => {
                warn!("udp recv error: {e}");
                continue;
            }
        }
    }
}
