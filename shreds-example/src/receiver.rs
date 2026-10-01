use std::io::ErrorKind;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::{error, info, warn};
use tokio::sync::mpsc;

pub struct ShredPacket {
    pub data: Vec<u8>,
    pub received_at: Instant,
}

pub fn run_receiver(port: u16, tx: mpsc::UnboundedSender<ShredPacket>, running: Arc<AtomicBool>) {
    let socket = match UdpSocket::bind(("0.0.0.0", port)) {
        Ok(s) => s,
        Err(e) => {
            error!("failed to bind UDP :{port}: {e}");
            running.store(false, Ordering::SeqCst);
            return;
        }
    };
    // Timeout so the loop notices `running` going false.
    socket
        .set_read_timeout(Some(Duration::from_millis(100)))
        .expect("set_read_timeout");

    let mut buf = [0u8; 1280];
    let mut received = 0u64;
    while running.load(Ordering::SeqCst) {
        match socket.recv_from(&mut buf) {
            Ok((n, _)) => {
                let packet = ShredPacket {
                    data: buf[..n].to_vec(),
                    received_at: Instant::now(),
                };
                if tx.send(packet).is_err() {
                    break;
                }
                received += 1;
                if received.is_multiple_of(50_000) {
                    info!("received {received} packets");
                }
            }
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(e) => warn!("udp recv error: {e}"),
        }
    }
}
