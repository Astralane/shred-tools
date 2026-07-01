use std::{
    net::UdpSocket,
    time::{Duration, Instant},
};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub struct ShredPacket {
    pub data: Vec<u8>,
    pub received_at: Instant,
}

pub struct UdpReceiver {
    pub source_name: String,
    pub udp_port: u16,
}

impl UdpReceiver {
    pub fn run(self, tx: mpsc::UnboundedSender<ShredPacket>, cancel: CancellationToken) {
        let socket = UdpSocket::bind(format!("0.0.0.0:{}", self.udp_port)).unwrap_or_else(|e| {
            panic!(
                "failed to bind UDP port {} ({}): {e}",
                self.udp_port, self.source_name
            )
        });
        socket
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("failed to set read timeout");

        let mut buf = [0u8; 1280];
        loop {
            if cancel.is_cancelled() {
                break;
            }
            match socket.recv_from(&mut buf) {
                Ok((n, _addr)) => {
                    let packet = ShredPacket {
                        data: buf[..n].to_vec(),
                        received_at: Instant::now(),
                    };
                    if tx.send(packet).is_err() {
                        break;
                    }
                }
                Err(ref e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    continue;
                }
                Err(_) => continue,
            }
        }
    }
}
