use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub async fn stub(response: &'static str) -> (String, tokio::sync::oneshot::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut raw = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            let n = socket.read(&mut buf).await.unwrap();
            raw.extend_from_slice(&buf[..n]);
            if n == 0 {
                break;
            }
            let text = String::from_utf8_lossy(&raw).to_string();
            if let Some(head_end) = text.find("\r\n\r\n") {
                let want = text
                    .lines()
                    .find(|l| l.to_lowercase().starts_with("content-length:"))
                    .and_then(|l| l.split(':').nth(1))
                    .and_then(|v| v.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                if raw.len() >= head_end + 4 + want {
                    break;
                }
            }
        }
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response.len(),
            response
        );
        let _ = socket.write_all(resp.as_bytes()).await;
        let _ = socket.flush().await;
        let _ = tx.send(String::from_utf8_lossy(&raw).to_string());
    });
    (format!("http://{}/", addr), rx)
}
