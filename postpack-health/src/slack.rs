use anyhow::{anyhow, Result};
use serde_json::{json, Value};

use crate::checker::Alert;

pub fn payload(alert: &Alert) -> Value {
    match alert {
        Alert::Degraded {
            win_rate,
            target_win_rate,
            p50_ms,
            p90_ms,
            late_count,
            late_rate,
            late_ms_threshold,
            worst_ms,
            pairs,
            window_secs,
        } => {
            let body = format!(
                "Win Rate: {:.1}% (Target: {:.1}%)\nLead: p50 {:+.1} ms | p90 {:+.1} ms\nTail Lag: {} txs ({:.1}%) >{} ms late (Worst: {:.0} ms)\nWindow: Last {} mins ({} matched pairs)",
                win_rate * 100.0,
                target_win_rate * 100.0,
                p50_ms,
                p90_ms,
                late_count,
                late_rate * 100.0,
                late_ms_threshold,
                worst_ms,
                window_secs / 60,
                pairs
            );
            json!({
                "icon_emoji": ":rotating_light:",
                "username": "postpack-degradation-bot",
                "text": format!(":rotating_light: *[DEGRADATION] Postpack vs Shreds Lead Below {:.0}%*\n{}", target_win_rate * 100.0, body),
            })
        }
        Alert::Recovered {
            win_rate,
            pairs,
            window_secs,
        } => json!({
            "icon_emoji": ":white_check_mark:",
            "username": "postpack-degradation-bot",
            "text": format!(
                ":white_check_mark: *Postpack vs Shreds Recovered*\nWin Rate: {:.1}% over the last {} mins ({} matched pairs).\nNo further action is required.",
                win_rate * 100.0,
                window_secs / 60,
                pairs
            ),
        }),
        Alert::NoData { minutes, pairs } => json!({
            "icon_emoji": ":warning:",
            "username": "postpack-degradation-bot",
            "text": format!(
                ":warning: *Postpack degradation checker is blind*\nNo postpack/shreds pairs to compare for {} mins (last window matched {}). Check shred_indexer and the relay subscription.",
                minutes, pairs
            ),
        }),
    }
}

pub async fn post(client: &reqwest::Client, webhook: &str, alert: &Alert) -> Result<()> {
    let response = client.post(webhook).json(&payload(alert)).send().await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("slack {}: {}", status, body.trim()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn degraded() -> Alert {
        Alert::Degraded {
            win_rate: 0.9407,
            target_win_rate: 0.85,
            p50_ms: 139.3,
            p90_ms: 450.6,
            late_count: 29,
            late_rate: 0.042,
            late_ms_threshold: 100,
            worst_ms: -236.9,
            pairs: 691,
            window_secs: 900,
        }
    }

    #[tokio::test]
    async fn degraded_payload_carries_the_window_numbers() {
        let (url, rx) = crate::test_support::stub("ok").await;
        post(&reqwest::Client::new(), &url, &degraded())
            .await
            .unwrap();

        let req = rx.await.unwrap();
        assert!(req.contains("postpack-degradation-bot"), "req was: {req}");
        assert!(req.contains("Win Rate: 94.1%"), "req was: {req}");
        assert!(req.contains("p50 +139.3 ms"), "req was: {req}");
        assert!(req.contains("29 txs (4.2%) >100 ms late"), "req was: {req}");
        assert!(req.contains("691 matched pairs"), "req was: {req}");
    }

    #[tokio::test]
    async fn recovery_payload_is_sent_to_the_same_channel() {
        let (url, rx) = crate::test_support::stub("ok").await;
        post(
            &reqwest::Client::new(),
            &url,
            &Alert::Recovered {
                win_rate: 0.983,
                pairs: 357,
                window_secs: 900,
            },
        )
        .await
        .unwrap();

        let req = rx.await.unwrap();
        assert!(req.contains("Recovered"), "req was: {req}");
        assert!(req.contains("98.3%"), "req was: {req}");
    }
}
