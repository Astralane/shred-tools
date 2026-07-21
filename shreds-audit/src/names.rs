//! Validator display names, resolved once at capture and embedded in the
//! manifest so the offline viewer never touches the network. Best-effort: any
//! failure just means the viewer shows pubkeys.
//!
//! The authoritative source is on-chain validator-info (getProgramAccounts on
//! the Config program), but that call is unavailable on every RPC we can reach —
//! public RPCs disable it and an un-indexed validator times out on the full-DB
//! scan. Names are only a cosmetic label, so we take them from the public
//! Firedancer daily validator report (identity pubkey -> name).

use std::time::Duration;

use ahash::AHashMap;
use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;

const REPORT_URL: &str = "https://reports.firedancer.io/api/export";
const LOOKBACK_DAYS: i64 = 7;

/// Fetch `identity pubkey -> display name` from the Firedancer validator report.
/// Today's report may not exist yet, so walk back until a day is available.
pub fn fetch_validator_names() -> Result<AHashMap<Pubkey, String>> {
    let today = chrono::Utc::now().date_naive();
    let mut last_err: Option<anyhow::Error> = None;
    for back in 0..=LOOKBACK_DAYS {
        let day = today - chrono::Duration::days(back);
        let url = format!(
            "{REPORT_URL}?date={}&report_type=validator&period=daily&min_stake=0",
            day.format("%Y-%m-%d")
        );
        match ureq::get(&url)
            .set("user-agent", "curl/8.0")
            .timeout(Duration::from_secs(20))
            .call()
        {
            Ok(resp) => {
                let csv = resp.into_string()?;
                let map = parse_report(&csv);
                if !map.is_empty() {
                    return Ok(map);
                }
            }
            // No report published for that day yet — try an earlier one.
            Err(ureq::Error::Status(404, _)) => {}
            Err(e) => last_err = Some(anyhow!("{e}")),
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow!("no validator report in the lookback window")))
}

fn parse_report(csv: &str) -> AHashMap<Pubkey, String> {
    let mut lines = csv.lines();
    let Some(header) = lines.next() else {
        return AHashMap::new();
    };
    let cols = parse_csv_line(header);
    let (Some(li), Some(ni)) = (
        cols.iter().position(|c| c == "leader"),
        cols.iter().position(|c| c == "name"),
    ) else {
        return AHashMap::new();
    };
    let mut out = AHashMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let f = parse_csv_line(line);
        let (Some(leader), Some(name)) = (f.get(li), f.get(ni)) else {
            continue;
        };
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        if let Ok(id) = leader.parse::<Pubkey>() {
            out.insert(id, name.to_string());
        }
    }
    out
}

/// Split one CSV line into fields, honouring double-quoted fields (which may
/// contain commas and `""`-escaped quotes). Embedded newlines are not expected.
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    cur.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            } else {
                cur.push(c);
            }
        } else {
            match c {
                '"' => in_quotes = true,
                ',' => fields.push(std::mem::take(&mut cur)),
                _ => cur.push(c),
            }
        }
    }
    fields.push(cur);
    fields
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_quoted_fields() {
        assert_eq!(
            parse_csv_line(r#"a,"b,c","d""e",f"#),
            vec!["a", "b,c", "d\"e", "f"]
        );
    }

    #[test]
    fn extracts_leader_name_by_column() {
        let csv = "rank,leader,stake,name\n\
                   1,11111111111111111111111111111111,100,\"Acme, Inc\"\n\
                   2,So11111111111111111111111111111111111111112,50,\n\
                   3,not-a-key,10,Bad";
        let map = parse_report(csv);
        assert_eq!(map.len(), 1);
        assert_eq!(
            map.get(&"11111111111111111111111111111111".parse().unwrap()).map(String::as_str),
            Some("Acme, Inc")
        );
    }
}
