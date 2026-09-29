use std::fmt::Write as _;

use ahash::AHashSet;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::sigreg::is_simple_vote;

pub type Key = [u8; 32];

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct FilterCfg {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vote: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub account_include: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub account_exclude: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub account_required: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Filter {
    pub name: String,
    pub cfg: FilterCfg,
    include: AHashSet<Key>,
    exclude: AHashSet<Key>,
    required: Vec<Key>,
}

#[derive(Debug, Clone)]
pub struct TxView {
    pub signature: [u8; 64],
    pub num_signatures: usize,
    pub legacy: bool,
    pub keys: Vec<Key>,
    pub num_static_keys: usize,
    pub num_instructions: usize,
    pub first_program: Option<Key>,
    pub failed: Option<bool>,
}

impl TxView {
    pub fn is_vote(&self) -> bool {
        is_simple_vote(
            self.num_signatures,
            self.legacy,
            self.num_instructions,
            self.first_program.as_ref().map(|k| &k[..]),
        )
    }

    fn contains(&self, key: &Key) -> bool {
        self.keys.iter().any(|k| k == key)
    }

    fn describe_key(&self, key: &Key) -> String {
        let origin = match self.keys.iter().position(|k| k == key) {
            Some(i) if i < self.num_static_keys => "static",
            Some(_) => "lookup table",
            None => "absent",
        };
        format!("{} ({origin})", short(key))
    }
}

fn short(key: &Key) -> String {
    let s = bs58::encode(key).into_string();
    format!("{}…", &s[..8.min(s.len())])
}

fn parse_keys(filter: &str, field: &str, list: &[String]) -> Result<Vec<Key>> {
    list.iter()
        .map(|s| {
            let raw = bs58::decode(s.trim()).into_vec().map_err(|e| {
                anyhow!("filter `{filter}`: {field} entry `{s}` is not base58: {e}")
            })?;
            raw.as_slice().try_into().map_err(|_| {
                anyhow!("filter `{filter}`: {field} entry `{s}` is not a 32-byte pubkey")
            })
        })
        .collect()
}

impl Filter {
    pub fn new(name: &str, cfg: &FilterCfg) -> Result<Self> {
        Ok(Self {
            name: name.to_string(),
            cfg: cfg.clone(),
            include: parse_keys(name, "account_include", &cfg.account_include)?
                .into_iter()
                .collect(),
            exclude: parse_keys(name, "account_exclude", &cfg.account_exclude)?
                .into_iter()
                .collect(),
            required: parse_keys(name, "account_required", &cfg.account_required)?,
        })
    }

    pub fn uses_failed(&self) -> bool {
        self.cfg.failed.is_some()
    }

    pub fn matches(&self, tx: &TxView) -> bool {
        self.mismatch(tx).is_none()
    }

    pub fn mismatch(&self, tx: &TxView) -> Option<String> {
        if let Some(want) = self.cfg.vote {
            let is_vote = tx.is_vote();
            if is_vote != want {
                return Some(format!("vote: filter wants {want}, tx is_vote={is_vote}"));
            }
        }
        if let (Some(want), Some(failed)) = (self.cfg.failed, tx.failed) {
            if failed != want {
                return Some(format!("failed: filter wants {want}, tx failed={failed}"));
            }
        }
        if !self.include.is_empty() && !tx.keys.iter().any(|k| self.include.contains(k)) {
            return Some("account_include: none of the listed keys is an account key".into());
        }
        if let Some(k) = tx.keys.iter().find(|k| self.exclude.contains(*k)) {
            return Some(format!("account_exclude: has {}", tx.describe_key(k)));
        }
        if let Some(k) = self.required.iter().find(|k| !tx.contains(k)) {
            return Some(format!("account_required: missing {}", short(k)));
        }
        None
    }

    pub fn match_reason(&self, tx: &TxView) -> String {
        let mut out = String::new();
        if let Some(want) = self.cfg.vote {
            let _ = write!(out, "vote={want}; ");
        }
        if let (Some(want), Some(_)) = (self.cfg.failed, tx.failed) {
            let _ = write!(out, "failed={want}; ");
        }
        if let Some(k) = tx.keys.iter().find(|k| self.include.contains(*k)) {
            let _ = write!(out, "account_include hit {}; ", tx.describe_key(k));
        }
        if !self.exclude.is_empty() {
            out.push_str("no excluded key; ");
        }
        if !self.required.is_empty() {
            out.push_str("all required keys present; ");
        }
        if out.is_empty() {
            out.push_str("filter has no clauses (matches everything)");
        }
        out.trim_end_matches("; ").to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sigreg::VOTE_PROGRAM_ID;

    fn key(b: u8) -> Key {
        [b; 32]
    }

    fn b58(k: Key) -> String {
        bs58::encode(k).into_string()
    }

    fn tx(static_keys: &[Key], loaded: &[Key]) -> TxView {
        let mut keys = static_keys.to_vec();
        keys.extend_from_slice(loaded);
        TxView {
            signature: [0; 64],
            num_signatures: 1,
            legacy: loaded.is_empty(),
            num_static_keys: static_keys.len(),
            keys,
            num_instructions: 2,
            first_program: Some(static_keys[static_keys.len() - 1]),
            failed: None,
        }
    }

    fn vote_tx() -> TxView {
        TxView {
            signature: [0; 64],
            num_signatures: 1,
            legacy: true,
            keys: vec![key(1), VOTE_PROGRAM_ID],
            num_static_keys: 2,
            num_instructions: 1,
            first_program: Some(VOTE_PROGRAM_ID),
            failed: None,
        }
    }

    fn filter(cfg: FilterCfg) -> Filter {
        Filter::new("f", &cfg).unwrap()
    }

    #[test]
    fn an_empty_filter_matches_everything() {
        let f = filter(FilterCfg::default());
        assert!(f.matches(&tx(&[key(1), key(2)], &[])));
        assert!(f.matches(&vote_tx()));
    }

    #[test]
    fn vote_selects_on_the_simple_vote_rule() {
        let votes = filter(FilterCfg {
            vote: Some(true),
            ..Default::default()
        });
        let non_votes = filter(FilterCfg {
            vote: Some(false),
            ..Default::default()
        });
        assert!(votes.matches(&vote_tx()));
        assert!(!non_votes.matches(&vote_tx()));
        let user = tx(&[key(1), key(2)], &[]);
        assert!(!votes.matches(&user));
        assert!(non_votes.matches(&user));
    }

    #[test]
    fn a_versioned_vote_is_not_a_simple_vote() {
        let mut v = vote_tx();
        v.legacy = false;
        assert!(
            !v.is_vote(),
            "agave only treats legacy messages as simple votes"
        );
    }

    #[test]
    fn include_matches_static_and_lookup_table_keys() {
        let f = filter(FilterCfg {
            account_include: vec![b58(key(9))],
            ..Default::default()
        });
        assert!(f.matches(&tx(&[key(1), key(9)], &[])), "static");
        assert!(
            f.matches(&tx(&[key(1), key(2)], &[key(9)])),
            "loaded via ALT"
        );
        assert!(!f.matches(&tx(&[key(1), key(2)], &[key(3)])));
        assert!(f
            .match_reason(&tx(&[key(1), key(2)], &[key(9)]))
            .contains("lookup table"));
    }

    #[test]
    fn exclude_rejects_any_listed_key_including_loaded_ones() {
        let f = filter(FilterCfg {
            account_exclude: vec![b58(key(9))],
            ..Default::default()
        });
        assert!(f.matches(&tx(&[key(1), key(2)], &[])));
        assert!(!f.matches(&tx(&[key(1), key(2)], &[key(9)])));
        assert!(f
            .mismatch(&tx(&[key(1), key(2)], &[key(9)]))
            .unwrap()
            .contains("lookup table"));
    }

    #[test]
    fn required_needs_every_listed_key() {
        let f = filter(FilterCfg {
            account_required: vec![b58(key(8)), b58(key(9))],
            ..Default::default()
        });
        assert!(f.matches(&tx(&[key(8), key(9)], &[])));
        assert!(f.matches(&tx(&[key(8), key(2)], &[key(9)])));
        assert!(!f.matches(&tx(&[key(8), key(2)], &[])));
    }

    #[test]
    fn failed_is_ignored_where_the_source_cannot_know() {
        let f = filter(FilterCfg {
            failed: Some(true),
            ..Default::default()
        });
        let mut t = tx(&[key(1), key(2)], &[]);
        t.failed = None;
        assert!(f.matches(&t));
        t.failed = Some(false);
        assert!(!f.matches(&t));
        t.failed = Some(true);
        assert!(f.matches(&t));
    }

    #[test]
    fn clauses_combine_with_and() {
        let f = filter(FilterCfg {
            vote: Some(false),
            account_include: vec![b58(key(9))],
            ..Default::default()
        });
        assert!(f.matches(&tx(&[key(1), key(9)], &[])));
        assert!(!f.matches(&tx(&[key(1), key(2)], &[])));
        let mut vote_with_key = vote_tx();
        vote_with_key.keys.push(key(9));
        assert!(!f.matches(&vote_with_key));
    }

    #[test]
    fn bad_pubkeys_are_rejected_with_the_filter_name() {
        let err = Filter::new(
            "mine",
            &FilterCfg {
                account_include: vec!["not-base58-0OIl".into()],
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("mine"));
        let err = Filter::new(
            "mine",
            &FilterCfg {
                account_include: vec!["abc".into()],
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("32-byte"));
    }
}
