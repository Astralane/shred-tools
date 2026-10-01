use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};

use super::spec::{Filter, FilterCfg};

// Tags are tracked as a u32 bitmask per delivery.
const MAX_FILTERS_PER_BUNDLE: usize = 32;

const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const COMPUTE_BUDGET_PROGRAM: &str = "ComputeBudget111111111111111111111111111111";
const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BundleCfg {
    pub name: String,
    pub filters: BTreeMap<String, FilterCfg>,
}

#[derive(Debug)]
pub struct Bundle {
    pub name: String,
    pub filters: Vec<Filter>,
    pub json: String,
}

impl Bundle {
    pub fn new(cfg: &BundleCfg) -> Result<Self> {
        let filters = cfg
            .filters
            .iter()
            .map(|(name, f)| Filter::new(name, f))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            name: cfg.name.clone(),
            filters,
            json: serde_json::to_string(&cfg.filters)?,
        })
    }

    pub fn all() -> Arc<Self> {
        let cfg = BundleCfg {
            name: "all".into(),
            filters: BTreeMap::from([("all".to_string(), FilterCfg::default())]),
        };
        Arc::new(Self::new(&cfg).expect("static bundle"))
    }

    pub fn fits_deshred(&self) -> bool {
        !self.filters.iter().any(Filter::uses_failed)
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.filters.iter().position(|f| f.name == name)
    }
}

fn never_key() -> String {
    bs58::encode([0xA5u8; 32]).into_string()
}

fn keys(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| s.to_string()).collect()
}

fn include(list: &[&str]) -> FilterCfg {
    FilterCfg {
        account_include: keys(list),
        ..Default::default()
    }
}

pub fn defaults() -> Vec<BundleCfg> {
    let bundle = |name: &str, filters: Vec<(&str, FilterCfg)>| BundleCfg {
        name: name.to_string(),
        filters: filters
            .into_iter()
            .map(|(n, f)| (n.to_string(), f))
            .collect(),
    };
    let non_vote = |cfg: FilterCfg| FilterCfg {
        vote: Some(false),
        ..cfg
    };
    vec![
        bundle("all", vec![("all", FilterCfg::default())]),
        bundle(
            "votes_split",
            vec![
                (
                    "votes",
                    FilterCfg {
                        vote: Some(true),
                        ..Default::default()
                    },
                ),
                ("non_votes", non_vote(FilterCfg::default())),
            ],
        ),
        bundle(
            "token_programs",
            vec![
                ("token", include(&[TOKEN_PROGRAM])),
                ("token_2022", include(&[TOKEN_2022_PROGRAM])),
            ],
        ),
        bundle(
            "alt_mints",
            vec![
                ("usdc", include(&[USDC_MINT])),
                ("wsol_non_vote", non_vote(include(&[WSOL_MINT]))),
            ],
        ),
        bundle(
            "exclude",
            vec![
                (
                    "no_token",
                    FilterCfg {
                        account_exclude: keys(&[TOKEN_PROGRAM]),
                        ..Default::default()
                    },
                ),
                (
                    "non_vote_no_cb",
                    non_vote(FilterCfg {
                        account_exclude: keys(&[COMPUTE_BUDGET_PROGRAM]),
                        ..Default::default()
                    }),
                ),
            ],
        ),
        bundle(
            "required",
            vec![(
                "token_and_cb",
                FilterCfg {
                    account_required: keys(&[TOKEN_PROGRAM, COMPUTE_BUDGET_PROGRAM]),
                    ..Default::default()
                },
            )],
        ),
        bundle(
            "combo_never",
            vec![
                ("jup_non_vote", non_vote(include(&[JUPITER_V6]))),
                (
                    "never",
                    FilterCfg {
                        account_include: vec![never_key()],
                        ..Default::default()
                    },
                ),
            ],
        ),
        bundle(
            "failed_split",
            vec![
                (
                    "failed",
                    FilterCfg {
                        failed: Some(true),
                        ..Default::default()
                    },
                ),
                (
                    "ok_non_vote",
                    non_vote(FilterCfg {
                        failed: Some(false),
                        ..Default::default()
                    }),
                ),
            ],
        ),
    ]
}

pub fn validate(bundles: &[BundleCfg], deshred: bool, _transactions: bool) -> Result<()> {
    if bundles.is_empty() {
        bail!("filter_rotation: `bundles` is empty");
    }
    let mut names = HashSet::new();
    let mut any_deshred = false;
    for b in bundles {
        if !names.insert(b.name.as_str()) {
            bail!("filter_rotation: duplicate bundle name `{}`", b.name);
        }
        if b.filters.is_empty() {
            bail!("filter_rotation: bundle `{}` has no filters", b.name);
        }
        if b.filters.len() > MAX_FILTERS_PER_BUNDLE {
            bail!(
                "filter_rotation: bundle `{}` has {} filters, at most {MAX_FILTERS_PER_BUNDLE}",
                b.name,
                b.filters.len()
            );
        }
        let parsed = Bundle::new(b)
            .map_err(|e| anyhow!("filter_rotation: bundle `{}`: {e}", b.name))?;
        any_deshred |= parsed.fits_deshred();
    }
    if deshred && !any_deshred {
        bail!(
            "filter_rotation: every bundle uses `failed`, which deshred sources cannot \
             subscribe with — add a bundle without it"
        );
    }
    Ok(())
}

pub fn for_mode(bundles: &[BundleCfg], deshred: bool) -> Vec<Arc<Bundle>> {
    bundles
        .iter()
        .filter_map(|b| Bundle::new(b).ok())
        .filter(|b| !deshred || b.fits_deshred())
        .map(Arc::new)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_defaults_parse_and_fit_both_modes() {
        validate(&defaults(), true, true).unwrap();
        let deshred = for_mode(&defaults(), true);
        let tx = for_mode(&defaults(), false);
        assert!(deshred.iter().all(|b| b.fits_deshred()));
        assert_eq!(tx.len(), defaults().len());
        assert_eq!(
            deshred.len(),
            defaults().len() - 1,
            "only failed_split is skipped"
        );
    }

    #[test]
    fn the_never_key_is_a_valid_pubkey() {
        let f = Filter::new(
            "never",
            &FilterCfg {
                account_include: vec![never_key()],
                ..Default::default()
            },
        );
        assert!(f.is_ok());
    }

    #[test]
    fn a_failed_only_rotation_is_rejected_for_deshred() {
        let only_failed: Vec<BundleCfg> = defaults()
            .into_iter()
            .filter(|b| b.name == "failed_split")
            .collect();
        assert!(validate(&only_failed, true, false).is_err());
        assert!(validate(&only_failed, false, true).is_ok());
    }

    #[test]
    fn duplicate_bundle_names_are_rejected() {
        let mut b = defaults();
        b.push(b[0].clone());
        assert!(validate(&b, true, true).is_err());
    }

    #[test]
    fn tags_resolve_to_bitmask_positions() {
        let b = Bundle::new(&defaults()[1]).unwrap();
        assert_eq!(b.index_of("non_votes"), Some(0), "BTreeMap order");
        assert_eq!(b.index_of("votes"), Some(1));
        assert_eq!(b.index_of("nope"), None);
    }
}
