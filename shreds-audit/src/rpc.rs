use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RpcProvider {
    #[default]
    Generic,
    Shyft,
    Helius,
    Triton,
}

impl RpcProvider {
    pub fn label(self) -> &'static str {
        match self {
            Self::Generic => "generic",
            Self::Shyft => "shyft",
            Self::Helius => "helius",
            Self::Triton => "triton",
        }
    }

    fn default_url(self) -> Option<&'static str> {
        match self {
            Self::Shyft => Some("https://rpc.shyft.to"),
            Self::Helius => Some("https://mainnet.helius-rpc.com"),
            Self::Generic | Self::Triton => None,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RpcCfg {
    #[serde(default)]
    pub provider: RpcProvider,
    #[serde(default)]
    pub url: String,
    #[serde(default, skip_serializing)]
    pub token: Option<String>,
}

impl RpcCfg {
    pub fn generic(url: &str) -> Self {
        Self {
            provider: RpcProvider::Generic,
            url: url.to_string(),
            token: None,
        }
    }

    pub fn resolve(&self) -> Result<RpcEndpoint> {
        let base = if self.url.trim().is_empty() {
            match self.provider.default_url() {
                Some(u) => u.to_string(),
                None => bail!(
                    "rpc: provider `{}` has no default url — set `url`",
                    self.provider.label()
                ),
            }
        } else {
            self.url.trim().trim_end_matches('/').to_string()
        };
        let token = match self.token.as_deref().map(str::trim) {
            None | Some("") => None,
            Some(t) => match t.strip_prefix("env:") {
                Some(var) => Some(
                    std::env::var(var)
                        .map_err(|_| anyhow!("rpc: token is `env:{var}` but ${var} is not set"))?,
                ),
                None => Some(t.to_string()),
            },
        };
        if token.is_none() && self.provider != RpcProvider::Generic {
            bail!(
                "rpc: provider `{}` needs a `token` (API key)",
                self.provider.label()
            );
        }
        Ok(RpcEndpoint {
            provider: self.provider,
            base,
            token,
        })
    }
}

#[derive(Clone)]
pub struct RpcEndpoint {
    provider: RpcProvider,
    base: String,
    token: Option<String>,
}

#[derive(Deserialize)]
struct Envelope<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "rpc error {}: {}", self.code, self.message)
    }
}

impl std::error::Error for RpcError {}

impl RpcEndpoint {
    pub fn label(&self) -> String {
        format!("{} {}", self.provider.label(), self.base)
    }

    fn url(&self) -> String {
        let Some(token) = &self.token else {
            return self.base.clone();
        };
        let sep = if self.base.contains('?') { '&' } else { '?' };
        match self.provider {
            RpcProvider::Generic => self.base.clone(),
            RpcProvider::Shyft => format!("{}{sep}api_key={token}", self.base),
            RpcProvider::Helius => format!("{}{sep}api-key={token}", self.base),
            RpcProvider::Triton => format!("{}/{token}", self.base),
        }
    }

    pub fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
        timeout: Duration,
    ) -> Result<T> {
        let body = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": method, "params": params
        });
        let mut req = ureq::post(&self.url())
            .set("content-type", "application/json")
            .timeout(timeout);
        if let (RpcProvider::Generic, Some(t)) = (self.provider, &self.token) {
            req = req.set("authorization", &format!("Bearer {t}"));
        }
        let resp = req.send_json(body).map_err(|e| match e {
            ureq::Error::Status(code, _) => {
                anyhow!("rpc {method}: HTTP {code} from {}", self.label())
            }
            ureq::Error::Transport(t) => {
                anyhow!(
                    "rpc {method}: transport error to {}: {}",
                    self.label(),
                    t.kind()
                )
            }
        })?;
        let env: Envelope<T> = resp.into_json()?;
        if let Some(err) = env.error {
            return Err(err.into());
        }
        env.result
            .ok_or_else(|| anyhow!("rpc {method}: empty result"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(provider: RpcProvider, url: &str, token: Option<&str>) -> RpcCfg {
        RpcCfg {
            provider,
            url: url.into(),
            token: token.map(Into::into),
        }
    }

    #[test]
    fn each_provider_places_the_token_its_own_way() {
        let shyft = cfg(RpcProvider::Shyft, "", Some("K")).resolve().unwrap();
        assert_eq!(shyft.url(), "https://rpc.shyft.to?api_key=K");
        let helius = cfg(RpcProvider::Helius, "", Some("K")).resolve().unwrap();
        assert_eq!(helius.url(), "https://mainnet.helius-rpc.com?api-key=K");
        let triton = cfg(RpcProvider::Triton, "https://x.rpcpool.com/", Some("K"))
            .resolve()
            .unwrap();
        assert_eq!(triton.url(), "https://x.rpcpool.com/K");
        let generic = cfg(RpcProvider::Generic, "http://node:8899", Some("K"))
            .resolve()
            .unwrap();
        assert_eq!(generic.url(), "http://node:8899");
    }

    #[test]
    fn an_existing_query_string_is_extended_not_replaced() {
        let e = cfg(
            RpcProvider::Shyft,
            "https://rpc.shyft.to?network=mainnet-beta",
            Some("K"),
        )
        .resolve()
        .unwrap();
        assert_eq!(
            e.url(),
            "https://rpc.shyft.to?network=mainnet-beta&api_key=K"
        );
    }

    #[test]
    fn the_label_never_contains_the_token() {
        let e = cfg(RpcProvider::Shyft, "", Some("SECRET"))
            .resolve()
            .unwrap();
        assert!(!e.label().contains("SECRET"));
        assert_eq!(e.label(), "shyft https://rpc.shyft.to");
    }

    #[test]
    fn hosted_providers_require_a_token() {
        assert!(cfg(RpcProvider::Shyft, "", None).resolve().is_err());
        assert!(cfg(RpcProvider::Generic, "http://n:8899", None)
            .resolve()
            .is_ok());
    }

    #[test]
    fn a_provider_without_a_default_needs_a_url() {
        assert!(cfg(RpcProvider::Triton, "", Some("K")).resolve().is_err());
    }

    #[test]
    fn env_tokens_are_read_from_the_environment() {
        std::env::set_var("SHRED_AUDIT_TEST_RPC_TOKEN", "FROMENV");
        let e = cfg(
            RpcProvider::Shyft,
            "",
            Some("env:SHRED_AUDIT_TEST_RPC_TOKEN"),
        )
        .resolve()
        .unwrap();
        assert_eq!(e.url(), "https://rpc.shyft.to?api_key=FROMENV");
        assert!(
            cfg(RpcProvider::Shyft, "", Some("env:SHRED_AUDIT_NOPE_UNSET"))
                .resolve()
                .is_err()
        );
    }
}
