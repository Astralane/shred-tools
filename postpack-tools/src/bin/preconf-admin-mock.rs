use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use agave_transaction_view::sanitize::SanitizeConfig;
use agave_transaction_view::transaction_view::SanitizedTransactionView;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use chrono::{DateTime, NaiveTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use solana_message::v1::{MAX_TRANSACTION_SIZE, V1_PREFIX};
use solana_pubkey::Pubkey;
use tokio::sync::RwLock;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

const DEFAULT_TIP_ACCOUNTS: [&str; 4] = [
    "astgCFnATW3DGN7Pnfj51w3BdVy2TGeMoK5eV7bt4x1",
    "astk6J92kctKFznK4SD3jaKCSTKftSPUV4W2r529HUx",
    "ast5CxenCFBBv99pf8hhJSTG7V3oeCTLqpRrcLYj9Bs",
    "asteY6wTGTTQ8amUUa7xRxf4Nc3kGV9ritjX6yJLFi3",
];
const DEFAULT_SOL_USD_PRICE_CENTS: u64 = 12_000;
const DEFAULT_SOL_PRICE_URL: &str =
    "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd";
const SOL_PRICE_REFRESH_INTERVAL: Duration = Duration::from_secs(5 * 60);
const SOL_PRICE_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_SOL_PRICE_AGE_SECS: i64 = 30 * 60;
const USER_AGENT: &str = concat!("astralane-preconf-admin-mock/", env!("CARGO_PKG_VERSION"));
const PAYMENT_CHECK_INTERVAL: Duration = Duration::from_secs(5);
const MAX_POSTPACK_PAY_TRANSACTIONS: usize = 1000;
const PACKET_DATA_SIZE: usize = 1232;
const CREDITS_PER_CENT: u64 = 2_000;
const LAMPORTS_PER_SOL: u64 = 1_000_000_000;
const SLOTS_PER_EPOCH: u64 = 432_000;
const MAX_OVERRIDE_SECONDS: i64 = 30 * 24 * 60 * 60;
const MAX_ADMIN_CREDITS_CHANGE: i64 = 1_000_000_000_000;
const MAX_HOURS: u32 = 7 * 24;
const MAX_DAYS: u32 = 90;
const MAX_HISTORY_ROWS: u32 = 500;
const SYSTEM_TRANSFER: [u8; 4] = 2u32.to_le_bytes();
const SANITIZE_CONFIG: SanitizeConfig = SanitizeConfig {
    min_requested_heap_size: 32 * 1024,
    max_requested_heap_size: 256 * 1024,
    max_instructions: 64,
    max_accounts_per_instruction: Some(255),
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
enum PostpackTier {
    #[serde(rename = "off")]
    Off,
    #[serde(rename = "tier-1")]
    Tier1,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, ToSchema,
)]
#[serde(rename_all = "snake_case")]
enum CreditedMessageType {
    BamPreconf,
    RakuraiPostpack,
    GravityPostpack,
}

impl CreditedMessageType {
    const ALL: [Self; 3] = [
        Self::BamPreconf,
        Self::RakuraiPostpack,
        Self::GravityPostpack,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::BamPreconf => "bam_preconf",
            Self::RakuraiPostpack => "rakurai_postpack",
            Self::GravityPostpack => "gravity_postpack",
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, ToSchema,
)]
#[serde(rename_all = "snake_case")]
enum CreditedUnitType {
    Message,
    Slot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
enum PostpackCreditChangeReason {
    Tip,
    ManualPayment,
    Usage,
    AdminChange,
    WriteOff,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
enum PaymentToken {
    Sol,
    Usdc,
    Usdt,
}

impl PaymentToken {
    const fn decimals(self) -> u32 {
        match self {
            Self::Sol => 9,
            Self::Usdc | Self::Usdt => 6,
        }
    }
}

fn credits_for_payment(
    amount: u64,
    token: PaymentToken,
    token_usd_price_cents: u64,
) -> Result<u64, String> {
    let credits = u128::from(amount)
        .checked_mul(u128::from(token_usd_price_cents))
        .and_then(|value| value.checked_mul(u128::from(CREDITS_PER_CENT)))
        .ok_or_else(|| format!("{amount} {token:?} at {token_usd_price_cents} cents overflows"))?
        / 10u128.pow(token.decimals());
    u64::try_from(credits).map_err(|_| {
        format!("{amount} {token:?} at {token_usd_price_cents} cents buys more credits than fit")
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
struct TierOverrideView {
    tier: PostpackTier,
    #[schema(example = 86_400)]
    time_left: i64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
struct PostpackClient {
    api_key_value: String,
    tier: PostpackTier,
    credits_balance: i64,
    created_at: DateTime<Utc>,
    comment: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tier_override: Option<TierOverrideView>,
    credits_enabled: Option<bool>,
}

#[derive(Debug, Clone, Copy, Deserialize, ToSchema)]
struct TierOverrideInput {
    tier: PostpackTier,
    #[schema(example = 86_400)]
    duration_seconds: i64,
}

impl TierOverrideInput {
    fn validate(&self) -> Result<(), &'static str> {
        if self.tier == PostpackTier::Off {
            return Err("override tier must not be off");
        }
        if !(0..=MAX_OVERRIDE_SECONDS).contains(&self.duration_seconds) {
            return Err("override duration_seconds must be between 0 and 30 days");
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, ToSchema)]
struct SetPostpackTierRequest {
    api_key_value: String,
    tier: PostpackTier,
    #[serde(default)]
    tier_override: Option<TierOverrideInput>,
    #[serde(default)]
    comment: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
struct SetCreditsEnabledRequest {
    credits_enabled: Option<bool>,
}

#[derive(Debug, Clone, Copy, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
struct CreditPayment {
    paid_amount: i64,
    paid_token: PaymentToken,
    token_usd_price_cents: i64,
}

impl CreditPayment {
    fn credits(&self) -> Result<i64, &'static str> {
        let paid_amount =
            u64::try_from(self.paid_amount).map_err(|_| "paid_amount must be positive")?;
        let price = u64::try_from(self.token_usd_price_cents)
            .map_err(|_| "token_usd_price_cents must be positive")?;
        if paid_amount == 0 {
            return Err("paid_amount must be positive");
        }
        if price == 0 {
            return Err("token_usd_price_cents must be positive");
        }
        credits_for_payment(paid_amount, self.paid_token, price)
            .ok()
            .and_then(|credits| i64::try_from(credits).ok())
            .ok_or("the payment buys more credits than a balance holds")
    }
}

#[derive(Debug, Deserialize, ToSchema)]
struct PostpackCreditChange {
    #[serde(default)]
    delta_credits: Option<i64>,
    #[serde(default = "admin_change")]
    reason: PostpackCreditChangeReason,
    comment: String,
    #[serde(default)]
    payment: Option<CreditPayment>,
}

fn admin_change() -> PostpackCreditChangeReason {
    PostpackCreditChangeReason::AdminChange
}

impl PostpackCreditChange {
    fn delta(&self) -> Result<i64, &'static str> {
        let delta = match self.reason {
            PostpackCreditChangeReason::AdminChange => {
                if self.payment.is_some() {
                    return Err("only a manual_payment records a payment");
                }
                self.delta_credits
                    .ok_or("an admin_change must say delta_credits")?
            }
            PostpackCreditChangeReason::ManualPayment => {
                let payment = self
                    .payment
                    .ok_or("a manual_payment must say what was paid")?;
                let credits = payment.credits()?;
                if self.delta_credits.is_some_and(|delta| delta != credits) {
                    return Err("delta_credits does not match what the payment buys");
                }
                credits
            }
            PostpackCreditChangeReason::Tip
            | PostpackCreditChangeReason::Usage
            | PostpackCreditChangeReason::WriteOff => {
                return Err("only admin_change and manual_payment can be added by an admin");
            }
        };
        if delta == 0 {
            return Err("the change must move the balance");
        }
        if delta.unsigned_abs() > MAX_ADMIN_CREDITS_CHANGE.unsigned_abs() {
            return Err("the change is larger than an admin may make at once");
        }
        if self.comment.trim().is_empty() {
            return Err("comment must say why the balance changes");
        }
        Ok(delta)
    }
}

type UnitPrices = BTreeMap<CreditedUnitType, u64>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
struct PostpackSettings {
    credits_enabled: bool,
    usage_charge_interval_secs: u64,
    #[schema(example = json!({"bam_preconf": {"message": 10}, "rakurai_postpack": {"message": 10}, "gravity_postpack": {"message": 10}}))]
    credits_per_unit: BTreeMap<CreditedMessageType, UnitPrices>,
}

impl Default for PostpackSettings {
    fn default() -> Self {
        Self {
            credits_enabled: false,
            usage_charge_interval_secs: 5,
            credits_per_unit: CreditedMessageType::ALL
                .into_iter()
                .map(|message_type| {
                    (
                        message_type,
                        BTreeMap::from([(CreditedUnitType::Message, 10)]),
                    )
                })
                .collect(),
        }
    }
}

impl PostpackSettings {
    fn credits_per_unit(
        &self,
        message_type: CreditedMessageType,
        unit: CreditedUnitType,
    ) -> Option<u64> {
        self.credits_per_unit
            .get(&message_type)
            .and_then(|prices| prices.get(&unit))
            .copied()
    }

    fn validate(&self) -> Result<(), String> {
        for message_type in CreditedMessageType::ALL {
            if self
                .credits_per_unit(message_type, CreditedUnitType::Message)
                .is_none()
            {
                return Err(format!(
                    "{} needs a price per message",
                    message_type.label()
                ));
            }
        }
        for (message_type, prices) in &self.credits_per_unit {
            for (unit, credits) in prices {
                if *credits == 0 || i64::try_from(*credits).is_err() {
                    return Err(format!(
                        "credits_per_unit for {} per {unit:?} must be positive",
                        message_type.label()
                    ));
                }
            }
        }
        if self.usage_charge_interval_secs == 0 {
            return Err("usage_charge_interval_secs must be positive".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Default, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
struct PostpackSettingsUpdate {
    credits_enabled: Option<bool>,
    usage_charge_interval_secs: Option<u64>,
    #[serde(default)]
    #[schema(example = json!({"bam_preconf": {"message": 7}}))]
    credits_per_unit: BTreeMap<CreditedMessageType, UnitPrices>,
}

impl PostpackSettingsUpdate {
    fn apply(&self, current: &PostpackSettings) -> PostpackSettings {
        let mut credits_per_unit = current.credits_per_unit.clone();
        for (message_type, prices) in &self.credits_per_unit {
            credits_per_unit
                .entry(*message_type)
                .or_default()
                .extend(prices);
        }
        PostpackSettings {
            credits_enabled: self.credits_enabled.unwrap_or(current.credits_enabled),
            usage_charge_interval_secs: self
                .usage_charge_interval_secs
                .unwrap_or(current.usage_charge_interval_secs),
            credits_per_unit,
        }
    }
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct CreditsPriceParams {
    #[param(example = 1_500_000_000)]
    lamports: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
struct CreditsQuote {
    #[schema(example = 1_500_000_000)]
    lamports: u64,
    #[schema(example = 18_000)]
    usd_cents: u64,
    #[schema(example = 36_000_000)]
    credits: u64,
    #[schema(example = 12_000)]
    sol_usd_price_cents: u64,
    #[schema(example = 200_000)]
    credits_per_usd: u64,
    updated_at: DateTime<Utc>,
}

fn quote_lamports(lamports: u64, price: SolPrice) -> Result<CreditsQuote, String> {
    if lamports == 0 {
        return Err("lamports must be positive".to_string());
    }
    let usd_cents = u128::from(lamports) * u128::from(price.cents) / u128::from(LAMPORTS_PER_SOL);
    Ok(CreditsQuote {
        lamports,
        usd_cents: u64::try_from(usd_cents).map_err(|error| error.to_string())?,
        credits: credits_for_payment(lamports, PaymentToken::Sol, price.cents)?,
        sol_usd_price_cents: price.cents,
        credits_per_usd: CREDITS_PER_CENT * 100,
        updated_at: price.updated_at,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
struct UsageSummary {
    credits_balance: i64,
    credits_used_24h: i64,
    messages_24h: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
struct HourlyUsage {
    hour: DateTime<Utc>,
    credits: i64,
    messages: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
struct DailyInstanceUsage {
    #[schema(example = "2026-10-08")]
    date: String,
    #[schema(example = "postpack-fra")]
    instance: String,
    messages: u64,
    credits: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
struct DailyUsage {
    instances: Vec<String>,
    rows: Vec<DailyInstanceUsage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
struct CreditHistoryEntry {
    id: i64,
    delta_credits: i64,
    reason: PostpackCreditChangeReason,
    signature: Option<String>,
    paid_amount: Option<i64>,
    paid_token: Option<PaymentToken>,
    token_usd_price_cents: Option<i64>,
    comment: Option<String>,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, ToSchema)]
struct PostpackValidators {
    #[schema(example = 870)]
    epoch: Option<i64>,
    total_epoch_stake_sol: f64,
    #[schema(example = 432_000)]
    total_epoch_slots: u64,
    validators: Vec<PostpackValidator>,
}

#[derive(Debug, Clone, PartialEq, Serialize, ToSchema)]
struct PostpackValidator {
    identity: String,
    name: Option<String>,
    #[schema(example = "bam")]
    provider: String,
    #[schema(example = "10.0.0.1")]
    source_ip: String,
    #[schema(example = "agave_bam")]
    client: Option<String>,
    #[schema(example = "frankfurt")]
    region: Option<String>,
    activated_stake_sol: f64,
    epoch_leader_slots: i32,
    #[schema(example = 0.29)]
    coverage: f64,
    expected_24h_slots: i32,
    received_24h_slots: u64,
    messages_24h: u64,
    last_seen_at: Option<DateTime<Utc>>,
    by_relay: Vec<RelayActivity>,
}

#[derive(Debug, Clone, PartialEq, Serialize, ToSchema)]
struct RelayActivity {
    #[schema(example = "fra")]
    relay: String,
    received_24h_slots: u64,
    messages_24h: u64,
    last_seen_at: Option<DateTime<Utc>>,
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
struct ConnectedStake {
    epoch: Option<i64>,
    total_epoch_stake_sol: f64,
    connected_validators: usize,
    connected_stake_sol: f64,
    #[schema(example = 0.1)]
    connected_stake_share: f64,
    by_provider: Vec<ProviderStake>,
    by_region: Vec<RegionStake>,
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
struct ProviderStake {
    provider: String,
    validators: usize,
    stake_sol: f64,
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
struct RegionStake {
    region: Option<String>,
    validators: usize,
    stake_sol: f64,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct HourlyUsageParams {
    #[serde(default = "default_hours")]
    #[param(default = 24, maximum = 168)]
    hours: u32,
}

fn default_hours() -> u32 {
    24
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct DailyUsageParams {
    #[serde(default = "default_days")]
    #[param(default = 30, maximum = 90)]
    days: u32,
}

fn default_days() -> u32 {
    30
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct CreditHistoryParams {
    #[serde(default)]
    before_id: Option<i64>,
    #[serde(default = "default_history_limit")]
    #[param(default = 50, maximum = 500)]
    limit: u32,
}

fn default_history_limit() -> u32 {
    50
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct PostpackPayParams {
    #[serde(rename = "api-key")]
    #[param(rename = "api-key")]
    api_key: String,
}

#[derive(Debug, Deserialize, ToSchema)]
struct PostpackPayEntry {
    #[schema(example = "AV1y...base64 signed transaction")]
    transaction: String,
    #[schema(example = 372_000_000)]
    slot: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
struct PostpackPayError {
    index: usize,
    signature: String,
    error: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct PostpackPayResponse {
    accepted: Vec<String>,
    errors: Vec<PostpackPayError>,
}

#[derive(Debug, Serialize, ToSchema)]
struct ErrorBody {
    error: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SolPrice {
    cents: u64,
    updated_at: DateTime<Utc>,
}

impl SolPrice {
    fn is_fresh(&self, now: DateTime<Utc>) -> bool {
        now - self.updated_at <= TimeDelta::seconds(MAX_SOL_PRICE_AGE_SECS)
    }
}

#[derive(Deserialize)]
struct SimplePrice {
    solana: UsdQuote,
}

#[derive(Deserialize)]
struct UsdQuote {
    usd: f64,
}

fn price_cents(body: &str) -> Result<u64, String> {
    let usd = serde_json::from_str::<SimplePrice>(body)
        .map_err(|error| format!("cannot parse the SOL price: {error}"))?
        .solana
        .usd;
    if !(usd.is_finite() && usd > 0.0 && usd < 1_000_000.0) {
        return Err(format!("implausible SOL price {usd}"));
    }
    Ok((usd * 100.0).round() as u64)
}

#[derive(Debug, Clone)]
struct ClientRecord {
    api_key_value: String,
    tier: PostpackTier,
    credits_balance: i64,
    created_at: DateTime<Utc>,
    comment: Option<String>,
    override_tier: Option<PostpackTier>,
    override_expires_at: Option<DateTime<Utc>>,
    credits_enabled: Option<bool>,
}

impl ClientRecord {
    fn new(api_key_value: &str, tier: PostpackTier, now: DateTime<Utc>) -> Self {
        Self {
            api_key_value: api_key_value.to_string(),
            tier,
            credits_balance: 0,
            created_at: now,
            comment: None,
            override_tier: None,
            override_expires_at: None,
            credits_enabled: None,
        }
    }

    fn view(&self, now: DateTime<Utc>) -> PostpackClient {
        PostpackClient {
            api_key_value: self.api_key_value.clone(),
            tier: self.tier,
            credits_balance: self.credits_balance,
            created_at: self.created_at,
            comment: self.comment.clone(),
            tier_override: self
                .override_tier
                .zip(
                    self.override_expires_at
                        .filter(|expires_at| *expires_at > now),
                )
                .map(|(tier, expires_at)| TierOverrideView {
                    tier,
                    time_left: ((expires_at - now).num_milliseconds() + 999) / 1000,
                }),
            credits_enabled: self.credits_enabled,
        }
    }
}

#[derive(Debug, Clone)]
struct HistoryRow {
    id: i64,
    api_key_value: String,
    delta_credits: i64,
    reason: PostpackCreditChangeReason,
    messages: Option<i64>,
    instance: Option<String>,
    signature: Option<String>,
    payment: Option<CreditPayment>,
    comment: Option<String>,
    created_at: DateTime<Utc>,
}

impl HistoryRow {
    fn entry(&self) -> CreditHistoryEntry {
        CreditHistoryEntry {
            id: self.id,
            delta_credits: self.delta_credits,
            reason: self.reason,
            signature: self.signature.clone(),
            paid_amount: self.payment.map(|payment| payment.paid_amount),
            paid_token: self.payment.map(|payment| payment.paid_token),
            token_usd_price_cents: self.payment.map(|payment| payment.token_usd_price_cents),
            comment: self.comment.clone(),
            created_at: self.created_at,
        }
    }
}

#[derive(Debug)]
struct Payment {
    signature: String,
    api_key: String,
    tip: u64,
    accounted: bool,
}

#[derive(Default)]
struct Store {
    api_keys: HashSet<String>,
    clients: HashMap<String, ClientRecord>,
    settings: PostpackSettings,
    history: Vec<HistoryRow>,
    paid_signatures: HashSet<String>,
    payments: Vec<Payment>,
}

impl Store {
    fn record(&mut self, mut row: HistoryRow) {
        row.id = self.history.last().map_or(1, |last| last.id + 1);
        self.history.push(row);
    }

    fn usage_rows<'a>(&'a self, key: &'a str) -> impl Iterator<Item = &'a HistoryRow> {
        self.history.iter().filter(move |row| {
            row.api_key_value == key && row.reason == PostpackCreditChangeReason::Usage
        })
    }
}

struct App {
    store: RwLock<Store>,
    validators: PostpackValidators,
    tip_accounts: Vec<Pubkey>,
    sol_price: RwLock<SolPrice>,
}

type Db = Arc<App>;
type ApiError = (StatusCode, String);
type PayError = (StatusCode, Json<ErrorBody>);

fn not_found() -> ApiError {
    (StatusCode::NOT_FOUND, "no such postpack client".to_string())
}

fn pay_error(status: StatusCode, error: impl Into<String>) -> PayError {
    (
        status,
        Json(ErrorBody {
            error: error.into(),
        }),
    )
}

fn start_of_hour(at: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp(at.timestamp() / 3_600 * 3_600, 0).unwrap_or(at)
}

fn seeded(now: DateTime<Utc>) -> Store {
    let mut store = Store {
        api_keys: [
            "demo-key-tier1",
            "demo-key-trial",
            "demo-key-off",
            "demo-key-new",
        ]
        .into_iter()
        .map(str::to_string)
        .chain((5..=10).map(|n| format!("demo-key-{n}")))
        .collect(),
        ..Store::default()
    };
    let paying = ClientRecord {
        credits_balance: 2_000_000,
        comment: Some("seeded paying client".to_string()),
        ..ClientRecord::new("demo-key-tier1", PostpackTier::Tier1, now)
    };
    let trial = ClientRecord {
        comment: Some("seeded trial client".to_string()),
        override_tier: Some(PostpackTier::Tier1),
        override_expires_at: Some(now + TimeDelta::days(7)),
        ..ClientRecord::new("demo-key-trial", PostpackTier::Off, now)
    };
    let off = ClientRecord::new("demo-key-off", PostpackTier::Off, now);
    for client in [paying, trial, off] {
        store.clients.insert(client.api_key_value.clone(), client);
    }

    store.record(HistoryRow {
        id: 0,
        api_key_value: "demo-key-tier1".to_string(),
        delta_credits: 2_000_000,
        reason: PostpackCreditChangeReason::AdminChange,
        messages: None,
        instance: None,
        signature: None,
        payment: None,
        comment: Some("seeded starting balance".to_string()),
        created_at: now - TimeDelta::days(3),
    });
    let this_hour = start_of_hour(now);
    for hours_ago in (0..48i64).rev() {
        for (index, instance) in ["postpack-fra", "postpack-ams"].into_iter().enumerate() {
            let messages = 50 + (hours_ago * 37 + index as i64 * 11) % 150;
            store.record(HistoryRow {
                id: 0,
                api_key_value: "demo-key-tier1".to_string(),
                delta_credits: -messages * 10,
                reason: PostpackCreditChangeReason::Usage,
                messages: Some(messages),
                instance: Some(instance.to_string()),
                signature: None,
                payment: None,
                comment: None,
                created_at: this_hour - TimeDelta::hours(hours_ago),
            });
        }
    }
    store
}

struct SeededValidator {
    name: Option<&'static str>,
    client: Option<&'static str>,
    region: Option<&'static str>,
    stake_sol: f64,
    epoch_leader_slots: i32,
    connections: &'static [(&'static str, &'static str)],
}

fn seeded_validators(now: DateTime<Utc>) -> PostpackValidators {
    let validators = [
        SeededValidator {
            name: Some("Helius"),
            client: Some("agave_bam"),
            region: Some("frankfurt"),
            stake_sol: 14_200_000.0,
            epoch_leader_slots: 14_920,
            connections: &[("bam", "10.0.0.1"), ("rakurai", "10.0.0.2")],
        },
        SeededValidator {
            name: Some("Everstake"),
            client: Some("everstake_rakurai"),
            region: Some("amsterdam"),
            stake_sol: 9_850_000.0,
            epoch_leader_slots: 10_348,
            connections: &[("rakurai", "10.0.0.2"), ("rakurai", "10.0.0.3")],
        },
        SeededValidator {
            name: Some("Gravity Blocks"),
            client: Some("gravity_blocks"),
            region: Some("new_york"),
            stake_sol: 3_100_000.0,
            epoch_leader_slots: 3_256,
            connections: &[("gravity", "10.0.0.4")],
        },
        SeededValidator {
            name: Some("Tokyo Stake"),
            client: Some("fire_bam"),
            region: Some("tokyo"),
            stake_sol: 1_240_000.0,
            epoch_leader_slots: 1_304,
            connections: &[("bam", "10.0.0.1")],
        },
        SeededValidator {
            name: None,
            client: None,
            region: None,
            stake_sol: 0.0,
            epoch_leader_slots: 0,
            connections: &[("bam", "10.0.0.1")],
        },
    ];
    let mut listed = Vec::new();
    for (index, seed) in validators.into_iter().enumerate() {
        let identity = Pubkey::new_from_array([index as u8 + 1; 32]).to_string();
        let expected_24h_slots = seed.epoch_leader_slots / 2;
        for (connection, (provider, source_ip)) in seed.connections.iter().enumerate() {
            let last_seen_at = Some(now - TimeDelta::seconds((index * 7 + connection) as i64));
            let by_relay: Vec<RelayActivity> = ["ams", "fra", "ny"]
                .into_iter()
                .enumerate()
                .map(|(relay, name)| {
                    let received = (expected_24h_slots as u64) * (3 - relay as u64) / 4;
                    RelayActivity {
                        relay: name.to_string(),
                        received_24h_slots: received,
                        messages_24h: received * 64,
                        last_seen_at,
                    }
                })
                .filter(|relay| relay.received_24h_slots > 0)
                .collect();
            let received_24h_slots = by_relay
                .iter()
                .map(|relay| relay.received_24h_slots)
                .max()
                .unwrap_or(0);
            listed.push(PostpackValidator {
                identity: identity.clone(),
                name: seed.name.map(str::to_string),
                provider: provider.to_string(),
                source_ip: source_ip.to_string(),
                client: seed.client.map(str::to_string),
                region: seed.region.map(str::to_string),
                activated_stake_sol: seed.stake_sol,
                epoch_leader_slots: seed.epoch_leader_slots,
                coverage: (f64::from(seed.epoch_leader_slots) * 100.0 / SLOTS_PER_EPOCH as f64
                    * 100.0)
                    .round()
                    / 100.0,
                expected_24h_slots,
                received_24h_slots,
                messages_24h: by_relay.iter().map(|relay| relay.messages_24h).sum(),
                last_seen_at,
                by_relay,
            });
        }
    }
    listed.sort_by(|a, b| {
        b.activated_stake_sol
            .total_cmp(&a.activated_stake_sol)
            .then_with(|| a.identity.cmp(&b.identity))
            .then_with(|| a.provider.cmp(&b.provider))
            .then_with(|| a.source_ip.cmp(&b.source_ip))
    });
    PostpackValidators {
        epoch: Some(870),
        total_epoch_stake_sol: 412_500_000.0,
        total_epoch_slots: SLOTS_PER_EPOCH,
        validators: listed,
    }
}

fn connected_stake(listed: &PostpackValidators) -> ConnectedStake {
    let mut identities: HashMap<&str, &PostpackValidator> = HashMap::new();
    let mut by_provider: BTreeMap<&str, HashMap<&str, f64>> = BTreeMap::new();
    for validator in &listed.validators {
        identities.entry(&validator.identity).or_insert(validator);
        by_provider
            .entry(&validator.provider)
            .or_default()
            .insert(&validator.identity, validator.activated_stake_sol);
    }
    let mut by_region: HashMap<Option<&str>, (usize, f64)> = HashMap::new();
    for validator in identities.values() {
        let region = by_region.entry(validator.region.as_deref()).or_default();
        region.0 += 1;
        region.1 += validator.activated_stake_sol;
    }
    let mut by_region = by_region
        .into_iter()
        .map(|(region, (validators, stake_sol))| RegionStake {
            region: region.map(str::to_owned),
            validators,
            stake_sol,
        })
        .collect::<Vec<_>>();
    by_region.sort_by(|a, b| {
        b.stake_sol
            .total_cmp(&a.stake_sol)
            .then_with(|| a.region.cmp(&b.region))
    });
    let connected_stake_sol: f64 = identities
        .values()
        .map(|validator| validator.activated_stake_sol)
        .sum();
    ConnectedStake {
        epoch: listed.epoch,
        total_epoch_stake_sol: listed.total_epoch_stake_sol,
        connected_validators: identities.len(),
        connected_stake_sol,
        connected_stake_share: if listed.total_epoch_stake_sol > 0.0 {
            connected_stake_sol / listed.total_epoch_stake_sol
        } else {
            0.0
        },
        by_provider: by_provider
            .into_iter()
            .map(|(provider, validators)| ProviderStake {
                provider: provider.to_owned(),
                validators: validators.len(),
                stake_sol: validators.values().sum(),
            })
            .collect(),
        by_region,
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Preconf admin API",
        description = "In-memory mock of the admin-server postpack endpoints and the public-server /postpack-pay endpoint. Known API keys: demo-key-tier1 (paying, with 48 hours of seeded usage), demo-key-trial (tier override), demo-key-off, demo-key-new and demo-key-5 to demo-key-10 (not postpack clients yet). The validators are a fixed seeded list."
    ),
    paths(
        get_all_postpack_clients,
        set_postpack_tier,
        get_postpack_client,
        delete_postpack_client,
        change_postpack_credits,
        set_postpack_credits_enabled,
        get_credits_settings,
        update_credits_settings,
        get_credits_price,
        get_usage_summary,
        get_hourly_usage,
        get_daily_usage,
        get_credit_history,
        get_validators,
        get_connected_stake,
        postpack_pay
    )
)]
struct ApiDoc;

fn router(db: Db) -> Router {
    Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/openapi.json", ApiDoc::openapi()))
        .route("/health", get(|| async { "ok" }))
        .route(
            "/admin/postpack",
            get(get_all_postpack_clients).put(set_postpack_tier),
        )
        .route(
            "/admin/postpack/{key}",
            get(get_postpack_client).delete(delete_postpack_client),
        )
        .route(
            "/admin/postpack/{key}/credits",
            post(change_postpack_credits),
        )
        .route(
            "/admin/postpack/{key}/credits-enabled",
            put(set_postpack_credits_enabled),
        )
        .route(
            "/admin/postpack/credits/settings",
            get(get_credits_settings).put(update_credits_settings),
        )
        .route("/admin/postpack/credits/price", get(get_credits_price))
        .route("/admin/postpack/{key}/usage", get(get_usage_summary))
        .route("/admin/postpack/{key}/usage/hourly", get(get_hourly_usage))
        .route("/admin/postpack/{key}/usage/daily", get(get_daily_usage))
        .route(
            "/admin/postpack/{key}/credits/history",
            get(get_credit_history),
        )
        .route("/admin/postpack/validators", get(get_validators))
        .route("/admin/postpack/validators/stake", get(get_connected_stake))
        .route("/postpack-pay", post(postpack_pay))
        .with_state(db)
}

#[utoipa::path(
    get,
    path = "/admin/postpack/credits/settings",
    tag = "credits settings",
    summary = "Read the post-pack settings",
    responses((status = 200, body = PostpackSettings))
)]
async fn get_credits_settings(State(db): State<Db>) -> Json<PostpackSettings> {
    Json(db.store.read().await.settings.clone())
}

#[utoipa::path(
    put,
    path = "/admin/postpack/credits/settings",
    tag = "credits settings",
    summary = "Change some post-pack settings",
    description = "Only the fields present in the body change; prices are merged per message type and unit. Every message type needs a price per message; a price per slot is optional.",
    request_body = PostpackSettingsUpdate,
    responses(
        (status = 200, body = PostpackSettings),
        (status = 422, description = "The resulting settings are invalid", body = String, example = "credits_per_unit for bam_preconf per Message must be positive")
    )
)]
async fn update_credits_settings(
    State(db): State<Db>,
    Json(body): Json<PostpackSettingsUpdate>,
) -> Result<Json<PostpackSettings>, ApiError> {
    let mut store = db.store.write().await;
    let settings = body.apply(&store.settings);
    settings
        .validate()
        .map_err(|error| (StatusCode::UNPROCESSABLE_ENTITY, error))?;
    store.settings = settings.clone();
    Ok(Json(settings))
}

#[utoipa::path(
    get,
    path = "/admin/postpack/credits/price",
    tag = "credits settings",
    summary = "Convert an amount of lamports to USD and credits",
    description = "The SOL price comes from CoinGecko and refreshes every 5 minutes; a price older than 30 minutes is not used. One USD always buys 200000 credits, so 5 USD buy one million.",
    params(CreditsPriceParams),
    responses(
        (status = 200, body = CreditsQuote),
        (status = 400, body = String, example = "lamports must be positive"),
        (status = 503, body = String, example = "no fresh SOL price, try again later")
    )
)]
async fn get_credits_price(
    State(db): State<Db>,
    Query(params): Query<CreditsPriceParams>,
) -> Result<Json<CreditsQuote>, ApiError> {
    let price = *db.sol_price.read().await;
    if !price.is_fresh(Utc::now()) {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "no fresh SOL price, try again later".to_string(),
        ));
    }
    quote_lamports(params.lamports, price)
        .map(Json)
        .map_err(|error| (StatusCode::BAD_REQUEST, error))
}

#[utoipa::path(
    get,
    path = "/admin/postpack",
    tag = "clients",
    summary = "List all postpack clients, newest first",
    responses((status = 200, body = Vec<PostpackClient>))
)]
async fn get_all_postpack_clients(State(db): State<Db>) -> Json<Vec<PostpackClient>> {
    let now = Utc::now();
    let mut clients: Vec<_> = db
        .store
        .read()
        .await
        .clients
        .values()
        .map(|client| client.view(now))
        .collect();
    clients.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    Json(clients)
}

#[utoipa::path(
    get,
    path = "/admin/postpack/{key}",
    tag = "clients",
    summary = "Get one postpack client",
    params(("key" = String, Path, description = "API key value")),
    responses(
        (status = 200, body = PostpackClient),
        (status = 404, body = String, example = "no such postpack client")
    )
)]
async fn get_postpack_client(
    State(db): State<Db>,
    Path(key): Path<String>,
) -> Result<Json<PostpackClient>, ApiError> {
    db.store
        .read()
        .await
        .clients
        .get(&key)
        .map(|client| Json(client.view(Utc::now())))
        .ok_or_else(not_found)
}

#[utoipa::path(
    put,
    path = "/admin/postpack",
    tag = "clients",
    summary = "Set a client's tier, tier override and comment",
    description = "Creates the client with a zero balance if it does not exist yet. tier is the client's own tier. A tier_override grants its tier for duration_seconds (at most 30 days) without charging credits; 0 ends an override now. An omitted tier_override or comment keeps the current one; a blank comment clears it.",
    request_body = SetPostpackTierRequest,
    responses(
        (status = 200, body = PostpackClient),
        (status = 404, body = String, example = "no such api key"),
        (status = 422, description = "The tier override is invalid", body = String, example = "override tier must not be off")
    )
)]
async fn set_postpack_tier(
    State(db): State<Db>,
    Json(body): Json<SetPostpackTierRequest>,
) -> Result<Json<PostpackClient>, ApiError> {
    let mut store = db.store.write().await;
    if !store.api_keys.contains(&body.api_key_value) {
        return Err((StatusCode::NOT_FOUND, "no such api key".to_string()));
    }
    if let Some(tier_override) = &body.tier_override {
        tier_override
            .validate()
            .map_err(|error| (StatusCode::UNPROCESSABLE_ENTITY, error.to_string()))?;
    }
    let now = Utc::now();
    let client = store
        .clients
        .entry(body.api_key_value.clone())
        .or_insert_with(|| ClientRecord::new(&body.api_key_value, body.tier, now));
    client.tier = body.tier;
    if let Some(tier_override) = body.tier_override {
        client.override_tier = Some(tier_override.tier);
        client.override_expires_at = Some(now + TimeDelta::seconds(tier_override.duration_seconds));
    }
    if let Some(comment) = body.comment.as_deref().map(str::trim) {
        client.comment = (!comment.is_empty()).then(|| comment.to_string());
    }
    Ok(Json(client.view(now)))
}

#[utoipa::path(
    post,
    path = "/admin/postpack/{key}/credits",
    tag = "clients",
    summary = "Add or remove credits",
    description = "reason is admin_change (default; delta_credits required, no payment) or manual_payment (payment required; the credits it buys are computed, and delta_credits, if given, must match). The change must not be zero or larger than 10^12, comment must not be blank, and the balance cannot go below 0. Every change is recorded in the credit history.",
    params(("key" = String, Path, description = "API key value")),
    request_body = PostpackCreditChange,
    responses(
        (status = 200, body = PostpackClient),
        (status = 404, body = String, example = "no such postpack client"),
        (status = 409, body = String, example = "the balance cannot go below 0"),
        (status = 422, description = "The change is invalid", body = String, example = "a manual_payment must say what was paid")
    )
)]
async fn change_postpack_credits(
    State(db): State<Db>,
    Path(key): Path<String>,
    Json(body): Json<PostpackCreditChange>,
) -> Result<Json<PostpackClient>, ApiError> {
    let delta = body
        .delta()
        .map_err(|error| (StatusCode::UNPROCESSABLE_ENTITY, error.to_string()))?;
    let mut store = db.store.write().await;
    let client = store.clients.get_mut(&key).ok_or_else(not_found)?;
    let balance = client.credits_balance.checked_add(delta).ok_or((
        StatusCode::INTERNAL_SERVER_ERROR,
        "cannot change credits balance".to_string(),
    ))?;
    if balance < 0 {
        return Err((
            StatusCode::CONFLICT,
            "the balance cannot go below 0".to_string(),
        ));
    }
    client.credits_balance = balance;
    let now = Utc::now();
    let view = client.view(now);
    store.record(HistoryRow {
        id: 0,
        api_key_value: key,
        delta_credits: delta,
        reason: body.reason,
        messages: None,
        instance: None,
        signature: None,
        payment: body.payment,
        comment: Some(body.comment.trim().to_string()),
        created_at: now,
    });
    Ok(Json(view))
}

#[utoipa::path(
    put,
    path = "/admin/postpack/{key}/credits-enabled",
    tag = "clients",
    summary = "Turn credits on or off for one client",
    description = "true always charges the client, false never does, null follows credits_enabled in the settings.",
    params(("key" = String, Path, description = "API key value")),
    request_body = SetCreditsEnabledRequest,
    responses(
        (status = 200, body = PostpackClient),
        (status = 404, body = String, example = "no such postpack client")
    )
)]
async fn set_postpack_credits_enabled(
    State(db): State<Db>,
    Path(key): Path<String>,
    Json(body): Json<SetCreditsEnabledRequest>,
) -> Result<Json<PostpackClient>, ApiError> {
    let mut store = db.store.write().await;
    let client = store.clients.get_mut(&key).ok_or_else(not_found)?;
    client.credits_enabled = body.credits_enabled;
    Ok(Json(client.view(Utc::now())))
}

#[utoipa::path(
    delete,
    path = "/admin/postpack/{key}",
    tag = "clients",
    summary = "Switch a postpack client off",
    description = "Sets the tier to off and ends any tier override. The client, its balance and its credit history are kept.",
    params(("key" = String, Path, description = "API key value")),
    responses(
        (status = 200, body = String, example = "success"),
        (status = 404, body = String, example = "no such postpack client")
    )
)]
async fn delete_postpack_client(
    State(db): State<Db>,
    Path(key): Path<String>,
) -> Result<&'static str, ApiError> {
    let mut store = db.store.write().await;
    let client = store.clients.get_mut(&key).ok_or_else(not_found)?;
    client.tier = PostpackTier::Off;
    client.override_tier = None;
    client.override_expires_at = None;
    Ok("success")
}

#[utoipa::path(
    get,
    path = "/admin/postpack/{key}/usage",
    tag = "usage",
    summary = "Balance and usage over the last 24 hours",
    params(("key" = String, Path, description = "API key value")),
    responses(
        (status = 200, body = UsageSummary),
        (status = 404, body = String, example = "no such postpack client")
    )
)]
async fn get_usage_summary(
    State(db): State<Db>,
    Path(key): Path<String>,
) -> Result<Json<UsageSummary>, ApiError> {
    let store = db.store.read().await;
    let client = store.clients.get(&key).ok_or_else(not_found)?;
    let since = start_of_hour(Utc::now()) - TimeDelta::hours(23);
    let (credits, messages) = store
        .usage_rows(&key)
        .filter(|row| row.created_at >= since)
        .fold((0, 0), |(credits, messages), row| {
            (
                credits - row.delta_credits,
                messages + row.messages.unwrap_or(0),
            )
        });
    Ok(Json(UsageSummary {
        credits_balance: client.credits_balance,
        credits_used_24h: credits,
        messages_24h: messages,
    }))
}

#[utoipa::path(
    get,
    path = "/admin/postpack/{key}/usage/hourly",
    tag = "usage",
    summary = "Credits and messages charged per hour",
    description = "One row per hour, oldest first, ending with the current hour; hours without usage are zero. hours is clamped to 1..=168.",
    params(("key" = String, Path, description = "API key value"), HourlyUsageParams),
    responses((status = 200, body = Vec<HourlyUsage>))
)]
async fn get_hourly_usage(
    State(db): State<Db>,
    Path(key): Path<String>,
    Query(params): Query<HourlyUsageParams>,
) -> Json<Vec<HourlyUsage>> {
    let store = db.store.read().await;
    let hours = i64::from(params.hours.clamp(1, MAX_HOURS));
    let this_hour = start_of_hour(Utc::now());
    let mut usage: BTreeMap<DateTime<Utc>, (i64, i64)> = (0..hours)
        .map(|hours_ago| (this_hour - TimeDelta::hours(hours_ago), (0, 0)))
        .collect();
    for row in store.usage_rows(&key) {
        if let Some((credits, messages)) = usage.get_mut(&start_of_hour(row.created_at)) {
            *credits -= row.delta_credits;
            *messages += row.messages.unwrap_or(0);
        }
    }
    Json(
        usage
            .into_iter()
            .map(|(hour, (credits, messages))| HourlyUsage {
                hour,
                credits,
                messages,
            })
            .collect(),
    )
}

#[utoipa::path(
    get,
    path = "/admin/postpack/{key}/usage/daily",
    tag = "usage",
    summary = "Messages sent and credits charged per day and relay instance",
    description = "Read from ClickHouse in production (503 when it is not configured). Rows are ordered by date, then instance; days is clamped to 1..=90 and counts today.",
    params(("key" = String, Path, description = "API key value"), DailyUsageParams),
    responses(
        (status = 200, body = DailyUsage),
        (status = 404, body = String, example = "no such postpack client"),
        (status = 503, body = String, example = "daily usage is not configured")
    )
)]
async fn get_daily_usage(
    State(db): State<Db>,
    Path(key): Path<String>,
    Query(params): Query<DailyUsageParams>,
) -> Result<Json<DailyUsage>, ApiError> {
    let store = db.store.read().await;
    if !store.clients.contains_key(&key) {
        return Err(not_found());
    }
    let days = i64::from(params.days.clamp(1, MAX_DAYS));
    let since = (Utc::now().date_naive() - TimeDelta::days(days - 1))
        .and_time(NaiveTime::MIN)
        .and_utc();
    let mut daily: BTreeMap<(String, String), (u64, u64)> = BTreeMap::new();
    for row in store.usage_rows(&key).filter(|row| row.created_at >= since) {
        let date = row.created_at.date_naive().to_string();
        let instance = row.instance.clone().unwrap_or_default();
        let (messages, credits) = daily.entry((date, instance)).or_default();
        *messages += row.messages.unwrap_or(0).unsigned_abs();
        *credits += row.delta_credits.unsigned_abs();
    }
    let rows: Vec<DailyInstanceUsage> = daily
        .into_iter()
        .map(
            |((date, instance), (messages, credits))| DailyInstanceUsage {
                date,
                instance,
                messages,
                credits,
            },
        )
        .collect();
    let mut instances: Vec<String> = rows.iter().map(|row| row.instance.clone()).collect();
    instances.sort();
    instances.dedup();
    Ok(Json(DailyUsage { instances, rows }))
}

#[utoipa::path(
    get,
    path = "/admin/postpack/{key}/credits/history",
    tag = "usage",
    summary = "Balance changes other than usage, newest first",
    description = "Tips, manual payments, admin changes and write-offs. Pass the last id as before_id for the next page; limit is clamped to 1..=500.",
    params(("key" = String, Path, description = "API key value"), CreditHistoryParams),
    responses((status = 200, body = Vec<CreditHistoryEntry>))
)]
async fn get_credit_history(
    State(db): State<Db>,
    Path(key): Path<String>,
    Query(params): Query<CreditHistoryParams>,
) -> Json<Vec<CreditHistoryEntry>> {
    let store = db.store.read().await;
    Json(
        store
            .history
            .iter()
            .rev()
            .filter(|row| {
                row.api_key_value == key
                    && row.reason != PostpackCreditChangeReason::Usage
                    && params.before_id.is_none_or(|before_id| row.id < before_id)
            })
            .take(params.limit.clamp(1, MAX_HISTORY_ROWS) as usize)
            .map(HistoryRow::entry)
            .collect(),
    )
}

#[utoipa::path(
    get,
    path = "/admin/postpack/validators",
    tag = "validators",
    summary = "Validators sending post-pack messages over the last 24 hours",
    description = "One row per validator and provider source IP, by stake. Stake, leader slots, coverage and expected slots come from the leader schedule and are 0 for a validator outside it.",
    responses((status = 200, body = PostpackValidators))
)]
async fn get_validators(State(db): State<Db>) -> Json<PostpackValidators> {
    Json(db.validators.clone())
}

#[utoipa::path(
    get,
    path = "/admin/postpack/validators/stake",
    tag = "validators",
    summary = "Connected stake overall, per provider and per region",
    description = "Each validator counts once overall, once per provider and once per region.",
    responses((status = 200, body = ConnectedStake))
)]
async fn get_connected_stake(State(db): State<Db>) -> Json<ConnectedStake> {
    Json(connected_stake(&db.validators))
}

#[utoipa::path(
    post,
    path = "/postpack-pay",
    tag = "public server",
    summary = "Register signed tip transactions that pay for postpack credits",
    description = "Served by the public server in production. Each transaction must be base64, carry a positive slot, verify its signatures and tip an Astralane tip account. Rejected entries are listed in errors while the rest are accepted. A signature that was already registered is neither accepted nor an error. The mock treats every accepted transaction as landed and, once the key is a postpack client, credits its tip within 5 seconds at the current SOL price, whether or not credits are enabled. Tips wait while the SOL price is older than 30 minutes.",
    params(PostpackPayParams),
    request_body = Vec<PostpackPayEntry>,
    responses(
        (status = 200, body = PostpackPayResponse),
        (status = 400, body = ErrorBody, example = json!({"error": "no transactions provided"})),
        (status = 401, body = ErrorBody, example = json!({"error": "unknown api key"}))
    )
)]
async fn postpack_pay(
    State(db): State<Db>,
    Query(params): Query<PostpackPayParams>,
    Json(requested): Json<Vec<PostpackPayEntry>>,
) -> Result<Json<PostpackPayResponse>, PayError> {
    let mut store = db.store.write().await;
    register_payments(&mut store, &params.api_key, requested, &db.tip_accounts).map(Json)
}

fn register_payments(
    store: &mut Store,
    api_key: &str,
    requested: Vec<PostpackPayEntry>,
    tip_accounts: &[Pubkey],
) -> Result<PostpackPayResponse, PayError> {
    if requested.is_empty() {
        return Err(pay_error(
            StatusCode::BAD_REQUEST,
            "no transactions provided",
        ));
    }
    if requested.len() > MAX_POSTPACK_PAY_TRANSACTIONS {
        return Err(pay_error(
            StatusCode::BAD_REQUEST,
            format!(
                "cannot exceed {} transactions, got {}",
                MAX_POSTPACK_PAY_TRANSACTIONS,
                requested.len()
            ),
        ));
    }

    let mut errors = Vec::new();
    let mut entries: Vec<(String, u64)> = Vec::new();
    let mut seen = HashSet::new();
    for (index, entry) in requested.into_iter().enumerate() {
        let error = |signature: String, error: &str| PostpackPayError {
            index,
            signature,
            error: error.to_string(),
        };
        if !entry.slot.is_some_and(|slot| slot > 0) {
            errors.push(error(String::new(), "missing or invalid shred slot"));
            continue;
        }
        let Some(wire) = decode_transaction(&entry.transaction) else {
            errors.push(error(String::new(), "failed to decode base64 transaction"));
            continue;
        };
        match parse_tip(&wire, tip_accounts) {
            Ok(tip) if tip.lamports == 0 => errors.push(error(
                tip.signature,
                "transaction does not tip an Astralane tip account",
            )),
            Ok(tip) => {
                if seen.insert(tip.signature.clone()) {
                    entries.push((tip.signature, tip.lamports));
                }
            }
            Err(reason) => errors.push(error(String::new(), reason)),
        }
    }

    if !store.api_keys.contains(api_key) {
        return Err(pay_error(StatusCode::UNAUTHORIZED, "unknown api key"));
    }
    let mut accepted = Vec::new();
    for (signature, tip) in entries {
        if store.paid_signatures.insert(signature.clone()) {
            store.payments.push(Payment {
                signature: signature.clone(),
                api_key: api_key.to_string(),
                tip,
                accounted: false,
            });
            accepted.push(signature);
        }
    }
    Ok(PostpackPayResponse { accepted, errors })
}

fn decode_transaction(encoded: &str) -> Option<Vec<u8>> {
    if encoded.len() > 4 * MAX_TRANSACTION_SIZE.div_ceil(3) {
        return None;
    }
    let wire = BASE64_STANDARD.decode(encoded).ok()?;
    let max_size = if wire.first() == Some(&V1_PREFIX) {
        MAX_TRANSACTION_SIZE
    } else {
        PACKET_DATA_SIZE
    };
    (!wire.is_empty() && wire.len() <= max_size).then_some(wire)
}

struct ParsedTip {
    signature: String,
    lamports: u64,
}

fn parse_tip(wire: &[u8], tip_accounts: &[Pubkey]) -> Result<ParsedTip, &'static str> {
    let view = SanitizedTransactionView::try_new_sanitized(wire, &SANITIZE_CONFIG)
        .map_err(|_| "failed to parse transaction")?;
    let message = view.message_data();
    if view
        .signatures()
        .iter()
        .zip(view.static_account_keys())
        .any(|(signature, pubkey)| !signature.verify(pubkey.as_ref(), message))
    {
        return Err("signature verification failed");
    }
    let keys = view.static_account_keys();
    let mut parsed = ParsedTip {
        signature: view.signatures()[0].to_string(),
        lamports: 0,
    };
    for instruction in view.instructions_iter() {
        let Some((to, lamports)) = transfer(instruction.data, instruction.accounts, keys) else {
            continue;
        };
        if tip_accounts.contains(&to) {
            parsed.lamports = parsed.lamports.saturating_add(lamports);
        }
    }
    Ok(parsed)
}

fn transfer(data: &[u8], accounts: &[u8], keys: &[Pubkey]) -> Option<(Pubkey, u64)> {
    if data.get(..4)? != SYSTEM_TRANSFER {
        return None;
    }
    let lamports = u64::from_le_bytes(data.get(4..12)?.try_into().ok()?);
    keys.get(*accounts.first()? as usize)?;
    let to = keys.get(*accounts.get(1)? as usize)?;
    Some((*to, lamports))
}

fn credit_payments(store: &mut Store, price: SolPrice, now: DateTime<Utc>) -> usize {
    if !price.is_fresh(now) {
        return 0;
    }
    let mut credited = Vec::new();
    for payment in store
        .payments
        .iter_mut()
        .filter(|payment| !payment.accounted)
    {
        let Some(client) = store.clients.get_mut(&payment.api_key) else {
            continue;
        };
        payment.accounted = true;
        let Some(credits) = credits_for_payment(payment.tip, PaymentToken::Sol, price.cents)
            .ok()
            .and_then(|credits| i64::try_from(credits).ok())
            .filter(|credits| *credits > 0)
        else {
            continue;
        };
        client.credits_balance = client.credits_balance.saturating_add(credits);
        credited.push(HistoryRow {
            id: 0,
            api_key_value: payment.api_key.clone(),
            delta_credits: credits,
            reason: PostpackCreditChangeReason::Tip,
            messages: None,
            instance: None,
            signature: Some(payment.signature.clone()),
            payment: Some(CreditPayment {
                paid_amount: i64::try_from(payment.tip).unwrap_or(i64::MAX),
                paid_token: PaymentToken::Sol,
                token_usd_price_cents: i64::try_from(price.cents).unwrap_or(i64::MAX),
            }),
            comment: None,
            created_at: now,
        });
    }
    let count = credited.len();
    for row in credited {
        store.record(row);
    }
    count
}

fn spawn_payment_checks(db: Db) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(PAYMENT_CHECK_INTERVAL);
        loop {
            tick.tick().await;
            let price = *db.sol_price.read().await;
            let credited = credit_payments(&mut *db.store.write().await, price, Utc::now());
            if credited > 0 {
                println!("credited {credited} postpack payments");
            }
        }
    });
}

fn spawn_sol_price_refresh(db: Db, url: String) {
    let client = reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(SOL_PRICE_REQUEST_TIMEOUT)
        .build()
        .expect("cannot build the SOL price client");
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(SOL_PRICE_REFRESH_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            match fetch_price_cents(&client, &url).await {
                Ok(cents) => {
                    *db.sol_price.write().await = SolPrice {
                        cents,
                        updated_at: Utc::now(),
                    };
                    println!("SOL price {cents} cents");
                }
                Err(error) => {
                    eprintln!("cannot fetch the SOL price, keeping the last one: {error}")
                }
            }
        }
    });
}

async fn fetch_price_cents(client: &reqwest::Client, url: &str) -> Result<u64, String> {
    let body = client
        .get(url)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|error| error.to_string())?
        .text()
        .await
        .map_err(|error| error.to_string())?;
    price_cents(&body)
}

#[tokio::main]
async fn main() {
    let address: SocketAddr = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("PRECONF_ADMIN_MOCK_ADDR").ok())
        .unwrap_or_else(|| "0.0.0.0:28090".to_string())
        .parse()
        .expect("a socket address like 0.0.0.0:28090");
    let tip_accounts: Vec<Pubkey> = std::env::var("PRECONF_ADMIN_MOCK_TIP_ACCOUNTS")
        .unwrap_or_else(|_| DEFAULT_TIP_ACCOUNTS.join(","))
        .split(',')
        .map(|account| Pubkey::from_str(account.trim()).expect("tip accounts are base58 pubkeys"))
        .collect();
    let sol_usd_price_cents = std::env::var("PRECONF_ADMIN_MOCK_SOL_USD_PRICE_CENTS")
        .map(|cents| {
            cents
                .parse()
                .expect("the SOL price is a whole number of cents")
        })
        .unwrap_or(DEFAULT_SOL_USD_PRICE_CENTS);
    let sol_price_url = std::env::var("PRECONF_ADMIN_MOCK_SOL_PRICE_URL")
        .unwrap_or_else(|_| DEFAULT_SOL_PRICE_URL.to_string());
    let now = Utc::now();
    let db = Arc::new(App {
        store: RwLock::new(seeded(now)),
        validators: seeded_validators(now),
        tip_accounts,
        sol_price: RwLock::new(SolPrice {
            cents: sol_usd_price_cents,
            updated_at: now,
        }),
    });
    spawn_sol_price_refresh(db.clone(), sol_price_url);
    spawn_payment_checks(db.clone());
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("cannot bind the listen address");
    println!("preconf admin mock listening on {address}, docs at http://{address}/swagger-ui");
    println!(
        "tip accounts {:?}, starting SOL price {sol_usd_price_cents} cents",
        db.tip_accounts
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    );
    axum::serve(listener, router(db))
        .await
        .expect("server failed");
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_hash::Hash;
    use solana_keypair::Keypair;
    use solana_signer::Signer;
    use solana_system_interface::instruction::transfer as system_transfer;
    use solana_transaction::Transaction;

    const PRICE_CENTS: u64 = 15_000;

    fn tip_account() -> Pubkey {
        Pubkey::from_str(DEFAULT_TIP_ACCOUNTS[0]).unwrap()
    }

    fn fresh_price() -> SolPrice {
        SolPrice {
            cents: PRICE_CENTS,
            updated_at: Utc::now(),
        }
    }

    fn signed_transfer(to: Pubkey, lamports: u64) -> Vec<u8> {
        let payer = Keypair::new();
        let transaction = Transaction::new_signed_with_payer(
            &[system_transfer(&payer.pubkey(), &to, lamports)],
            Some(&payer.pubkey()),
            &[&payer],
            Hash::new_from_array([7; 32]),
        );
        wincode::serialize(&transaction).unwrap()
    }

    fn entry(wire: &[u8], slot: Option<i64>) -> PostpackPayEntry {
        PostpackPayEntry {
            transaction: BASE64_STANDARD.encode(wire),
            slot,
        }
    }

    fn app_priced(price: SolPrice) -> Db {
        let now = Utc::now();
        Arc::new(App {
            store: RwLock::new(seeded(now)),
            validators: seeded_validators(now),
            tip_accounts: vec![tip_account()],
            sol_price: RwLock::new(price),
        })
    }

    fn app() -> Db {
        app_priced(fresh_price())
    }

    fn balance(store: &Store, key: &str) -> i64 {
        store.clients[key].credits_balance
    }

    #[test]
    fn a_tip_is_registered_once_and_credited_even_with_credits_off() {
        let mut store = seeded(Utc::now());
        assert!(!store.settings.credits_enabled);
        let tip = signed_transfer(tip_account(), 10_000_000);

        let paid = register_payments(
            &mut store,
            "demo-key-tier1",
            vec![entry(&tip, Some(1)), entry(&tip, Some(1))],
            &[tip_account()],
        )
        .unwrap();
        assert_eq!(paid.accepted.len(), 1);
        assert!(paid.errors.is_empty());

        let again = register_payments(
            &mut store,
            "demo-key-tier1",
            vec![entry(&tip, Some(2))],
            &[tip_account()],
        )
        .unwrap();
        assert!(again.accepted.is_empty() && again.errors.is_empty());

        assert_eq!(credit_payments(&mut store, fresh_price(), Utc::now()), 1);
        assert_eq!(balance(&store, "demo-key-tier1"), 2_000_000 + 300_000);
        assert_eq!(credit_payments(&mut store, fresh_price(), Utc::now()), 0);

        let tip_row = store.history.last().unwrap().entry();
        assert_eq!(tip_row.reason, PostpackCreditChangeReason::Tip);
        assert_eq!(
            tip_row.signature.as_deref(),
            Some(paid.accepted[0].as_str())
        );
        assert_eq!(
            (
                tip_row.paid_amount,
                tip_row.paid_token,
                tip_row.token_usd_price_cents
            ),
            (Some(10_000_000), Some(PaymentToken::Sol), Some(15_000))
        );
    }

    #[test]
    fn tips_wait_for_a_fresh_sol_price() {
        let mut store = seeded(Utc::now());
        let tip = signed_transfer(tip_account(), 10_000_000);
        register_payments(
            &mut store,
            "demo-key-tier1",
            vec![entry(&tip, Some(1))],
            &[tip_account()],
        )
        .unwrap();
        let stale = SolPrice {
            cents: PRICE_CENTS,
            updated_at: Utc::now() - TimeDelta::hours(1),
        };
        assert_eq!(credit_payments(&mut store, stale, Utc::now()), 0);
        assert_eq!(balance(&store, "demo-key-tier1"), 2_000_000);
        assert_eq!(credit_payments(&mut store, fresh_price(), Utc::now()), 1);
    }

    #[test]
    fn a_tip_from_a_key_that_is_not_a_postpack_client_waits_until_it_becomes_one() {
        let mut store = seeded(Utc::now());
        let tip = signed_transfer(tip_account(), LAMPORTS_PER_SOL);
        register_payments(
            &mut store,
            "demo-key-new",
            vec![entry(&tip, Some(1))],
            &[tip_account()],
        )
        .unwrap();
        assert_eq!(credit_payments(&mut store, fresh_price(), Utc::now()), 0);

        store.clients.insert(
            "demo-key-new".to_string(),
            ClientRecord::new("demo-key-new", PostpackTier::Tier1, Utc::now()),
        );
        assert_eq!(credit_payments(&mut store, fresh_price(), Utc::now()), 1);
        assert_eq!(balance(&store, "demo-key-new"), 30_000_000);
    }

    #[test]
    fn bad_entries_are_listed_as_errors_like_the_public_server_does() {
        let mut store = seeded(Utc::now());
        let mut forged = signed_transfer(tip_account(), 1_000);
        let last = forged.len() - 1;
        forged[last] ^= 1;
        let untipped = signed_transfer(Pubkey::new_unique(), 1_000);

        let paid = register_payments(
            &mut store,
            "demo-key-tier1",
            vec![
                entry(&signed_transfer(tip_account(), 1_000), None),
                PostpackPayEntry {
                    transaction: "not base64".to_string(),
                    slot: Some(1),
                },
                entry(&[1, 2, 3], Some(1)),
                entry(&forged, Some(1)),
                entry(&untipped, Some(1)),
            ],
            &[tip_account()],
        )
        .unwrap();
        assert!(paid.accepted.is_empty());
        let errors: Vec<(usize, &str)> = paid
            .errors
            .iter()
            .map(|error| (error.index, error.error.as_str()))
            .collect();
        assert_eq!(
            errors,
            vec![
                (0, "missing or invalid shred slot"),
                (1, "failed to decode base64 transaction"),
                (2, "failed to parse transaction"),
                (3, "signature verification failed"),
                (4, "transaction does not tip an Astralane tip account"),
            ]
        );
        assert!(!paid.errors[4].signature.is_empty());
    }

    #[test]
    fn unknown_keys_and_empty_requests_are_refused() {
        let mut store = seeded(Utc::now());
        let tip = signed_transfer(tip_account(), 1_000);
        let (status, body) = register_payments(
            &mut store,
            "nobody",
            vec![entry(&tip, Some(1))],
            &[tip_account()],
        )
        .unwrap_err();
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body.error, "unknown api key");

        let (status, body) =
            register_payments(&mut store, "demo-key-tier1", vec![], &[tip_account()]).unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body.error, "no transactions provided");
    }

    async fn set_tier(db: &Db, body: serde_json::Value) -> Result<PostpackClient, ApiError> {
        set_postpack_tier(
            State(db.clone()),
            Json(serde_json::from_value(body).unwrap()),
        )
        .await
        .map(|Json(client)| client)
    }

    async fn change_credits(
        db: &Db,
        key: &str,
        body: serde_json::Value,
    ) -> Result<PostpackClient, ApiError> {
        change_postpack_credits(
            State(db.clone()),
            Path(key.to_string()),
            Json(serde_json::from_value(body).unwrap()),
        )
        .await
        .map(|Json(client)| client)
    }

    #[tokio::test]
    async fn a_tier_override_keeps_the_own_tier_and_shows_with_the_comment() {
        let db = app();
        let client = set_tier(
            &db,
            serde_json::json!({
                "api_key_value": "demo-key-new",
                "tier": "off",
                "tier_override": { "tier": "tier-1", "duration_seconds": 3600 },
                "comment": "  trial for a week  "
            }),
        )
        .await
        .unwrap();
        assert_eq!(client.tier, PostpackTier::Off);
        assert_eq!(client.comment.as_deref(), Some("trial for a week"));
        let tier_override = client.tier_override.expect("an active override");
        assert_eq!(tier_override.tier, PostpackTier::Tier1);
        assert!((3599..=3600).contains(&tier_override.time_left));

        let client = set_tier(
            &db,
            serde_json::json!({ "api_key_value": "demo-key-new", "tier": "tier-1" }),
        )
        .await
        .unwrap();
        assert_eq!(client.tier, PostpackTier::Tier1);
        assert_eq!(client.comment.as_deref(), Some("trial for a week"));
        assert!(client.tier_override.is_some());

        let client = set_tier(
            &db,
            serde_json::json!({
                "api_key_value": "demo-key-new",
                "tier": "tier-1",
                "tier_override": { "tier": "tier-1", "duration_seconds": 0 },
                "comment": " "
            }),
        )
        .await
        .unwrap();
        assert_eq!(client.comment, None);
        assert!(client.tier_override.is_none());
        let json = serde_json::to_value(&client).unwrap();
        assert!(json.get("comment").is_some() && json.get("tier_override").is_none());
        assert!(json["credits_enabled"].is_null());
    }

    #[tokio::test]
    async fn setting_a_tier_needs_a_known_key_and_a_valid_override() {
        let db = app();
        let (status, error) = set_tier(
            &db,
            serde_json::json!({ "api_key_value": "nobody", "tier": "tier-1" }),
        )
        .await
        .unwrap_err();
        assert_eq!(
            (status, error.as_str()),
            (StatusCode::NOT_FOUND, "no such api key")
        );

        for (tier_override, expected) in [
            (
                serde_json::json!({ "tier": "off", "duration_seconds": 60 }),
                "override tier must not be off",
            ),
            (
                serde_json::json!({ "tier": "tier-1", "duration_seconds": MAX_OVERRIDE_SECONDS + 1 }),
                "override duration_seconds must be between 0 and 30 days",
            ),
        ] {
            let (status, error) = set_tier(
                &db,
                serde_json::json!({
                    "api_key_value": "demo-key-new",
                    "tier": "tier-1",
                    "tier_override": tier_override
                }),
            )
            .await
            .unwrap_err();
            assert_eq!(
                (status, error.as_str()),
                (StatusCode::UNPROCESSABLE_ENTITY, expected)
            );
        }
    }

    #[tokio::test]
    async fn a_manual_payment_buys_what_it_paid_for_and_is_recorded() {
        let db = app();
        let client = change_credits(
            &db,
            "demo-key-tier1",
            serde_json::json!({
                "reason": "manual_payment",
                "comment": "wire transfer",
                "payment": { "paid_amount": 10_000_000, "paid_token": "usdc", "token_usd_price_cents": 100 }
            }),
        )
        .await
        .unwrap();
        assert_eq!(client.credits_balance, 2_000_000 + 2_000_000);

        let Json(history) = get_credit_history(
            State(db.clone()),
            Path("demo-key-tier1".to_string()),
            Query(CreditHistoryParams {
                before_id: None,
                limit: 1,
            }),
        )
        .await;
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].reason, PostpackCreditChangeReason::ManualPayment);
        assert_eq!(history[0].paid_token, Some(PaymentToken::Usdc));
        assert_eq!(history[0].comment.as_deref(), Some("wire transfer"));

        let (status, error) = change_credits(
            &db,
            "demo-key-tier1",
            serde_json::json!({
                "reason": "manual_payment",
                "delta_credits": 5,
                "comment": "wire transfer",
                "payment": { "paid_amount": 10_000_000, "paid_token": "usdc", "token_usd_price_cents": 100 }
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(
            (status, error.as_str()),
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                "delta_credits does not match what the payment buys"
            )
        );
    }

    #[tokio::test]
    async fn an_admin_change_cannot_take_the_balance_below_zero_or_skip_the_rules() {
        let db = app();
        let (status, error) = change_credits(
            &db,
            "demo-key-tier1",
            serde_json::json!({ "delta_credits": -2_000_001, "comment": "too much" }),
        )
        .await
        .unwrap_err();
        assert_eq!(
            (status, error.as_str()),
            (StatusCode::CONFLICT, "the balance cannot go below 0")
        );

        for (body, expected) in [
            (
                serde_json::json!({ "comment": "no delta" }),
                "an admin_change must say delta_credits",
            ),
            (
                serde_json::json!({ "delta_credits": 5, "comment": " " }),
                "comment must say why the balance changes",
            ),
            (
                serde_json::json!({ "delta_credits": 0, "comment": "nothing" }),
                "the change must move the balance",
            ),
            (
                serde_json::json!({ "delta_credits": 5, "reason": "write_off", "comment": "x" }),
                "only admin_change and manual_payment can be added by an admin",
            ),
        ] {
            let (status, error) = change_credits(&db, "demo-key-tier1", body)
                .await
                .unwrap_err();
            assert_eq!(
                (status, error.as_str()),
                (StatusCode::UNPROCESSABLE_ENTITY, expected)
            );
        }

        let (status, _) = change_credits(
            &db,
            "demo-key-new",
            serde_json::json!({ "delta_credits": 5, "comment": "gift" }),
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn deleting_a_client_switches_it_off_and_keeps_its_balance() {
        let db = app();
        delete_postpack_client(State(db.clone()), Path("demo-key-trial".to_string()))
            .await
            .unwrap();
        let Json(client) =
            get_postpack_client(State(db.clone()), Path("demo-key-trial".to_string()))
                .await
                .unwrap();
        assert_eq!(client.tier, PostpackTier::Off);
        assert!(client.tier_override.is_none());

        delete_postpack_client(State(db.clone()), Path("demo-key-tier1".to_string()))
            .await
            .unwrap();
        assert_eq!(
            balance(&*db.store.read().await, "demo-key-tier1"),
            2_000_000
        );
    }

    #[tokio::test]
    async fn credits_can_be_forced_on_or_off_per_client() {
        let db = app();
        for credits_enabled in [Some(true), Some(false), None] {
            let Json(client) = set_postpack_credits_enabled(
                State(db.clone()),
                Path("demo-key-tier1".to_string()),
                Json(SetCreditsEnabledRequest { credits_enabled }),
            )
            .await
            .unwrap();
            assert_eq!(client.credits_enabled, credits_enabled);
        }
        assert!(serde_json::from_str::<SetCreditsEnabledRequest>(r#"{"enabled": true}"#).is_err());
        let (status, _) = set_postpack_credits_enabled(
            State(db.clone()),
            Path("demo-key-new".to_string()),
            Json(SetCreditsEnabledRequest {
                credits_enabled: Some(true),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn settings_are_priced_per_message_type_and_unit() {
        let db = app();
        let Json(settings) = get_credits_settings(State(db.clone())).await;
        assert_eq!(
            serde_json::to_value(&settings).unwrap(),
            serde_json::json!({
                "credits_enabled": false,
                "usage_charge_interval_secs": 5,
                "credits_per_unit": {
                    "bam_preconf": { "message": 10 },
                    "rakurai_postpack": { "message": 10 },
                    "gravity_postpack": { "message": 10 }
                }
            })
        );

        let Json(settings) = update_credits_settings(
            State(db.clone()),
            Json(
                serde_json::from_str(
                    r#"{"credits_enabled": true, "credits_per_unit": {"bam_preconf": {"slot": 500}}}"#,
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        assert!(settings.credits_enabled);
        assert_eq!(
            settings.credits_per_unit(CreditedMessageType::BamPreconf, CreditedUnitType::Message),
            Some(10)
        );
        assert_eq!(
            settings.credits_per_unit(CreditedMessageType::BamPreconf, CreditedUnitType::Slot),
            Some(500)
        );

        let (status, error) = update_credits_settings(
            State(db.clone()),
            Json(
                serde_json::from_str(r#"{"credits_per_unit": {"bam_preconf": {"message": 0}}}"#)
                    .unwrap(),
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(
            (status, error.as_str()),
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                "credits_per_unit for bam_preconf per Message must be positive"
            )
        );
        for unknown in [
            r#"{"enabled": true}"#,
            r#"{"credits_per_message": {"bam_preconf": 5}}"#,
            r#"{"credits_per_unit": {"preconf": {"message": 5}}}"#,
        ] {
            assert!(
                serde_json::from_str::<PostpackSettingsUpdate>(unknown).is_err(),
                "{unknown}"
            );
        }
    }

    #[tokio::test]
    async fn a_price_quote_needs_lamports_and_a_fresh_sol_price() {
        let Json(quote) = get_credits_price(
            State(app_priced(SolPrice {
                cents: 12_000,
                updated_at: Utc::now(),
            })),
            Query(CreditsPriceParams {
                lamports: LAMPORTS_PER_SOL,
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            (quote.lamports, quote.usd_cents, quote.credits),
            (1_000_000_000, 12_000, 24_000_000)
        );
        assert_eq!(quote.credits_per_usd * 5, 1_000_000);

        let (status, error) =
            get_credits_price(State(app()), Query(CreditsPriceParams { lamports: 0 }))
                .await
                .unwrap_err();
        assert_eq!(
            (status, error.as_str()),
            (StatusCode::BAD_REQUEST, "lamports must be positive")
        );

        let (status, error) = get_credits_price(
            State(app_priced(SolPrice {
                cents: 12_000,
                updated_at: Utc::now() - TimeDelta::hours(1),
            })),
            Query(CreditsPriceParams { lamports: 1 }),
        )
        .await
        .unwrap_err();
        assert_eq!(
            (status, error.as_str()),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "no fresh SOL price, try again later"
            )
        );
    }

    #[tokio::test]
    async fn usage_is_reported_per_hour_per_day_and_over_24_hours() {
        let db = app();
        let key = || Path("demo-key-tier1".to_string());
        let Json(hourly) = get_hourly_usage(
            State(db.clone()),
            key(),
            Query(HourlyUsageParams { hours: 24 }),
        )
        .await;
        assert_eq!(hourly.len(), 24);
        assert!(hourly.windows(2).all(|pair| pair[0].hour < pair[1].hour));
        assert_eq!(hourly.last().unwrap().hour, start_of_hour(Utc::now()));
        assert!(hourly.iter().all(|hour| hour.credits == hour.messages * 10));

        let Json(summary) = get_usage_summary(State(db.clone()), key()).await.unwrap();
        assert_eq!(summary.credits_balance, 2_000_000);
        assert_eq!(
            summary.messages_24h,
            hourly.iter().map(|hour| hour.messages).sum::<i64>()
        );
        assert_eq!(summary.credits_used_24h, summary.messages_24h * 10);

        let Json(daily) = get_daily_usage(
            State(db.clone()),
            key(),
            Query(DailyUsageParams { days: 30 }),
        )
        .await
        .unwrap();
        assert_eq!(daily.instances, vec!["postpack-ams", "postpack-fra"]);
        assert!(
            daily
                .rows
                .iter()
                .all(|row| row.credits == row.messages * 10)
        );

        let Json(empty) = get_hourly_usage(
            State(db.clone()),
            Path("demo-key-off".to_string()),
            Query(HourlyUsageParams { hours: 1_000 }),
        )
        .await;
        assert_eq!(empty.len(), MAX_HOURS as usize);
        assert!(empty.iter().all(|hour| hour.credits == 0));
        assert_eq!(
            get_usage_summary(State(db), Path("demo-key-new".to_string()))
                .await
                .unwrap_err()
                .0,
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn the_credit_history_leaves_out_usage_and_pages_backwards() {
        let db = app();
        for delta in [1, 2, 3] {
            change_credits(
                &db,
                "demo-key-tier1",
                serde_json::json!({ "delta_credits": delta, "comment": "gift" }),
            )
            .await
            .unwrap();
        }
        let page = |before_id, limit| {
            get_credit_history(
                State(db.clone()),
                Path("demo-key-tier1".to_string()),
                Query(CreditHistoryParams { before_id, limit }),
            )
        };
        let Json(first) = page(None, 2).await;
        assert_eq!(
            first
                .iter()
                .map(|row| row.delta_credits)
                .collect::<Vec<_>>(),
            vec![3, 2]
        );
        let Json(rest) = page(Some(first[1].id), 50).await;
        assert_eq!(
            rest.iter().map(|row| row.delta_credits).collect::<Vec<_>>(),
            vec![1, 2_000_000]
        );
        assert!(
            rest.iter()
                .all(|row| row.reason != PostpackCreditChangeReason::Usage)
        );
    }

    #[test]
    fn the_coingecko_answer_becomes_cents() {
        assert_eq!(price_cents(r#"{"solana":{"usd":120.456}}"#), Ok(12_046));
        assert!(price_cents(r#"{"solana":{"usd":0}}"#).is_err());
        assert!(price_cents("<html>rate limited</html>").is_err());
    }

    #[test]
    fn every_default_tip_account_is_a_pubkey() {
        for account in DEFAULT_TIP_ACCOUNTS {
            Pubkey::from_str(account).unwrap();
        }
    }

    #[test]
    fn seeded_validators_are_listed_by_stake() {
        let listed = seeded_validators(Utc::now());
        assert!(
            listed
                .validators
                .windows(2)
                .all(|pair| pair[0].activated_stake_sol >= pair[1].activated_stake_sol)
        );
        let helius = &listed.validators[0];
        assert_eq!(helius.name.as_deref(), Some("Helius"));
        assert_eq!(helius.coverage, 3.45);
        assert!(helius.received_24h_slots <= helius.expected_24h_slots as u64);
        let untracked = listed.validators.last().unwrap();
        assert_eq!(untracked.name, None);
        assert_eq!(untracked.coverage, 0.0);
        assert!(untracked.by_relay.is_empty());
    }

    #[test]
    fn connected_stake_counts_each_validator_once_overall_per_provider_and_per_region() {
        let stake = connected_stake(&seeded_validators(Utc::now()));
        assert_eq!(stake.connected_validators, 5);
        assert_eq!(stake.connected_stake_sol, 28_390_000.0);
        assert_eq!(
            stake
                .by_provider
                .iter()
                .map(|provider| (
                    provider.provider.as_str(),
                    provider.validators,
                    provider.stake_sol
                ))
                .collect::<Vec<_>>(),
            [
                ("bam", 3, 15_440_000.0),
                ("gravity", 1, 3_100_000.0),
                ("rakurai", 2, 24_050_000.0),
            ]
        );
        assert_eq!(stake.by_region[0].region.as_deref(), Some("frankfurt"));
        assert_eq!(
            stake
                .by_region
                .iter()
                .map(|region| region.validators)
                .sum::<usize>(),
            5
        );
    }

    #[tokio::test]
    async fn validators_are_served_from_the_seed() {
        let app = app();
        let Json(listed) = get_validators(State(app.clone())).await;
        assert_eq!(listed.validators.len(), 7);
        let Json(stake) = get_connected_stake(State(app)).await;
        assert_eq!(stake.epoch, Some(870));
    }
}
