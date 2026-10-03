use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use agave_transaction_view::sanitize::SanitizeConfig;
use agave_transaction_view::transaction_view::SanitizedTransactionView;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use chrono::{DateTime, TimeDelta, Utc};
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
const USER_AGENT: &str = concat!("astralane-preconf-admin-mock/", env!("CARGO_PKG_VERSION"));
const PAYMENT_CHECK_INTERVAL: Duration = Duration::from_secs(5);
const MAX_POSTPACK_PAY_TRANSACTIONS: usize = 1000;
const PACKET_DATA_SIZE: usize = 1232;
const CREDITS_PER_CENT: u128 = 2_000;
const LAMPORTS_PER_SOL: u128 = 1_000_000_000;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
enum CreditedMessageType {
    Preconf,
}

impl CreditedMessageType {
    const ALL: [Self; 1] = [Self::Preconf];

    fn label(self) -> &'static str {
        match self {
            Self::Preconf => "preconf",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
enum PostpackCreditChangeReason {
    Tip,
    ManualPayment,
    Usage,
    AdminChange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
enum PaymentToken {
    Sol,
    Usdc,
    Usdt,
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
        if self.duration_seconds < 0 {
            return Err("override duration_seconds must be >= 0");
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

#[derive(Debug, Clone, Copy, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
struct CreditPayment {
    paid_amount: i64,
    #[allow(dead_code)]
    paid_token: PaymentToken,
    token_usd_price_cents: i64,
}

#[derive(Debug, Deserialize, ToSchema)]
struct PostpackCreditChange {
    delta_credits: i64,
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
    fn validate(&self) -> Result<(), &'static str> {
        match self.reason {
            PostpackCreditChangeReason::AdminChange if self.payment.is_some() => {
                return Err("only a manual_payment records a payment");
            }
            PostpackCreditChangeReason::AdminChange => {}
            PostpackCreditChangeReason::ManualPayment if self.delta_credits < 0 => {
                return Err("a manual_payment cannot take credits away");
            }
            PostpackCreditChangeReason::ManualPayment => match self.payment {
                None => return Err("a manual_payment must say what was paid"),
                Some(payment) if payment.paid_amount <= 0 => {
                    return Err("paid_amount must be positive");
                }
                Some(payment) if payment.token_usd_price_cents <= 0 => {
                    return Err("token_usd_price_cents must be positive");
                }
                Some(_) => {}
            },
            PostpackCreditChangeReason::Tip | PostpackCreditChangeReason::Usage => {
                return Err("only admin_change and manual_payment can be added by an admin");
            }
        }
        if self.delta_credits == 0 {
            return Err("delta_credits must not be zero");
        }
        if self.comment.trim().is_empty() {
            return Err("comment must say why the balance changes");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, ToSchema)]
struct CreditsSettings {
    enabled: bool,
    max_overdraft: u64,
    usage_charge_interval_secs: u64,
    credits_per_message: BTreeMap<CreditedMessageType, u64>,
}

impl Default for CreditsSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            max_overdraft: 200_000,
            usage_charge_interval_secs: 5,
            credits_per_message: CreditedMessageType::ALL
                .into_iter()
                .map(|t| (t, 10))
                .collect(),
        }
    }
}

impl CreditsSettings {
    fn validate(&self) -> Result<(), String> {
        for message_type in CreditedMessageType::ALL {
            if self
                .credits_per_message
                .get(&message_type)
                .copied()
                .unwrap_or(0)
                == 0
            {
                return Err(format!(
                    "credits_per_message for {} must be positive",
                    message_type.label()
                ));
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
struct CreditsSettingsUpdate {
    enabled: Option<bool>,
    max_overdraft: Option<u64>,
    usage_charge_interval_secs: Option<u64>,
    #[serde(default)]
    credits_per_message: BTreeMap<CreditedMessageType, u64>,
}

impl CreditsSettingsUpdate {
    fn apply(&self, current: &CreditsSettings) -> CreditsSettings {
        let mut credits_per_message = current.credits_per_message.clone();
        credits_per_message.extend(&self.credits_per_message);
        CreditsSettings {
            enabled: self.enabled.unwrap_or(current.enabled),
            max_overdraft: self.max_overdraft.unwrap_or(current.max_overdraft),
            usage_charge_interval_secs: self
                .usage_charge_interval_secs
                .unwrap_or(current.usage_charge_interval_secs),
            credits_per_message,
        }
    }
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

#[derive(Debug, Clone, Copy)]
struct SolPrice {
    cents: u64,
    updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct CreditsPriceParams {
    #[param(example = 1.5)]
    sol: f64,
}

#[derive(Debug, Serialize, ToSchema)]
struct CreditsPrice {
    #[schema(example = 1.5)]
    sol: f64,
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
    updated_at: Option<DateTime<Utc>>,
}

fn credits_price(sol: f64, price: SolPrice) -> Result<CreditsPrice, String> {
    let lamports = (sol * LAMPORTS_PER_SOL as f64).round();
    if !(lamports.is_finite() && lamports >= 1.0 && lamports <= u64::MAX as f64) {
        return Err(format!("sol must be at least one lamport, got {sol}"));
    }
    let lamports = lamports as u64;
    let usd_cents = u128::from(lamports) * u128::from(price.cents) / LAMPORTS_PER_SOL;
    Ok(CreditsPrice {
        sol,
        lamports,
        usd_cents: u64::try_from(usd_cents).unwrap_or(u64::MAX),
        credits: credits_for_lamports(lamports, price.cents) as u64,
        sol_usd_price_cents: price.cents,
        credits_per_usd: (CREDITS_PER_CENT * 100) as u64,
        updated_at: price.updated_at,
    })
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
    override_expires_at: Option<DateTime<Utc>>,
}

impl ClientRecord {
    fn view(&self, now: DateTime<Utc>) -> PostpackClient {
        PostpackClient {
            api_key_value: self.api_key_value.clone(),
            tier: self.tier,
            credits_balance: self.credits_balance,
            created_at: self.created_at,
            comment: self.comment.clone(),
            tier_override: self
                .override_expires_at
                .filter(|expires_at| *expires_at > now)
                .map(|expires_at| TierOverrideView {
                    tier: self.tier,
                    time_left: ((expires_at - now).num_milliseconds() + 999) / 1000,
                }),
        }
    }
}

#[derive(Debug)]
struct Payment {
    api_key: String,
    tip: u64,
    accounted: bool,
}

#[derive(Default)]
struct Store {
    api_keys: HashSet<String>,
    clients: HashMap<String, ClientRecord>,
    settings: CreditsSettings,
    paid_signatures: HashSet<String>,
    payments: Vec<Payment>,
}

struct App {
    store: RwLock<Store>,
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

fn seeded() -> Store {
    let now = Utc::now();
    let clients = [
        (
            "demo-key-tier1",
            PostpackTier::Tier1,
            2_000_000,
            Some("seeded paying client"),
            None,
        ),
        (
            "demo-key-trial",
            PostpackTier::Tier1,
            0,
            Some("seeded trial client"),
            Some(now + TimeDelta::days(7)),
        ),
        ("demo-key-off", PostpackTier::Off, 0, None, None),
    ]
    .into_iter()
    .map(
        |(key, tier, credits_balance, comment, override_expires_at)| {
            (
                key.to_string(),
                ClientRecord {
                    api_key_value: key.to_string(),
                    tier,
                    credits_balance,
                    created_at: now,
                    comment: comment.map(str::to_string),
                    override_expires_at,
                },
            )
        },
    )
    .collect();
    Store {
        api_keys: [
            "demo-key-tier1",
            "demo-key-trial",
            "demo-key-off",
            "demo-key-new",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
        clients,
        ..Store::default()
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Preconf admin API",
        description = "In-memory mock of the admin-server postpack endpoints and the public-server /postpack-pay endpoint. Known API keys: demo-key-tier1, demo-key-trial, demo-key-off, demo-key-new (not a postpack client yet)."
    ),
    paths(
        get_all_postpack_clients,
        set_postpack_tier,
        get_postpack_client,
        delete_postpack_client,
        change_postpack_credits,
        get_credits_settings,
        update_credits_settings,
        get_credits_price,
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
            "/admin/postpack/credits/settings",
            get(get_credits_settings).put(update_credits_settings),
        )
        .route("/admin/postpack/credits/price", get(get_credits_price))
        .route("/postpack-pay", post(postpack_pay))
        .with_state(db)
}

#[utoipa::path(
    get,
    path = "/admin/postpack/credits/settings",
    tag = "credits settings",
    summary = "Read the credits settings",
    responses((status = 200, body = CreditsSettings))
)]
async fn get_credits_settings(State(db): State<Db>) -> Json<CreditsSettings> {
    Json(db.store.read().await.settings.clone())
}

#[utoipa::path(
    put,
    path = "/admin/postpack/credits/settings",
    tag = "credits settings",
    summary = "Change some credits settings",
    description = "Only the fields present in the body change; prices are merged per message type.",
    request_body = CreditsSettingsUpdate,
    responses(
        (status = 200, body = CreditsSettings),
        (status = 422, description = "The resulting settings are invalid", body = String, example = "credits_per_message for preconf must be positive")
    )
)]
async fn update_credits_settings(
    State(db): State<Db>,
    Json(body): Json<CreditsSettingsUpdate>,
) -> Result<Json<CreditsSettings>, ApiError> {
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
    summary = "Convert an amount of SOL to USD and credits",
    description = "The SOL price comes from CoinGecko and refreshes every 5 minutes; updated_at is null until the first successful fetch, while a default price is used. One USD always buys 200000 credits, so 5 USD buy one million.",
    params(CreditsPriceParams),
    responses(
        (status = 200, body = CreditsPrice),
        (status = 400, body = String, example = "sol must be at least one lamport, got 0")
    )
)]
async fn get_credits_price(
    State(db): State<Db>,
    Query(params): Query<CreditsPriceParams>,
) -> Result<Json<CreditsPrice>, ApiError> {
    credits_price(params.sol, *db.sol_price.read().await)
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
    description = "Creates the client with a zero balance if it does not exist yet. A tier_override sets the tier for duration_seconds, after which the client is neither limited nor billed differently; 0 ends an override now. An omitted tier_override or comment keeps the current one; a blank comment clears it.",
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
    let now = Utc::now();
    let override_expires_at = match &body.tier_override {
        Some(tier_override) => {
            tier_override
                .validate()
                .map_err(|error| (StatusCode::UNPROCESSABLE_ENTITY, error.to_string()))?;
            let expires_at = TimeDelta::try_seconds(tier_override.duration_seconds)
                .and_then(|duration| now.checked_add_signed(duration))
                .ok_or_else(|| {
                    (
                        StatusCode::UNPROCESSABLE_ENTITY,
                        "override duration_seconds is too large".to_string(),
                    )
                })?;
            Some(expires_at)
        }
        None => None,
    };
    let tier = body
        .tier_override
        .map_or(body.tier, |tier_override| tier_override.tier);
    let client = store
        .clients
        .entry(body.api_key_value.clone())
        .or_insert_with(|| ClientRecord {
            api_key_value: body.api_key_value.clone(),
            tier,
            credits_balance: 0,
            created_at: now,
            comment: None,
            override_expires_at: None,
        });
    client.tier = tier;
    if override_expires_at.is_some() {
        client.override_expires_at = override_expires_at;
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
    description = "reason is admin_change (default, no payment) or manual_payment (positive delta with a payment). comment must not be blank.",
    params(("key" = String, Path, description = "API key value")),
    request_body = PostpackCreditChange,
    responses(
        (status = 200, body = PostpackClient),
        (status = 404, body = String, example = "no such postpack client"),
        (status = 422, description = "The change is invalid", body = String, example = "a manual_payment must say what was paid")
    )
)]
async fn change_postpack_credits(
    State(db): State<Db>,
    Path(key): Path<String>,
    Json(body): Json<PostpackCreditChange>,
) -> Result<Json<PostpackClient>, ApiError> {
    body.validate()
        .map_err(|error| (StatusCode::UNPROCESSABLE_ENTITY, error.to_string()))?;
    let mut store = db.store.write().await;
    let client = store.clients.get_mut(&key).ok_or_else(not_found)?;
    client.credits_balance += body.delta_credits;
    Ok(Json(client.view(Utc::now())))
}

#[utoipa::path(
    delete,
    path = "/admin/postpack/{key}",
    tag = "clients",
    summary = "Delete a postpack client",
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
    db.store
        .write()
        .await
        .clients
        .remove(&key)
        .map(|_| "success")
        .ok_or_else(not_found)
}

#[utoipa::path(
    post,
    path = "/postpack-pay",
    tag = "public server",
    summary = "Register signed tip transactions that pay for postpack credits",
    description = "Served by the public server in production. Each transaction must be base64, carry a positive slot, and verify its signatures; unlike the public server, the mock also accepts a transaction that does not tip, which then earns no credits. Rejected entries are listed in errors while the rest are accepted. A signature that was already registered is neither accepted nor an error. The mock treats every accepted transaction as landed and, while credits are enabled and the key is a postpack client, credits its tip within 5 seconds at the current SOL price from /admin/postpack/credits/price.",
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

fn credits_for_lamports(lamports: u64, sol_usd_price_cents: u64) -> i64 {
    let credits = u128::from(lamports) * u128::from(sol_usd_price_cents) * CREDITS_PER_CENT
        / LAMPORTS_PER_SOL;
    i64::try_from(credits).unwrap_or(i64::MAX)
}

fn credit_payments(store: &mut Store, sol_usd_price_cents: u64) -> usize {
    if !store.settings.enabled {
        return 0;
    }
    let Store {
        clients, payments, ..
    } = store;
    let mut credited = 0;
    for payment in payments.iter_mut().filter(|payment| !payment.accounted) {
        let Some(client) = clients.get_mut(&payment.api_key) else {
            continue;
        };
        payment.accounted = true;
        let credits = credits_for_lamports(payment.tip, sol_usd_price_cents);
        if credits > 0 {
            client.credits_balance = client.credits_balance.saturating_add(credits);
            credited += 1;
        }
    }
    credited
}

fn spawn_payment_checks(db: Db) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(PAYMENT_CHECK_INTERVAL);
        loop {
            tick.tick().await;
            let cents = db.sol_price.read().await.cents;
            let credited = credit_payments(&mut *db.store.write().await, cents);
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
                        updated_at: Some(Utc::now()),
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
    let db = Arc::new(App {
        store: RwLock::new(seeded()),
        tip_accounts,
        sol_price: RwLock::new(SolPrice {
            cents: sol_usd_price_cents,
            updated_at: None,
        }),
    });
    spawn_sol_price_refresh(db.clone(), sol_price_url);
    spawn_payment_checks(db.clone());
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("cannot bind the listen address");
    println!("preconf admin mock listening on {address}, docs at http://{address}/swagger-ui");
    println!(
        "tip accounts {:?}, default SOL price {sol_usd_price_cents} cents",
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

    fn app() -> Db {
        Arc::new(App {
            store: RwLock::new(seeded()),
            tip_accounts: vec![tip_account()],
            sol_price: RwLock::new(SolPrice {
                cents: PRICE_CENTS,
                updated_at: None,
            }),
        })
    }

    #[test]
    fn a_tip_is_registered_once_and_credited_only_while_credits_are_enabled() {
        let mut store = seeded();
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

        assert_eq!(credit_payments(&mut store, PRICE_CENTS), 0);
        assert_eq!(store.clients["demo-key-tier1"].credits_balance, 2_000_000);

        store.settings.enabled = true;
        assert_eq!(credit_payments(&mut store, PRICE_CENTS), 1);
        assert_eq!(
            store.clients["demo-key-tier1"].credits_balance,
            2_000_000 + 300_000
        );
        assert_eq!(credit_payments(&mut store, PRICE_CENTS), 0);
    }

    #[test]
    fn a_tip_from_a_key_that_is_not_a_postpack_client_waits_until_it_becomes_one() {
        let mut store = seeded();
        store.settings.enabled = true;
        let tip = signed_transfer(tip_account(), LAMPORTS_PER_SOL as u64);
        register_payments(
            &mut store,
            "demo-key-new",
            vec![entry(&tip, Some(1))],
            &[tip_account()],
        )
        .unwrap();
        assert_eq!(credit_payments(&mut store, PRICE_CENTS), 0);

        store.clients.insert(
            "demo-key-new".to_string(),
            ClientRecord {
                api_key_value: "demo-key-new".to_string(),
                tier: PostpackTier::Tier1,
                credits_balance: 0,
                created_at: Utc::now(),
                comment: None,
                override_expires_at: None,
            },
        );
        assert_eq!(credit_payments(&mut store, PRICE_CENTS), 1);
        assert_eq!(store.clients["demo-key-new"].credits_balance, 30_000_000);
    }

    #[test]
    fn bad_entries_are_listed_as_errors_like_the_public_server_does() {
        let mut store = seeded();
        let mut forged = signed_transfer(tip_account(), 1_000);
        let last = forged.len() - 1;
        forged[last] ^= 1;

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
            ]
        );
    }

    #[test]
    fn a_transaction_without_a_tip_is_accepted_and_earns_nothing() {
        let mut store = seeded();
        store.settings.enabled = true;
        let paid = register_payments(
            &mut store,
            "demo-key-tier1",
            vec![entry(
                &signed_transfer(Pubkey::new_unique(), 1_000),
                Some(1),
            )],
            &[tip_account()],
        )
        .unwrap();
        assert_eq!(paid.accepted.len(), 1);
        assert!(paid.errors.is_empty());

        assert_eq!(credit_payments(&mut store, PRICE_CENTS), 0);
        assert_eq!(store.clients["demo-key-tier1"].credits_balance, 2_000_000);
        assert!(store.payments.iter().all(|payment| payment.accounted));
    }

    #[test]
    fn unknown_keys_and_empty_requests_are_refused() {
        let mut store = seeded();
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

    #[tokio::test]
    async fn a_tier_override_and_a_comment_show_on_the_client() {
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
        assert_eq!(client.tier, PostpackTier::Tier1);
        assert_eq!(client.comment.as_deref(), Some("trial for a week"));
        let tier_override = client.tier_override.expect("an active override");
        assert!((3599..=3600).contains(&tier_override.time_left));

        let client = set_tier(
            &db,
            serde_json::json!({ "api_key_value": "demo-key-new", "tier": "tier-1" }),
        )
        .await
        .unwrap();
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

        let (status, error) = set_tier(
            &db,
            serde_json::json!({
                "api_key_value": "demo-key-new",
                "tier": "tier-1",
                "tier_override": { "tier": "off", "duration_seconds": 60 }
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(
            (status, error.as_str()),
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                "override tier must not be off"
            )
        );
    }

    #[test]
    fn a_sol_at_120_dollars_buys_24_million_credits_and_5_dollars_buy_a_million() {
        let at_120 = SolPrice {
            cents: 12_000,
            updated_at: None,
        };
        let one = credits_price(1.0, at_120).unwrap();
        assert_eq!(
            (one.lamports, one.usd_cents, one.credits),
            (1_000_000_000, 12_000, 24_000_000)
        );
        assert_eq!(one.credits_per_usd * 5, 1_000_000);

        let some = credits_price(0.1, at_120).unwrap();
        assert_eq!(
            (some.lamports, some.usd_cents, some.credits),
            (100_000_000, 1_200, 2_400_000)
        );

        for bad in [0.0, -1.0, f64::NAN, f64::INFINITY, 1e30] {
            assert!(
                credits_price(bad, at_120).is_err(),
                "{bad} is not an amount"
            );
        }
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
}
