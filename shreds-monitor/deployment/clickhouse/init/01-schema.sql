-- Schema for the standalone shreds-monitor analytics stack.
-- Applied once on first ClickHouse init (docker-entrypoint-initdb.d).
--
-- Mirrors db-core/clickhouse/fec_stats.sql, minus the Postgres-engine `pg`
-- database. Provider names are resolved from a local `providers` table that
-- the monitor upserts from its SHRED_PROVIDERS env config on startup.

create table if not exists fec_stats
(
    provider_id              UInt32,
    slot                     UInt64,
    fec_set_index            UInt32,
    fec_first_shred_delay_ns Nullable(Int64),
    fec_decode_delay_ns      Nullable(Int64),
    fec_last_shred_delay_ns  Nullable(Int64),
    invalid_shreds           UInt32,
    missed_shreds            UInt32,
    duplicated_shreds        UInt32,
    is_valid                 Bool
)
engine = MergeTree
order by (provider_id, slot, fec_set_index);

CREATE TABLE IF NOT EXISTS leader_schedule
(
    slot          UInt64,
    leader_pubkey LowCardinality(String),
    PROJECTION by_leader
    (
        SELECT slot, leader_pubkey
        ORDER BY (leader_pubkey, slot)
    )
)
ENGINE = MergeTree
ORDER BY slot;

create table if not exists slot_timings
(
    slot    UInt64,
    seen_at DateTime64(3)
)
engine = MergeTree
order by slot;

create table if not exists validators
(
    pubkey      String,
    name        Nullable(String),
    client_type Nullable(String),
    version     Nullable(String),
    location    Nullable(String),
    stake       Nullable(UInt64),
    gossip_ip   Nullable(String),
    ping_ms     Nullable(Float64),
    updated_at  DateTime
)
engine = ReplacingMergeTree(updated_at)
order by pubkey;

-- Provider id -> name, upserted by the monitor from SHRED_PROVIDERS.
create table if not exists providers
(
    id         UInt32,
    name       String,
    updated_at DateTime
)
engine = ReplacingMergeTree(updated_at)
order by id;
