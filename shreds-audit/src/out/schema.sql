CREATE TABLE IF NOT EXISTS audit_runs (
    host           text        NOT NULL,
    started_at     timestamptz NOT NULL,
    tool_version   text        NOT NULL,
    git_commit     text        NOT NULL,
    schema_version integer     NOT NULL,
    rpc_url        text        NOT NULL,
    providers      text[]      NOT NULL,
    PRIMARY KEY (host, started_at)
);

CREATE TABLE IF NOT EXISTS provider_stats (
    ts               timestamptz      NOT NULL,
    host             text             NOT NULL,
    provider         text             NOT NULL,

    window_secs      double precision NOT NULL,

    sets_present     bigint           NOT NULL,
    sets_valid       bigint           NOT NULL,
    sets_total       bigint           NOT NULL,

    races            bigint           NOT NULL,
    wins             bigint           NOT NULL,

    behind_sum_us    double precision NOT NULL,
    behind_n         bigint           NOT NULL,
    behind_max_us    double precision NOT NULL,

    missed           bigint           NOT NULL,
    invalid          bigint           NOT NULL,
    invalid_sig      bigint           NOT NULL,
    invalid_data     bigint           NOT NULL,
    invalid_unknown  bigint           NOT NULL,
    duplicated       bigint           NOT NULL,
    sig_unverifiable bigint           NOT NULL,

    shreds_data      bigint           NOT NULL,
    shreds_code      bigint           NOT NULL,

    fill_sum_us      double precision NOT NULL,
    fill_n           bigint           NOT NULL,
    fill_max_us      double precision NOT NULL,

    PRIMARY KEY (ts, host, provider)
);

CREATE TABLE IF NOT EXISTS txn_source_stats (
    ts                    timestamptz      NOT NULL,
    host                  text             NOT NULL,
    source                text             NOT NULL,
    kind                  text             NOT NULL,

    seen                  bigint           NOT NULL,
    contested             bigint           NOT NULL,
    winrate               double precision,
    behind_mean_us        double precision,
    behind_p50_us         double precision,
    behind_p90_us         double precision,
    behind_p99_us         double precision,

    onchain_slots_checked bigint           NOT NULL,
    onchain_slots_absent  bigint           NOT NULL,
    onchain_txns          bigint           NOT NULL,
    onchain_missed        bigint           NOT NULL,
    onchain_corrupted     bigint           NOT NULL,
    onchain_duplicated    bigint           NOT NULL,
    onchain_bad           bigint           NOT NULL,
    onchain_bad_pct       double precision,

    PRIMARY KEY (ts, host, source)
);

CREATE TABLE IF NOT EXISTS audit_counters (
    ts          timestamptz      NOT NULL,
    host        text             NOT NULL,
    window_secs double precision NOT NULL,
    name        text             NOT NULL,
    value       bigint           NOT NULL,
    PRIMARY KEY (ts, host, name)
);

CREATE TABLE IF NOT EXISTS provider_pings (
    ts         timestamptz      NOT NULL,
    host       text             NOT NULL,
    provider   text             NOT NULL,
    ip         text             NOT NULL,
    kind       text             NOT NULL,
    source     text             NOT NULL,
    rtt_ms     double precision,
    checked_at timestamptz,
    PRIMARY KEY (ts, host, provider, ip)
);
