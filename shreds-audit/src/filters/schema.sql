CREATE TABLE IF NOT EXISTS filter_windows (
    host                 text             NOT NULL,
    source               text             NOT NULL,
    kind                 text             NOT NULL,
    commitment           text,
    connection_id        bigint           NOT NULL,
    started_at           timestamptz      NOT NULL,
    ended_at             timestamptz      NOT NULL,
    duration_secs        double precision NOT NULL,
    bundle               text             NOT NULL,
    filters              jsonb            NOT NULL,
    end_reason           text             NOT NULL,
    connect_ms           double precision,
    first_msg_ms         double precision,
    tip_open             bigint,
    tip_close            bigint,
    audit_start_slot     bigint,
    audit_end_slot       bigint,

    delivered            bigint           NOT NULL,
    tagged               jsonb            NOT NULL,
    untagged_updates     bigint           NOT NULL,
    unknown_tags         bigint           NOT NULL,
    duplicates           bigint           NOT NULL,
    tag_false_positive   bigint           NOT NULL,
    tag_missing          bigint           NOT NULL,
    vote_flag_mismatch   bigint           NOT NULL,

    slots_checked        bigint           NOT NULL,
    slots_skipped        bigint           NOT NULL,
    slots_unchecked      bigint           NOT NULL,
    expected             bigint           NOT NULL,
    matched              bigint           NOT NULL,
    missed               bigint           NOT NULL,
    extra_in_block       bigint           NOT NULL,
    extra_not_in_block   bigint           NOT NULL,

    contested            bigint           NOT NULL,
    wins                 bigint           NOT NULL,
    behind_p50_us        double precision,
    behind_p90_us        double precision,
    behind_p99_us        double precision,
    vs_shred_n           bigint           NOT NULL,
    vs_shred_p10_us      double precision,
    vs_shred_p50_us      double precision,
    vs_shred_p90_us      double precision,
    vs_shred_p99_us      double precision,
    server_delay_p50_us  double precision,
    server_delay_p90_us  double precision,
    server_delay_p99_us  double precision,
    PRIMARY KEY (host, source, started_at)
);
CREATE INDEX IF NOT EXISTS filter_windows_ended_at ON filter_windows (ended_at);

CREATE TABLE IF NOT EXISTS filter_slot_checks (
    ts                   timestamptz      NOT NULL,
    host                 text             NOT NULL,
    source               text             NOT NULL,
    kind                 text             NOT NULL,
    window_started_at    timestamptz      NOT NULL,
    bundle               text             NOT NULL,
    filter               text             NOT NULL,
    slot                 bigint           NOT NULL,
    block_status         text             NOT NULL,
    block_txs            integer          NOT NULL,
    expected             integer          NOT NULL,
    delivered            integer          NOT NULL,
    matched              integer          NOT NULL,
    missed               integer          NOT NULL,
    extra_in_block       integer          NOT NULL,
    extra_not_in_block   integer          NOT NULL,
    PRIMARY KEY (host, source, window_started_at, slot, filter)
);
CREATE INDEX IF NOT EXISTS filter_slot_checks_ts ON filter_slot_checks (ts);

CREATE TABLE IF NOT EXISTS filter_violations (
    ts                   timestamptz      NOT NULL,
    host                 text             NOT NULL,
    source               text             NOT NULL,
    kind                 text             NOT NULL,
    window_started_at    timestamptz      NOT NULL,
    bundle               text             NOT NULL,
    filter               text             NOT NULL,
    slot                 bigint           NOT NULL,
    signature            text             NOT NULL,
    violation            text             NOT NULL,
    reason               text             NOT NULL,
    PRIMARY KEY (host, source, window_started_at, slot, filter, signature, violation)
);
CREATE INDEX IF NOT EXISTS filter_violations_ts ON filter_violations (ts);

CREATE TABLE IF NOT EXISTS rpc_block_fetches (
    ts                   timestamptz      NOT NULL,
    host                 text             NOT NULL,
    rpc                  text             NOT NULL,
    slot                 bigint           NOT NULL,
    attempt              integer          NOT NULL,
    status               text             NOT NULL,
    latency_ms           double precision NOT NULL,
    block_txs            integer,
    error                text,
    PRIMARY KEY (host, slot, attempt, ts)
);
CREATE INDEX IF NOT EXISTS rpc_block_fetches_ts ON rpc_block_fetches (ts);
