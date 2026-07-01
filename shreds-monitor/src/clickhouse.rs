use crate::config::ClickhouseConfig;

pub fn create_clickhouse_client(cfg: &ClickhouseConfig) -> clickhouse::Client {
    clickhouse::Client::default()
        .with_url(&cfg.url)
        .with_database(&cfg.database)
        .with_user(&cfg.user)
        .with_password(&cfg.password)
        .with_compression(clickhouse::Compression::Lz4)
}
