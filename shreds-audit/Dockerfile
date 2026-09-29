FROM debian:trixie-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates iputils-ping \
    && rm -rf /var/lib/apt/lists/*

COPY target/release/shred-audit /usr/local/bin/shred-audit

RUN /usr/local/bin/shred-audit --version

ENTRYPOINT ["/usr/local/bin/shred-audit"]
CMD ["--config", "/etc/shred-audit/config.yaml", "--export", "postgres", "--no-tui"]
