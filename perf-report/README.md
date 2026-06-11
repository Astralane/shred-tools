# perf-report

Shred-source performance measurement: extracts competition data from the
`shred_metrics` DB, joins it with the live Solana leader schedule, and builds
xlsx reports (general, per-provider shareable, per-leader, signature-valid).

## Scripts

| script | what it does |
|---|---|
| `extract.py` | streams last 24h of `shred_arrivals` → CSVs in `$OUT_DIR`: per-source stats (win rate, delta percentiles vs jito), head-to-head, per-leader splits, own-vs-other slots, 7d daily trend, `shred_first_provider` shares, IP coverage. Optional merry test-hub section (astra vs lucky vs jito). |
| `build_xlsx.py` | CSVs → `shred_source_performance.xlsx` (general, with daily-trend line charts) + one small sanitized xlsx per provider in `provider_reports/` (only their streams — shareable). |
| `build_leader_xlsx.py` | CSVs → `per_leader_breakdown.xlsx` with `src_ip` + ASN/datacenter columns (ipinfo.io, cached in `$DATA_DIR/ip_asn.json`). |
| `sig_valid_panel.py` | per-source stats counting only arrivals whose signature matches jito's for the same (slot, fec_set) — catches sources sending repacked/non-canonical FEC sets. |

## Requirements

```
pip install psycopg requests openpyxl
```

The DB hostnames (`rpc`, `client-rpc`) are tailnet names — run from a machine
on the tailscale network (e.g. merry-gar).

## Env

```bash
export SHRED_METRICS_URL="postgresql://<user>:<pass>@rpc:5432/shred_metrics"
export ASTRALANE_DB_URL="postgresql://<user>:<pass>@client-rpc:5432/astralane_db"
export MERRY_DB_URL="postgres://postgres:postgres@127.0.0.1:45432/postgres"  # optional, merry only
export DATA_DIR=./data   # CSVs (default)
export OUT_DIR=./out     # xlsx outputs (default; extract.py writes CSVs to its own OUT_DIR=./data)
```

## Run

```bash
# 1. extract (~25 min for 24h window; prints progress per hour, ends with DONE)
OUT_DIR=./data python3 extract.py

# 2. provider name -> ip map (needed by build_leader_xlsx.py)
psql "$ASTRALANE_DB_URL" -Atc "SELECT name, src_ip FROM shred_providers" > data/name_ip_map.txt

# 3. build spreadsheets
python3 build_xlsx.py          # general + provider_reports/*.xlsx
python3 build_leader_xlsx.py   # per_leader_breakdown.xlsx

# 4. signature-validity panel (CSV only)
OUT_DIR=./data python3 sig_valid_panel.py

# 5. send to telegram
curl -s -X POST "https://api.telegram.org/bot$TG_BOT_TOKEN/sendDocument" \
  -F chat_id="$TG_CHAT_ID" -F message_thread_id="$TG_THREAD_ID" \
  -F document=@out/shred_source_performance.xlsx -F caption="..."
```

## Notes

- `delta_ns` = arrival vs the jito baseline on the same hub for the same FEC
  set; negative = earlier than jito. Only each source's FIRST arrival per FEC
  set counts (the hub dedups since 2026-06-10; older multi-row data is
  collapsed client-side).
- Provider→validator pubkeys for the leader analysis live in `extract.py`
  (`PROVIDER_PUBKEYS`); keys marked confirmed came from the providers.
- Hubs: `198.13.138.175` = fra (terra-1 prod), `64.130.37.201` = ny.
