#!/usr/bin/env python3
import csv
import datetime
import io
import json
import os
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

SOLANA_RPC_URL = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_URL", "http://localhost:18123")
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "default")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "default")
FIREDANCER_EXPORT = "https://reports.firedancer.io/api/export"
FIREDANCER_DATE = os.environ.get("FIREDANCER_DATE", "")
FIREDANCER_LOOKBACK_DAYS = 7
ENABLE_PING = os.environ.get("ENABLE_PING", "1") not in ("0", "false", "False", "")
PING_CONCURRENCY = int(os.environ.get("PING_CONCURRENCY", "64"))
INSERT_CHUNK = 50000


def rpc(method, params):
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    request = urllib.request.Request(
        SOLANA_RPC_URL, data=payload, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        body = json.loads(response.read())
    if body.get("error"):
        raise RuntimeError(f"rpc {method} failed: {body['error']}")
    return body["result"]


def clickhouse(query, body=b""):
    url = CLICKHOUSE_URL.rstrip("/") + "/?" + urllib.parse.urlencode(
        {"database": CLICKHOUSE_DB, "query": query}
    )
    headers = {"Content-Type": "application/octet-stream"}
    if CLICKHOUSE_USER:
        headers["X-ClickHouse-User"] = CLICKHOUSE_USER
    if CLICKHOUSE_PASSWORD:
        headers["X-ClickHouse-Key"] = CLICKHOUSE_PASSWORD
    request = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=300) as response:
            return response.read().decode()
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"clickhouse error {error.code}: {error.read().decode()}") from error


def parse_int(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def target_epochs():
    info = rpc("getEpochInfo", [])
    current_first = info["absoluteSlot"] - info["slotIndex"]
    slots = info["slotsInEpoch"]
    next_first = current_first + slots
    return [
        (info["epoch"], current_first, slots),
        (info["epoch"] + 1, next_first, slots),
    ]


def fetch_schedule(first_slot):
    schedule = rpc("getLeaderSchedule", [first_slot])
    if not schedule:
        return []
    rows = []
    for pubkey, offsets in schedule.items():
        for offset in offsets:
            rows.append((first_slot + offset, pubkey))
    rows.sort()
    return rows


def replace_epoch(first_slot, end_slot, rows):
    clickhouse(
        f"ALTER TABLE leader_schedule DELETE WHERE slot >= {first_slot} AND slot < {end_slot} "
        f"SETTINGS mutations_sync = 2"
    )
    for start in range(0, len(rows), INSERT_CHUNK):
        chunk = rows[start:start + INSERT_CHUNK]
        body = "\n".join(
            json.dumps({"slot": slot, "leader_pubkey": pubkey}) for slot, pubkey in chunk
        ).encode()
        clickhouse("INSERT INTO leader_schedule FORMAT JSONEachRow", body)


def fetch_validator_report():
    start = (
        datetime.date.fromisoformat(FIREDANCER_DATE)
        if FIREDANCER_DATE
        else datetime.datetime.now(datetime.timezone.utc).date()
    )
    for back in range(FIREDANCER_LOOKBACK_DAYS + 1):
        day = start - datetime.timedelta(days=back)
        url = FIREDANCER_EXPORT + "?" + urllib.parse.urlencode(
            {"date": day.isoformat(), "report_type": "validator", "period": "daily", "min_stake": 0}
        )
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "curl/8.0"})
            with urllib.request.urlopen(request, timeout=120) as response:
                raw = response.read().decode("utf-8", "replace")
        except (urllib.error.URLError, OSError):
            continue
        meta = {}
        for row in csv.DictReader(io.StringIO(raw)):
            pubkey = row.get("leader")
            if not pubkey:
                continue
            meta[pubkey] = {
                "name": row.get("name") or None,
                "client_type": row.get("client") or None,
                "version": row.get("version") or None,
                "location": row.get("data_center_key") or None,
                "stake": parse_int(row.get("active_stake")),
            }
        if meta:
            print(f"firedancer report {day.isoformat()}: {len(meta)} validators", flush=True)
            return meta
    print("firedancer report: none found in lookback window", flush=True)
    return {}


def fetch_gossip_ips():
    try:
        nodes = rpc("getClusterNodes", [])
    except RuntimeError as error:
        print(f"getClusterNodes failed: {error}", flush=True)
        return {}
    ips = {}
    for node in nodes:
        pubkey = node.get("pubkey")
        gossip = node.get("gossip")
        if pubkey and gossip:
            ips[pubkey] = gossip.rsplit(":", 1)[0]
    return ips


def ping_ms(ip):
    if not ip:
        return None
    try:
        result = subprocess.run(
            ["ping", "-c", "1", "-W", "1", ip],
            capture_output=True,
            text=True,
            timeout=3,
        )
    except (subprocess.SubprocessError, OSError):
        return None
    if result.returncode != 0:
        return None
    for token in result.stdout.split():
        if token.startswith("time="):
            try:
                return float(token.split("=", 1)[1])
            except ValueError:
                return None
    return None


def measure_pings(ips_by_pubkey):
    if not ENABLE_PING or not ips_by_pubkey:
        return {}
    pubkeys = list(ips_by_pubkey)
    with ThreadPoolExecutor(max_workers=PING_CONCURRENCY) as pool:
        latencies = pool.map(lambda pubkey: ping_ms(ips_by_pubkey[pubkey]), pubkeys)
        return {pubkey: latency for pubkey, latency in zip(pubkeys, latencies)}


def upsert_validators(pubkeys, report, gossip_ips, pings):
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for pubkey in sorted(pubkeys):
        meta = report.get(pubkey, {})
        rows.append({
            "pubkey": pubkey,
            "name": meta.get("name"),
            "client_type": meta.get("client_type"),
            "version": meta.get("version"),
            "location": meta.get("location"),
            "stake": meta.get("stake"),
            "gossip_ip": gossip_ips.get(pubkey),
            "ping_ms": pings.get(pubkey),
            "updated_at": now,
        })
    for start in range(0, len(rows), INSERT_CHUNK):
        body = "\n".join(json.dumps(row) for row in rows[start:start + INSERT_CHUNK]).encode()
        clickhouse("INSERT INTO validators FORMAT JSONEachRow", body)


def main():
    leader_pubkeys = set()
    for epoch, first_slot, slots in target_epochs():
        try:
            rows = fetch_schedule(first_slot)
        except RuntimeError as error:
            print(f"epoch {epoch}: schedule unavailable, skipping ({error})", flush=True)
            continue
        if not rows:
            print(f"epoch {epoch}: no schedule yet, skipping", flush=True)
            continue
        replace_epoch(first_slot, first_slot + slots, rows)
        leader_pubkeys.update(pubkey for _, pubkey in rows)
        print(
            f"epoch {epoch}: loaded {len(rows)} slots [{first_slot}, {first_slot + slots})",
            flush=True,
        )

    if not leader_pubkeys:
        return
    report = fetch_validator_report()
    gossip_ips = fetch_gossip_ips()
    pings = measure_pings({p: gossip_ips[p] for p in leader_pubkeys if p in gossip_ips})
    upsert_validators(leader_pubkeys, report, gossip_ips, pings)
    print(
        f"validators: upserted {len(leader_pubkeys)} "
        f"(report {len(report)}, gossip {len(gossip_ips)}, pinged {len(pings)})",
        flush=True,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        sys.exit(1)
