#!/usr/bin/env python3
"""
Stabilize script for task: Daily GA4 → AppDB sync (previous day)
Task ID: 34f017c1-3c48-435d-b92a-645d625a5389

Wraps workspace/src/ga4_sync.py for the daily previous-day sync.
Runs at 08:00 Asia/Shanghai; syncs yesterday's GA4 data into all 8 AppDB tables.

Improvements (v2):
- Functional GitHub recovery: actually downloads ga4_sync.py if missing (not just prints message)
- Parses stdout to verify rows upserted per table (step 3 requirement)
- Detects partial failures: tables reporting ERROR in output exit non-zero
- Tables with 0 rows but no ERROR emit a warning (legitimate on low-traffic days)
- Wraps subprocess.run in try/except to handle interpreter or OS-level failures
- Prints per-table verification summary before final status line

Usage:
    python3 scripts/daily-ga4-appdb-sync-previous-_34f017c1-3c48-435d-b92a-645d625a5389.py
"""

import sys
import subprocess
import os
import re
import urllib.request

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKSPACE_DIR = os.path.dirname(SCRIPT_DIR)
SYNC_SCRIPT = os.path.join(WORKSPACE_DIR, "src", "ga4_sync.py")

# GitHub backup (per user rule: check GitHub before rewriting lost scripts)
GITHUB_RAW_URL = (
    "https://raw.githubusercontent.com/ladyzeng12-dotcom/"
    "competitor-ad-research/main/ga4/ga4_sync.py"
)

# Tables expected every day. ERRORs on any of these → hard failure (exit 1).
# 0 rows without ERROR → warning only (legitimate on low-traffic days / weekends).
EXPECTED_TABLES = [
    "ga_daily_metrics",
    "ga_channel_metrics",
    "ga_source_medium_metrics",
    "ga_landing_page_metrics",
    "ga_device_metrics",
    "ga_events_by_campaign",
    "ga_events_by_creative",
    "ga_device_platform_metrics",
]


def recover_from_github() -> bool:
    """Download ga4_sync.py from GitHub backup. Returns True on success."""
    print(f"Attempting GitHub recovery from:\n  {GITHUB_RAW_URL}", flush=True)
    try:
        os.makedirs(os.path.dirname(SYNC_SCRIPT), exist_ok=True)
        urllib.request.urlretrieve(GITHUB_RAW_URL, SYNC_SCRIPT)
        print(f"  ✓ Recovered ga4_sync.py → {SYNC_SCRIPT}", flush=True)
        return True
    except Exception as e:
        print(f"  ✗ GitHub recovery failed: {e}", file=sys.stderr)
        return False


def parse_sync_output(output: str) -> dict:
    """
    Parse ga4_sync.py stdout to extract per-table results.

    ga4_sync.py prints:
      [<table_name>]
        Upserted N rows into <table_name>   ← success
        No rows to upsert into <table_name> ← 0 rows (legitimate)
        ERROR ...                           ← API/DB failure

    Returns: {table_name: {"upserted": int, "error": bool, "error_msg": str}}
    """
    results = {}
    current_table = None

    for line in output.splitlines():
        # Section header: "[ga_daily_metrics]"
        m = re.match(r"^\[(\w+)\]", line.strip())
        if m:
            current_table = m.group(1)
            if current_table not in results:
                results[current_table] = {"upserted": 0, "error": False, "error_msg": ""}
            continue

        if current_table is None:
            continue

        # "  Upserted N rows into ga_daily_metrics"
        m = re.search(r"Upserted (\d+) rows into (\w+)", line)
        if m:
            tbl = m.group(2)
            if tbl not in results:
                results[tbl] = {"upserted": 0, "error": False, "error_msg": ""}
            results[tbl]["upserted"] = int(m.group(1))
            continue

        # "  ERROR ..." or "  DB ERROR ..."
        if re.search(r"\bERROR\b", line):
            results[current_table]["error"] = True
            results[current_table]["error_msg"] = line.strip()

    return results


def main():
    # ── Step 1: Ensure sync script exists ─────────────────────────────────────
    if not os.path.exists(SYNC_SCRIPT):
        print(f"Sync script not found: {SYNC_SCRIPT}", file=sys.stderr)
        if not recover_from_github():
            print("Cannot proceed without ga4_sync.py. Aborting.", file=sys.stderr)
            sys.exit(1)

    # ── Step 2: Run sync, capture output for verification ─────────────────────
    try:
        result = subprocess.run(
            [sys.executable, SYNC_SCRIPT, "yesterday", "yesterday"],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as e:
        print(f"ERROR: Python interpreter not found: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Unexpected subprocess failure: {e}", file=sys.stderr)
        sys.exit(1)

    # Echo captured output so it appears in task logs
    if result.stdout:
        print(result.stdout, end="", flush=True)
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)

    if result.returncode != 0:
        print(
            f"ERROR: ga4_sync.py exited with non-zero code {result.returncode}",
            file=sys.stderr,
        )
        sys.exit(1)

    # ── Step 3: Verify rows upserted per table ─────────────────────────────────
    table_results = parse_sync_output(result.stdout)

    hard_failures = []   # tables that reported ERROR in output → exit 1
    soft_warnings = []   # tables with 0 rows but no error → warn only
    missing_tables = []  # expected tables absent from output entirely

    for table in EXPECTED_TABLES:
        if table not in table_results:
            missing_tables.append(table)
            continue
        info = table_results[table]
        if info["error"]:
            hard_failures.append(
                f"{table}: API/DB error — {info['error_msg'] or 'see output above'}"
            )
        elif info["upserted"] == 0:
            soft_warnings.append(f"{table}: 0 rows upserted (low traffic or no data)")

    # Tables that appeared in output but weren't in EXPECTED_TABLES are fine — log them
    extra_tables = [t for t in table_results if t not in EXPECTED_TABLES]

    # ── Print verification summary ─────────────────────────────────────────────
    print("\n─── Verification Summary ───", flush=True)
    for table in EXPECTED_TABLES:
        info = table_results.get(table)
        if info is None:
            print(f"  {table}: ⚠ NOT IN OUTPUT", flush=True)
        elif info["error"]:
            print(f"  {table}: ✗ ERROR", flush=True)
        else:
            rows = info["upserted"]
            icon = "✓" if rows > 0 else "⚠"
            print(f"  {table}: {icon} {rows} rows", flush=True)
    for table in extra_tables:
        info = table_results[table]
        print(f"  {table}: ✓ {info['upserted']} rows (extra table)", flush=True)

    if missing_tables:
        print(f"\n⚠ Missing from output: {', '.join(missing_tables)}", flush=True)

    if soft_warnings:
        print("\nWarnings (non-critical):", flush=True)
        for w in soft_warnings:
            print(f"  ⚠ {w}", flush=True)

    if hard_failures:
        print("\nCritical failures:", file=sys.stderr)
        for f in hard_failures:
            print(f"  ✗ {f}", file=sys.stderr)
        print("\nDaily GA4 → AppDB sync FAILED (see errors above).", file=sys.stderr)
        sys.exit(1)

    print("\nDaily GA4 → AppDB sync completed successfully.", flush=True)


if __name__ == "__main__":
    main()
