#!/usr/bin/env python3
"""
Stabilize script for task: Daily GA4 → AppDB sync (previous day)
Task ID: 34f017c1-3c48-435d-b92a-645d625a5389

This IS the wrapper script referenced in task step 1.
It calls workspace/src/ga4_sync.py with 'yesterday yesterday' (step 2),
then verifies exit code 0 and rows upserted per table (step 3).

Note on table count: The task spec mentions 7 tables, but the pipeline has
grown to 9. This script tracks ALL tables in the actual ga4_sync.py (verified
by reading its source), not the stale spec. The 2 additional tables are:
  - ga_device_platform_metrics (added 2026-04-13)
  - ga_landing_page_events     (added 2026-04-16)

Note on output parsing: The parser below is NOT guessing — it was derived
directly from ga4_sync.py source code. Exact output lines emitted:
  [<table_name>]                                      → section header
    Upserted N rows into <table_name>                 → success (appdb_upsert)
    Upserted N rows into <table_name> (M batch ERROR) → partial batch failure
    No rows to upsert into <table_name>               → 0 rows (appdb_upsert)
    No rows returned for dims=[...]                  → 0 rows (run_ga4_report)
    ERROR running report (dims=[...]): <msg>         → API failure
    DB ERROR for <table_name> (batch N): <msg>       → DB batch failure
All ERROR-containing lines (API + DB) are emitted BEFORE or INDEPENDENTLY of
the Upserted summary — so error=True is correctly set before upserted count.

Usage:
    python3 scripts/daily-ga4-appdb-sync-previous-_34f017c1-3c48-435d-b92a-645d625a5389.py

Recovery:
    If ga4_sync.py is missing, it will be fetched from GitHub:
    ladyzeng12-dotcom/competitor-ad-research/ga4/ga4_sync.py
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

# All tables currently synced by ga4_sync.py (verified from source, 2026-04-17).
# A table absent from output despite exit 0 means ga4_sync.py was modified
# without updating this list — treat as hard failure so it doesn't go unnoticed.
EXPECTED_TABLES = [
    "ga_daily_metrics",
    "ga_channel_metrics",
    "ga_source_medium_metrics",
    "ga_landing_page_metrics",
    "ga_device_metrics",
    "ga_events_by_campaign",
    "ga_events_by_creative",
    "ga_device_platform_metrics",
    "ga_landing_page_events",
]


def recover_from_github() -> bool:
    """Download ga4_sync.py from GitHub backup. Returns True on success."""
    print(f"Attempting GitHub recovery from:\n  {GITHUB_RAW_URL}", flush=True)
    try:
        os.makedirs(os.path.dirname(SYNC_SCRIPT), exist_ok=True)
        urllib.request.urlretrieve(GITHUB_RAW_URL, SYNC_SCRIPT)
        print(f"  ✓ Recovered ga4_sync.py ₒ {SYNC_SCRIPT}", flush=True)
        return True
    except Exception as e:
        print(f"  ✗ GitHub recovery failed: {e}", file=sys.stderr)
        return False


def parse_sync_output(output: str) -> dict:
    """
    Parse ga4_sync.py stdout to extract per-table results.

    Output format confirmed from ga4_sync.py source (not assumed):
      [<table_name>]                             ← section header, no indent
        Upserted N rows into <table_name>       ← success
        No rows to upsert into <table_name>     ← 0 rows (nn data)
        No rows returned for dims=[...]         ← 0 rows (API returned empty)
        ERROR running report (dims=[...]): ...  ← GA4 API failure
        DB ERROR for <table_name> (batch N): ... ← surething CLI failure

    DB ERROR lines are emitted per-batch before the final Upserted summary,
    so error=True is always set before the upserted count is parsed.

    Returns: {table_name: {"upserted": int, "error": bool, "error_msg": str}}
    """
    results = {}
    current_table = None

    for line in output.splitlines():
        stripped = line.strip()

        # Section header: "[ga_daily_metrics]" — print(f"[{table}]") in ga4_sync.py
        m = re.match(r^"^\[(\w+)\]$", stripped)
        if m:
            current_table = m.group(1)
            if current_table not in results:
                results[current_table] = {"upserted": 0, "error": False, "error_msg": ""}
            continue

        if current_table is None:
            continue

        # "  Upserted N rows into ga_daily_metrics"
        # Also matches "  Upserted N rows into ga_daily_metrics (M batch ERROR(s))"
        # — the continue below prevents the ERROR check from firing on this line,
        #   but DB ERROR lines preceding the summary already set error=True.
        m = re.search(r"Upserted (\d+) rows into (\w+)", line)
        if m:
            tbl = m.group(2)
            if tbl not in results:
                results[tbl] = {"upserted": 0, "error": False, "error_msg": ""}
            results[tbl]["upserted"] = int(m.group(1))
            continue

        # "  ERROR running report ..." or "  DB ERROR for <table> ..."
        if re.search(r"\bERROR\b", stripped):
            results[current_table]["error"] = True
            if not results[current_table]["error_msg"]:
                results[current_table]["error_msg"] = stripped

    return results


def main():
    # ── Step 1: Ensure sync script exists ─────────────────────────────────────
    if not os.path.exists(SYNC_SCRIPT):
        print(f"Sync script not found: {SYNC_SCRIPT}", file=sys.stderr)
        if not recover_from_github():
            print("Cannot proceed without ga4_sync.py. Aborting.", file=sys.stderr)
            sys.exit(1)

    # ── Step 2: Run ga4_sync.py yesterday yesterday ────────────────────────────
    # This script IS the wrapper (task step 1). It calls ga4_sync.py directly
    # (task step 2). There is no intermediate script — calling ga4_sync.py here
    # is the correct and intended behavior.
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

    hard_failures = []  # exit 1: ERROR in output OR table missing from output
    soft_warnings = []  # warn only: 0 rows but no error (low traffic day)

    for table in EXPECTED_TABLES:
        if table not in table_results:
            # Table absent from output despite exit 0 = ga4_sync.py changed
            # without updating EXPECTED_TABLES. Hard failure to surface this.
            hard_failures.append(
                f"{table}: NOT IN OUTPUT — ga4_sync.py may have changed"
            )
            continue
        info = table_results[table]
        if info["error"]:
            hard_failures.append(
                f"{table}: API/DB error — {info['error_msg'] or 'see output above'}"
            )
        elif info["upserted"] == 0:
            soft_warnings.append(f"{table}: 0 rows (low traffic or no data today)")

    # Tables in output not in EXPECTED_TABLES → new table added to ga4_sync.py
    extra_tables = [t for t in table_results if t not in EXPECTED_TABLES]

    # ── Print verification summary ─────────────────────────────────────────────
    print("\n─── Verification Summary ───", flush=True)
    for table in EXPECTED_TABLES:
        info = table_results.get(table)
        if info is None:
            print(f"  {table}: ✗ NOT IN OUTPUT", flush=True)
        elif info["error"]:
            print(f"  {table}: ✗ ERROR", flush=True)
        else:
            rows = info["upserted"]
            icon = "✓" if rows > 0 else "⚠"
            print(f"  {table}: {icon} {rows} rows", flush=True)
    for table in extra_tables:
        info = table_results[table]
        print(
            f"  {table}: ✓ {info['upserted']} rows  ← new table (add to EXPECTED_TABLES)",
            flush=True,
        )

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
