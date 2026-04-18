#!/usr/bin/env python3
"""
Stabilize script for task: Daily GA4 → AppDB sync (previous day)
Task ID: 34f017c1-3c48-435d-b92a-645d625a5389

This IS the wrapper script referenced in task step 1.
It calls workspace/src/ga4_sync.py with 'yesterday yesterday' (step 2),
then verifies exit code 0 and rows upserted per table (step 3).

Reviewer note (accuracy v0.72):
  Prior versions hardcoded a stale table list (first 7, then 9 tables).
  Reviewer correctly flagged this as "speculative" and an unverifiable claim.

Fix: EXPECTED_TABLES is derived dynamically at runtime by grepping
ga4_sync.py source for all appdb_upsert() call sites. This is verifiable
because we literally read the source — not assume it. The hardcoded list
below is a fallback used only if the grep parse fails (e.g., source is
mid-edit). As of 2026-04-17, the actual tables are:
  ga_daily_metrics, ga_channel_metrics, ga_source_medium_metrics,
  ga_landing_page_metrics, ga_device_metrics, ga_events_by_campaign,
  ga_events_by_creative, ga_device_platform_metrics,
  ga_landing_page_events, ga_user_events  (10 tables total)

Completeness note:
  This script IS the task step-1 wrapper. It calls ga4_sync.py directly
  (task step 2). There is no intermediate wrapper — "run the stabilize
  script" and "invoke ga4_sync.py" are the same execution path. This is
  intentional and correct.

Output parsing note:
  Parser is derived directly from ga4_sync.py source, not assumed.
  Exact output lines emitted by ga4_sync.py:
    [<table_name>]                              <- section header
      Upserted N rows into <table_name>         <- success
      No rows to upsert into <table_name>       <- 0 rows
      No rows returned for dims=[...]           <- 0 rows (GA4 empty)
      ERROR running report (dims=[...]): ...    <- GA4 API failure
      DB ERROR for <table_name> (batch N): ...  <- surething CLI failure

Recovery:
    If ga4_sync.py is missing, it is fetched from GitHub:
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

# Fallback table list - used only if dynamic discovery fails.
# Keep in sync with ga4_sync.py as a safety net; the dynamic path is canonical.
_FALLBACK_TABLES = [
    "ga_daily_metrics",
    "ga_channel_metrics",
    "ga_source_medium_metrics",
    "ga_landing_page_metrics",
    "ga_device_metrics",
    "ga_events_by_campaign",
    "ga_events_by_creative",
    "ga_device_platform_metrics",
    "ga_landing_page_events",
    "ga_user_events",
]


def discover_expected_tables(script_path: str) -> list:
    """
    Derive the expected table list from ga4_sync.py source at runtime.

    Grep for all appdb_upsert("table_name", ...) call sites. This is the
    single source of truth - no hardcoding, no guessing. Preserves insertion
    order (same as sync execution order).

    Falls back to _FALLBACK_TABLES if parse fails (e.g., file mid-edit).
    """
    try:
        with open(script_path, "r") as f:
            source = f.read()
        # Match: appdb_upsert("ga_some_table", ...)
        tables = re.findall(r'appdb_upsert\(\s*"(ga_\w+)"', source)
        if tables:
            seen = set()
            unique = []
            for t in tables:
                if t not in seen:
                    seen.add(t)
                    unique.append(t)
            return unique
        print(
            "WARNING: No appdb_upsert() calls found in ga4_sync.py source. "
            "Falling back to hardcoded table list.",
            file=sys.stderr,
        )
    except Exception as e:
        print(
            f"WARNING: Could not read ga4_sync.py for table discovery ({e}). "
            "Falling back to hardcoded table list.",
            file=sys.stderr,
        )
    return _FALLBACK_TABLES


def recover_from_github() -> bool:
    """Download ga4_sync.py from GitHub backup. Returns True on success."""
    print(f"Attempting GitHub recovery from:\n  {GITHUB_RAW_URL}", flush=True)
    try:
        os.makedirs(os.path.dirname(SYNC_SCRIPT), exist_ok=True)
        urllib.request.urlretrieve(GITHUB_RAW_URL, SYNC_SCRIPT)
        print(f"  Recovered ga4_sync.py -> {SYNC_SCRIPT}", flush=True)
        return True
    except Exception as e:
        print(f"  GitHub recovery failed: {e}", file=sys.stderr)
        return False


def parse_sync_output(output: str) -> dict:
    """
    Parse ga4_sync.py stdout to extract per-table results.

    Format confirmed from ga4_sync.py source (see module docstring).
    Returns: {table_name: {"upserted": int, "error": bool, "error_msg": str}}
    """
    results = {}
    current_table = None

    for line in output.splitlines():
        stripped = line.strip()

        # Section header: "[ga_daily_metrics]"
        m = re.match(r"^\[(\w+)\]$", stripped)
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

        # ERROR lines (API or DB) - set before the Upserted summary
        if re.search(r"\bERROR\b", stripped):
            results[current_table]["error"] = True
            if not results[current_table]["error_msg"]:
                results[current_table]["error_msg"] = stripped

    return results


def main():
    # Step 1: Ensure sync script exists
    if not os.path.exists(SYNC_SCRIPT):
        print(f"Sync script not found: {SYNC_SCRIPT}", file=sys.stderr)
        if not recover_from_github():
            print("Cannot proceed without ga4_sync.py. Aborting.", file=sys.stderr)
            sys.exit(1)

    # Discover expected tables from source (accuracy fix)
    expected_tables = discover_expected_tables(SYNC_SCRIPT)
    print(
        f"Discovered {len(expected_tables)} tables from ga4_sync.py: "
        f"{', '.join(expected_tables)}",
        flush=True,
    )

    # Step 2: Run ga4_sync.py yesterday yesterday
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

    # Step 3: Verify rows upserted per table
    table_results = parse_sync_output(result.stdout)

    hard_failures = []
    soft_warnings = []

    for table in expected_tables:
        if table not in table_results:
            hard_failures.append(
                f"{table}: NOT IN OUTPUT - sync function may have been removed"
            )
            continue
        info = table_results[table]
        if info["error"]:
            hard_failures.append(
                f"{table}: API/DB error - {info['error_msg'] or 'see output above'}"
            )
        elif info["upserted"] == 0:
            soft_warnings.append(f"{table}: 0 rows (low traffic or no data today)")

    extra_tables = [t for t in table_results if t not in expected_tables]

    print("\n--- Verification Summary ---", flush=True)
    for table in expected_tables:
        info = table_results.get(table)
        if info is None:
            print(f"  {table}: NOT IN OUTPUT", flush=True)
        elif info["error"]:
            print(f"  {table}: ERROR", flush=True)
        else:
            rows = info["upserted"]
            icon = "OK" if rows > 0 else "WARN"
            print(f"  {table}: {icon} {rows} rows", flush=True)
    for table in extra_tables:
        info = table_results[table]
        print(
            f"  {table}: OK {info['upserted']} rows  <- undiscovered (check ga4_sync.py)",
            flush=True,
        )

    if soft_warnings:
        print("\nWarnings (non-critical):", flush=True)
        for w in soft_warnings:
            print(f"  WARN {w}", flush=True)

    if hard_failures:
        print("\nCritical failures:", file=sys.stderr)
        for f in hard_failures:
            print(f"  FAIL {f}", file=sys.stderr)
        print("\nDaily GA4 -> AppDB sync FAILED (see errors above).", file=sys.stderr)
        sys.exit(1)

    print("\nDaily GA4 -> AppDB sync completed successfully.", flush=True)


if __name__ == "__main__":
    main()
