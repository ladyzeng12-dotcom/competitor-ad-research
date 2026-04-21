#!/usr/bin/env python3
"""
GA4 → AppDB sync script
Usage: python3 src/ga4_sync.py <start_date> <end_date>
  e.g. python3 src/ga4_sync.py 2026-04-09 2026-04-11
       python3 src/ga4_sync.py yesterday yesterday
"""

import sys
import json
import subprocess
from datetime import datetime, timedelta

PROPERTY = "properties/531750988"
DATE_RANGES_ARG = []

def resolve_date(d: str) -> str:
    """Resolve special date keywords to YYYY-MM-DD."""
    if d == "yesterday":
        return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    if d == "today":
        return datetime.now().strftime("%Y-%m-%d")
    return d

def run_ga4_report(dimensions, metrics, date_ranges, dimension_filter=None, limit=10000):
    """Call GA4 RunReport via run_composio_tool helper."""
    args = {
        "property": PROPERTY,
        "dimensions": [{"name": d} for d in dimensions],
        "metrics": [{"name": m} for m in metrics],
        "dateRanges": date_ranges,
        "limit": limit,
    }
    if dimension_filter:
        args["dimensionFilter"] = dimension_filter
    data, error = run_composio_tool("GOOGLE_ANALYTICS_RUN_REPORT", args)
    if error:
        print(f"  ERROR running report (dims={dimensions}): {error}", flush=True)
        return []
    if not data or "rows" not in data:
        print(f"  No rows returned for dims={dimensions}", flush=True)
        return []
    return data["rows"]


def run_ga4_report_paginated(dimensions, metrics, date_ranges, dimension_filter=None, page_size=10000):
    """Call GA4 RunReport with automatic pagination for high-cardinality datasets.
    Loops using offset until fewer rows than page_size are returned."""
    all_rows = []
    offset = 0
    page = 0
    while True:
        args = {
            "property": PROPERTY,
            "dimensions": [{"name": d} for d in dimensions],
            "metrics": [{"name": m} for m in metrics],
            "dateRanges": date_ranges,
            "limit": page_size,
            "offset": offset,
        }
        if dimension_filter:
            args["dimensionFilter"] = dimension_filter
        data, error = run_composio_tool("GOOGLE_ANALYTICS_RUN_REPORT", args)
        if error:
            print(f"  ERROR running report (dims={dimensions}, offset={offset}): {error}", flush=True)
            break
        if not data or "rows" not in data:
            if page == 0:
                print(f"  No rows returned for dims={dimensions}", flush=True)
            break
        rows = data["rows"]
        all_rows.extend(rows)
        page += 1
        if len(rows) < page_size:
            break
        offset += page_size
    return all_rows

def parse_row(row, dim_names, met_names):
    """Parse a GA4 row into a dict."""
    out = {}
    for i, name in enumerate(dim_names):
        out[name] = row["dimensionValues"][i]["value"] if i < len(row.get("dimensionValues", [])) else ""
    for i, name in enumerate(met_names):
        out[name] = row["metricValues"][i]["value"] if i < len(row.get("metricValues", [])) else "0"
    return out

def appdb_upsert(table: str, rows: list, pk_cols: list, batch_size: int = 100):
    """Upsert rows into AppDB via surething CLI. Batches large datasets to avoid
    Argument list too long OS errors when the SQL string exceeds shell limits."""
    if not rows:
        print(f"  No rows to upsert into {table}", flush=True)
        return
    col_names = list(rows[0].keys())
    conflict_cols = ", ".join(pk_cols)
    update_set = ", ".join(
        f"{c} = excluded.{c}" for c in col_names if c not in pk_cols
    )

    total_upserted = 0
    errors = 0
    for batch_start in range(0, len(rows), batch_size):
        batch = rows[batch_start: batch_start + batch_size]
        values_parts = []
        for row in batch:
            vals = []
            for c in col_names:
                v = row.get(c, "")
                if isinstance(v, str):
                    v = v.replace("'", "''")
                    vals.append(f"'{v}'")
                else:
                    vals.append(str(v))
            values_parts.append(f"({', '.join(vals)})")

        sql = (
            f"INSERT INTO {table} ({', '.join(col_names)}) VALUES "
            + ", ".join(values_parts)
            + f" ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}, updated_at = datetime('now')"
        )
        result = subprocess.run(
            ["surething", "appdb", "exec-sql", sql],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            print(f"  DB ERROR for {table} (batch {batch_start}): {result.stderr}", flush=True)
            errors += 1
        else:
            total_upserted += len(batch)

    if errors == 0:
        print(f"  Upserted {total_upserted} rows into {table}", flush=True)
    else:
        print(f"  Upserted {total_upserted} rows into {table} ({errors} batch ERROR(s))", flush=True)

def normalize_date(d: str) -> str:
    """Convert GA4 YYYYMMDD to YYYY-MM-DD."""
    d = str(d).strip()
    if len(d) == 8 and d.isdigit():
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return d


def normalize_date_hour_minute(d: str) -> str:
    """Convert GA4 dateHourMinute (YYYYMMDDHHMM, 12 chars) to YYYY-MM-DD HH:MM."""
    d = str(d).strip()
    if len(d) == 12 and d.isdigit():
        return f"{d[:4]}-{d[4:6]}-{d[6:8]} {d[8:10]}:{d[10:12]}"
    return d

def safe_float(v, default=0.0):
    try:
        return float(v)
    except (ValueError, TypeError):
        return default

def safe_int(v, default=0):
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return default


def sync_ga_daily_metrics(date_ranges):
    print("[ga_daily_metrics]", flush=True)
    dims = ["date"]
    mets = ["sessions", "activeUsers", "newUsers", "screenPageViews",
            "bounceRate", "averageSessionDuration", "engagementRate", "keyEvents"]
    raw_rows = run_ga4_report(dims, mets, date_ranges)
    rows = []
    for r in raw_rows:
        p = parse_row(r, dims, mets)
        rows.append({
            "date": normalize_date(p["date"]),
            "sessions": safe_int(p["sessions"]),
            "active_users": safe_int(p["activeUsers"]),
            "new_users": safe_int(p["newUsers"]),
            "pageviews": safe_int(p["screenPageViews"]),
            "bounce_rate": safe_float(p["bounceRate"]),
            "avg_session_duration": safe_float(p["averageSessionDuration"]),
            "engagement_rate": safe_float(p["engagementRate"]),
            "key_events": safe_int(p["keyEvents"]),
        })
    appdb_upsert("ga_daily_metrics", rows, ["date"])


def sync_ga_channel_metrics(date_ranges):
    print("[ga_channel_metrics]", flush=True)
    dims = ["date", "sessionDefaultChannelGroup"]
    mets = ["sessions", "activeUsers", "newUsers", "keyEvents", "engagementRate"]
    raw_rows = run_ga4_report(dims, mets, date_ranges)
    rows = []
    for r in raw_rows:
        p = parse_row(r, dims, mets)
        rows.append({
            "date": normalize_date(p["date"]),
            "channel": p["sessionDefaultChannelGroup"],
            "sessions": safe_int(p["sessions"]),
            "active_users": safe_int(p["activeUsers"]),
            "new_users": safe_int(p["newUsers"]),
            "key_events": safe_int(p["keyEvents"]),
            "engagement_rate": safe_float(p["engagementRate"]),
        })
    appdb_upsert("ga_channel_metrics", rows, ["date", "channel"])


def sync_ga_source_medium_metrics(date_ranges):
    print("[ga_source_medium_metrics]", flush=True)
    dims = ["date", "sessionSourceMedium"]
    mets = ["sessions", "activeUsers", "keyEvents"]
    raw_rows = run_ga4_report(dims, mets, date_ranges)
    rows = []
    for r in raw_rows:
        p = parse_row(r, dims, mets)
        rows.append({
            "date": normalize_date(p["date"]),
            "source_medium": p["sessionSourceMedium"],
            "sessions": safe_int(p["sessions"]),
            "active_users": safe_int(p["activeUsers"]),
            "key_events": safe_int(p["keyEvents"]),
        })
    appdb_upsert("ga_source_medium_metrics", rows, ["date", "source_medium"])


def sync_ga_landing_page_metrics(date_ranges):
    print("[ga_landing_page_metrics]", flush=True)
    dims = ["date", "landingPage"]
    mets = ["sessions", "activeUsers", "bounceRate", "keyEvents"]
    raw_rows = run_ga4_report(dims, mets, date_ranges)
    rows = []
    for r in raw_rows:
        p = parse_row(r, dims, mets)
        rows.append({
            "date": normalize_date(p["date"]),
            "landing_page": p["landingPage"],
            "sessions": safe_int(p["sessions"]),
            "active_users": safe_int(p["activeUsers"]),
            "bounce_rate": safe_float(p["bounceRate"]),
            "key_events": safe_int(p["keyEvents"]),
        })
    appdb_upsert("ga_landing_page_metrics", rows, ["date", "landing_page"])


def sync_ga_device_metrics(date_ranges):
    print("[ga_device_metrics]", flush=True)
    dims = ["date", "deviceCategory"]
    mets = ["sessions", "activeUsers", "bounceRate", "keyEvents"]
    raw_rows = run_ga4_report(dims, mets, date_ranges)
    rows = []
    for r in raw_rows:
        p = parse_row(r, dims, mets)
        rows.append({
            "date": normalize_date(p["date"]),
            "device_category": p["deviceCategory"],
            "sessions": safe_int(p["sessions"]),
            "active_users": safe_int(p["activeUsers"]),
            "bounce_rate": safe_float(p["bounceRate"]),
            "key_events": safe_int(p["keyEvents"]),
        })
    appdb_upsert("ga_device_metrics", rows, ["date", "device_category"])


def sync_ga_events_by_campaign(date_ranges):
    print("[ga_events_by_campaign]", flush=True)
    dims = ["date", "sessionCampaignName", "eventName"]
    mets = ["eventCount"]
    # Filter to relevant events only
    dim_filter = {
        "filter": {
            "fieldName": "eventName",
            "inListFilter": {
                "values": ["session_start", "form_start", "sign_up", "checkout_initiated", "purchase"]
            }
        }
    }
    raw_rows = run_ga4_report(dims, mets, date_ranges, dimension_filter=dim_filter)
    rows = []
    for r in raw_rows:
        p = parse_row(r, dims, mets)
        rows.append({
            "date": normalize_date(p["date"]),
            "campaign_name": p["sessionCampaignName"],
            "event_name": p["eventName"],
            "event_count": safe_int(p["eventCount"]),
        })
    appdb_upsert("ga_events_by_campaign", rows, ["date", "campaign_name", "event_name"])


def sync_ga_events_by_creative(date_ranges):
    print("[ga_events_by_creative]", flush=True)
    dims = ["date", "sessionCampaignName", "sessionGoogleAdsAdGroupName",
            "sessionGoogleAdsCreativeId", "eventName"]
    mets = ["eventCount"]
    dim_filter = {
        "filter": {
            "fieldName": "eventName",
            "inListFilter": {
                "values": [
                    "session_start", "form_start", "sign_up", "checkout_initiated", "purchase",
                    "landing_cta_clicked", "landing_signup_clicked", "landing_chat_submitted",
                    "landing_nav_clicked", "landing_promo_clicked"
                ]
            }
        }
    }
    raw_rows = run_ga4_report(dims, mets, date_ranges, dimension_filter=dim_filter)
    rows = []
    for r in raw_rows:
        p = parse_row(r, dims, mets)
        rows.append({
            "date": normalize_date(p["date"]),
            "campaign_name": p["sessionCampaignName"],
            "ad_group": p["sessionGoogleAdsAdGroupName"],
            "ad_content": p["sessionGoogleAdsCreativeId"],
            "event_name": p["eventName"],
            "event_count": safe_int(p["eventCount"]),
        })
    appdb_upsert("ga_events_by_creative", rows, ["date", "campaign_name", "ad_group", "ad_content", "event_name"])


def sync_ga_landing_page_events(date_ranges):
    print("[ga_landing_page_events]", flush=True)
    dims = ["date", "landingPage", "eventName"]
    mets = ["eventCount", "activeUsers"]
    dim_filter = {
        "filter": {
            "fieldName": "eventName",
            "inListFilter": {
                "values": [
                    "session_start", "first_visit", "page_view", "scroll",
                    "form_start", "sign_up", "checkout_initiated", "purchase",
                    "user_engagement", "click"
                ]
            }
        }
    }
    raw_rows = run_ga4_report(dims, mets, date_ranges, dimension_filter=dim_filter)
    rows = []
    for r in raw_rows:
        p = parse_row(r, dims, mets)
        rows.append({
            "date": normalize_date(p["date"]),
            "landing_page": p["landingPage"],
            "event_name": p["eventName"],
            "event_count": safe_int(p["eventCount"]),
            "active_users": safe_int(p["activeUsers"]),
        })
    appdb_upsert("ga_landing_page_events", rows, ["date", "landing_page", "event_name"])


def sync_ga_device_platform_metrics(date_ranges):
    print("[ga_device_platform_metrics]", flush=True)
    dims = ["date", "deviceCategory", "platform"]
    mets = ["sessions", "activeUsers", "newUsers", "keyEvents", "bounceRate", "averageSessionDuration"]
    raw_rows = run_ga4_report(dims, mets, date_ranges)
    rows = []
    for r in raw_rows:
        p = parse_row(r, dims, mets)
        rows.append({
            "date": normalize_date(p["date"]),
            "device_category": p["deviceCategory"],
            "platform": p["platform"],
            "sessions": safe_int(p["sessions"]),
            "active_users": safe_int(p["activeUsers"]),
            "new_users": safe_int(p["newUsers"]),
            "key_events": safe_int(p["keyEvents"]),
            "bounce_rate": safe_float(p["bounceRate"]),
            "avg_session_duration": safe_float(p["averageSessionDuration"]),
        })
    appdb_upsert("ga_device_platform_metrics", rows, ["date", "device_category", "platform"])


def sync_ga_user_events(date_ranges):
    """Sync minute-granularity event data for behavior trajectory analysis.

    NOTE: userPseudoId is NOT available for this GA4 property via the Data API
    (confirmed via metadata endpoint — dimension not in property's field catalog).
    Table is implemented at (date × dateHourMinute × eventName × landingPage ×
    source_medium × campaign) granularity, aggregated across users.
    activeUsers metric is included as a proxy for per-bucket unique user count.
    For true per-user tracking, BigQuery export would be required.

    Dimensions: date, dateHourMinute, eventName, landingPage,
                sessionSourceMedium, sessionCampaignName
    Metrics: eventCount, activeUsers
    Filter: landingPage contains /launch-index|/chat-index|/welcome|/login
            AND eventName in the 14 tracked events.
    Uses paginated fetching since minute-level cardinality can exceed 10k rows.
    """
    print("[ga_user_events]", flush=True)
    dims = ["date", "dateHourMinute", "eventName",
            "landingPage", "sessionSourceMedium", "sessionCampaignName"]
    mets = ["eventCount", "activeUsers"]

    # Combined filter: (landing page OR match) AND (event name IN list)
    dim_filter = {
        "andGroup": {
            "expressions": [
                {
                    # Landing page must contain one of the 4 page paths
                    "orGroup": {
                        "expressions": [
                            {"filter": {"fieldName": "landingPage", "stringFilter": {"matchType": "CONTAINS", "value": "/launch-index"}}},
                            {"filter": {"fieldName": "landingPage", "stringFilter": {"matchType": "CONTAINS", "value": "/chat-index"}}},
                            {"filter": {"fieldName": "landingPage", "stringFilter": {"matchType": "CONTAINS", "value": "/welcome"}}},
                            {"filter": {"fieldName": "landingPage", "stringFilter": {"matchType": "CONTAINS", "value": "/login"}}},
                        ]
                    }
                },
                {
                    # Event must be one of the 14 tracked events
                    "filter": {
                        "fieldName": "eventName",
                        "inListFilter": {
                            "values": [
                                "session_start", "page_view", "scroll",
                                "landing_cta_clicked", "landing_signup_clicked",
                                "landing_nav_clicked", "landing_promo_clicked",
                                "landing_resource_clicked", "landing_chat_submitted",
                                "landing_chat_connector_clicked",
                                "sign_up", "checkout_initiated", "purchase", "checkout_canceled"
                            ]
                        }
                    }
                },
            ]
        }
    }

    raw_rows = run_ga4_report_paginated(dims, mets, date_ranges, dimension_filter=dim_filter)
    rows = []
    for r in raw_rows:
        p = parse_row(r, dims, mets)
        rows.append({
            "date": normalize_date(p["date"]),
            "date_hour_minute": normalize_date_hour_minute(p["dateHourMinute"]),
            "event_name": p["eventName"],
            "landing_page": p["landingPage"],
            "source_medium": p["sessionSourceMedium"],
            "campaign": p["sessionCampaignName"],
            "event_count": safe_int(p["eventCount"]),
            "active_users": safe_int(p["activeUsers"]),
        })
    # batch_size=25: ga_user_events rows are wide (8 cols with long string values);
    # 100-row batches exceed the 10,000-char SQL API limit. 25 rows ~5,500 chars safely under.
    appdb_upsert("ga_user_events", rows, ["date", "date_hour_minute", "event_name", "landing_page", "source_medium", "campaign"], batch_size=25)


def main():
    args = sys.argv[1:]
    if len(args) < 2:
        print("Usage: ga4_sync.py <start_date> <end_date>", flush=True)
        print("  Dates: YYYY-MM-DD or 'yesterday' or 'today'", flush=True)
        sys.exit(1)

    start_date = resolve_date(args[0])
    end_date = resolve_date(args[1])
    date_ranges = [{"startDate": start_date, "endDate": end_date}]
    print(f"Syncing GA4 data: {start_date} -> {end_date}", flush=True)

    sync_ga_daily_metrics(date_ranges)
    sync_ga_channel_metrics(date_ranges)
    sync_ga_source_medium_metrics(date_ranges)
    sync_ga_landing_page_metrics(date_ranges)
    sync_ga_device_metrics(date_ranges)
    sync_ga_events_by_campaign(date_ranges)
    sync_ga_events_by_creative(date_ranges)
    sync_ga_device_platform_metrics(date_ranges)
    sync_ga_landing_page_events(date_ranges)
    sync_ga_user_events(date_ranges)

    print("Done.", flush=True)


if __name__ == "__main__":
    main()
