"""
Parse GA4 batch report results and insert into all AppDB tables.
Usage: python parse_and_insert_all.py <results_json_file>
"""
import json
import subprocess
import sys

BATCH_SIZE = 50  # Batch multiple rows per INSERT

def fmt_date(raw):
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    return raw

def esc(v):
    return str(v).replace("'", "''")

def run_sql(sql):
    r = subprocess.run(["surething", "appdb", "exec-sql", sql], capture_output=True, text=True)
    if r.returncode != 0:
        print(f"SQL ERROR: {r.stderr[:200]}\nSQL: {sql[:200]}")
    return r

def get_rows(report):
    return report.get("rows") or []

def mv(row, i):
    return row["metricValues"][i]["value"]

def dv(row, i):
    return row["dimensionValues"][i]["value"]

def batch_insert(table, columns, rows_data, batch_size=BATCH_SIZE):
    """Execute batch INSERT with multiple rows per statement."""
    col_str = ", ".join(columns)
    inserted = 0
    for i in range(0, len(rows_data), batch_size):
        batch = rows_data[i:i+batch_size]
        values_list = [f"({row})" for row in batch]
        values_str = ", ".join(values_list)
        sql = f"INSERT OR REPLACE INTO {table} ({col_str}) VALUES {values_str}"
        run_sql(sql)
        inserted += len(batch)
    return inserted

results_file = sys.argv[1]
with open(results_file) as f:
    data = json.load(f)

# Support both old COMPOSIO_MULTI_EXECUTE_TOOL format and new {batch1, batch2} format
if "data" in data and "results" in data.get("data", {}):
    all_results = data["data"]["results"]
    batch1 = all_results[0]["response"]["data"]["reports"]
    batch2 = all_results[1]["response"]["data"]["reports"]
else:
    # New format: {"batch1": {kind, reports}, "batch2": {kind, reports}}
    batch1 = data["batch1"]["reports"]
    batch2 = data["batch2"]["reports"]

inserted = {}

# ── BATCH 1 ──────────────────────────────────────────────

# 1. ga_daily_metrics
print("1. ga_daily_metrics")
rows_data = []
for row in get_rows(batch1[0]):
    date = fmt_date(dv(row, 0))
    rows_data.append(f"'{date}', {mv(row,0)}, {mv(row,1)}, {mv(row,2)}, {mv(row,3)}, {mv(row,4)}, {mv(row,5)}, {mv(row,6)}, {int(float(mv(row,7)))}")
    print(f"   {date}: sessions={mv(row,0)}")
inserted["ga_daily_metrics"] = batch_insert("ga_daily_metrics", 
    ["date", "sessions", "active_users", "new_users", "pageviews", "bounce_rate", "avg_session_duration", "engagement_rate", "key_events"],
    rows_data)

# 2. ga_channel_metrics
print("2. ga_channel_metrics")
rows_data = []
for row in get_rows(batch1[1]):
    date = fmt_date(dv(row, 0))
    channel = esc(dv(row, 1))
    rows_data.append(f"'{date}', '{channel}', {mv(row,0)}, {mv(row,1)}, {mv(row,2)}, {int(float(mv(row,3)))}, {mv(row,4)}")
inserted["ga_channel_metrics"] = batch_insert("ga_channel_metrics",
    ["date", "channel", "sessions", "active_users", "new_users", "key_events", "engagement_rate"],
    rows_data)

# 3. ga_source_medium_metrics
print("3. ga_source_medium_metrics")
rows_data = []
for row in get_rows(batch1[2]):
    date = fmt_date(dv(row, 0))
    sm = esc(dv(row, 1))
    rows_data.append(f"'{date}', '{sm}', {mv(row,0)}, {mv(row,1)}, {int(float(mv(row,2)))}")
inserted["ga_source_medium_metrics"] = batch_insert("ga_source_medium_metrics",
    ["date", "source_medium", "sessions", "active_users", "key_events"],
    rows_data)

# 4. ga_landing_page_metrics
print("4. ga_landing_page_metrics")
rows_data = []
for row in get_rows(batch1[3]):
    date = fmt_date(dv(row, 0))
    page = esc(dv(row, 1))
    rows_data.append(f"'{date}', '{page}', {mv(row,0)}, {mv(row,1)}, {mv(row,2)}, {int(float(mv(row,3)))}")
inserted["ga_landing_page_metrics"] = batch_insert("ga_landing_page_metrics",
    ["date", "landing_page", "sessions", "active_users", "bounce_rate", "key_events"],
    rows_data)

# 5. ga_device_metrics
print("5. ga_device_metrics")
rows_data = []
for row in get_rows(batch1[4]):
    date = fmt_date(dv(row, 0))
    device = esc(dv(row, 1))
    rows_data.append(f"'{date}', '{device}', {mv(row,0)}, {mv(row,1)}, {mv(row,2)}, {int(float(mv(row,3)))}")
inserted["ga_device_metrics"] = batch_insert("ga_device_metrics",
    ["date", "device_category", "sessions", "active_users", "bounce_rate", "key_events"],
    rows_data)

# ── BATCH 2 ──────────────────────────────────────────────

# 6. ga_campaign_metrics
print("6. ga_campaign_metrics")
rows_data = []
for row in get_rows(batch2[0]):
    date = fmt_date(dv(row, 0))
    campaign = esc(dv(row, 1))
    rows_data.append(f"'{date}', '{campaign}', {mv(row,0)}, {mv(row,1)}, {mv(row,2)}, {mv(row,3)}, {mv(row,4)}, {mv(row,5)}, {mv(row,6)}, {int(float(mv(row,7)))}")
inserted["ga_campaign_metrics"] = batch_insert("ga_campaign_metrics",
    ["date", "campaign_name", "sessions", "active_users", "new_users", "pageviews", "bounce_rate", "avg_session_duration", "engagement_rate", "key_events"],
    rows_data)

# 7. ga_ad_creative_metrics
print("7. ga_ad_creative_metrics")
rows_data = []
for row in get_rows(batch2[1]):
    date = fmt_date(dv(row, 0))
    campaign = esc(dv(row, 1))
    ad_group = esc(dv(row, 2))
    ad_content = esc(dv(row, 3))
    rows_data.append(f"'{date}', '{campaign}', '{ad_group}', '{ad_content}', {mv(row,0)}, {mv(row,1)}, {mv(row,2)}, {mv(row,3)}, {mv(row,4)}, {int(float(mv(row,5)))}")
inserted["ga_ad_creative_metrics"] = batch_insert("ga_ad_creative_metrics",
    ["date", "campaign_name", "ad_group", "ad_content", "sessions", "active_users", "new_users", "bounce_rate", "engagement_rate", "key_events"],
    rows_data)

# 8. ga_events_by_campaign
print("8. ga_events_by_campaign")
rows_data = []
for row in get_rows(batch2[2]):
    date = fmt_date(dv(row, 0))
    campaign = esc(dv(row, 1))
    event = esc(dv(row, 2))
    rows_data.append(f"'{date}', '{campaign}', '{event}', {mv(row,0)}")
inserted["ga_events_by_campaign"] = batch_insert("ga_events_by_campaign",
    ["date", "campaign_name", "event_name", "event_count"],
    rows_data)

# 9. ga_events_by_creative
print("9. ga_events_by_creative")
rows_data = []
for row in get_rows(batch2[3]):
    date = fmt_date(dv(row, 0))
    campaign = esc(dv(row, 1))
    ad_group = esc(dv(row, 2))
    ad_content = esc(dv(row, 3))
    event = esc(dv(row, 4))
    rows_data.append(f"'{date}', '{campaign}', '{ad_group}', '{ad_content}', '{event}', {mv(row,0)}")
inserted["ga_events_by_creative"] = batch_insert("ga_events_by_creative",
    ["date", "campaign_name", "ad_group", "ad_content", "event_name", "event_count"],
    rows_data)

print("\n✅ All tables updated:")
for table, count in inserted.items():
    print(f"   {table}: {count} rows")
