"""
Parse GA4 batch report results and insert into all AppDB tables.
Usage: python parse_and_insert_all.py <results_json_file>
"""
import json
import subprocess
import sys

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

results_file = sys.argv[1]
with open(results_file) as f:
    data = json.load(f)

all_results = data["data"]["results"]
batch1 = all_results[0]["response"]["data"]["reports"]
batch2 = all_results[1]["response"]["data"]["reports"]

inserted = {}

# ── BATCH 1 ──────────────────────────────────────────────

# 1. ga_daily_metrics
print("1. ga_daily_metrics")
for row in get_rows(batch1[0]):
    date = fmt_date(dv(row, 0))
    sql = f"""INSERT OR REPLACE INTO ga_daily_metrics
        (date, sessions, active_users, new_users, pageviews, bounce_rate, avg_session_duration, engagement_rate, key_events)
        VALUES ('{date}', {mv(row,0)}, {mv(row,1)}, {mv(row,2)}, {mv(row,3)}, {mv(row,4)}, {mv(row,5)}, {mv(row,6)}, {int(float(mv(row,7)))})"""
    run_sql(sql)
    print(f"   {date}: sessions={mv(row,0)}")
inserted["ga_daily_metrics"] = len(get_rows(batch1[0]))

# 2. ga_channel_metrics
print("2. ga_channel_metrics")
c = 0
for row in get_rows(batch1[1]):
    date = fmt_date(dv(row, 0))
    channel = esc(dv(row, 1))
    sql = f"""INSERT OR REPLACE INTO ga_channel_metrics
        (date, channel, sessions, active_users, new_users, key_events, engagement_rate)
        VALUES ('{date}', '{channel}', {mv(row,0)}, {mv(row,1)}, {mv(row,2)}, {int(float(mv(row,3)))}, {mv(row,4)})"""
    run_sql(sql); c += 1
print(f"   {c} rows")
inserted["ga_channel_metrics"] = c

# 3. ga_source_medium_metrics
print("3. ga_source_medium_metrics")
c = 0
for row in get_rows(batch1[2]):
    date = fmt_date(dv(row, 0))
    sm = esc(dv(row, 1))
    sql = f"""INSERT OR REPLACE INTO ga_source_medium_metrics
        (date, source_medium, sessions, active_users, key_events)
        VALUES ('{date}', '{sm}', {mv(row,0)}, {mv(row,1)}, {int(float(mv(row,2)))})"""
    run_sql(sql); c += 1
print(f"   {c} rows")
inserted["ga_source_medium_metrics"] = c

# 4. ga_landing_page_metrics
print("4. ga_landing_page_metrics")
c = 0
for row in get_rows(batch1[3]):
    date = fmt_date(dv(row, 0))
    page = esc(dv(row, 1))
    sql = f"""INSERT OR REPLACE INTO ga_landing_page_metrics
        (date, landing_page, sessions, active_users, bounce_rate, key_events)
        VALUES ('{date}', '{page}', {mv(row,0)}, {mv(row,1)}, {mv(row,2)}, {int(float(mv(row,3)))})"""
    run_sql(sql); c += 1
print(f"   {c} rows")
inserted["ga_landing_page_metrics"] = c

# 5. ga_device_metrics
print("5. ga_device_metrics")
c = 0
for row in get_rows(batch1[4]):
    date = fmt_date(dv(row, 0))
    device = esc(dv(row, 1))
    sql = f"""INSERT OR REPLACE INTO ga_device_metrics
        (date, device_category, sessions, active_users, bounce_rate, key_events)
        VALUES ('{date}', '{device}', {mv(row,0)}, {mv(row,1)}, {mv(row,2)}, {int(float(mv(row,3)))})"""
    run_sql(sql); c += 1
print(f"   {c} rows")
inserted["ga_device_metrics"] = c

# ── BATCH 2 ──────────────────────────────────────────────

# 6. ga_campaign_metrics
print("6. ga_campaign_metrics")
c = 0
for row in get_rows(batch2[0]):
    date = fmt_date(dv(row, 0))
    campaign = esc(dv(row, 1))
    sql = f"""INSERT OR REPLACE INTO ga_campaign_metrics
        (date, campaign_name, sessions, active_users, new_users, pageviews, bounce_rate, avg_session_duration, engagement_rate, key_events)
        VALUES ('{date}', '{campaign}', {mv(row,0)}, {mv(row,1)}, {mv(row,2)}, {mv(row,3)}, {mv(row,4)}, {mv(row,5)}, {mv(row,6)}, {int(float(mv(row,7)))})"""
    run_sql(sql); c += 1
print(f"   {c} rows")
inserted["ga_campaign_metrics"] = c

# 7. ga_ad_creative_metrics
print("7. ga_ad_creative_metrics")
c = 0
for row in get_rows(batch2[1]):
    date = fmt_date(dv(row, 0))
    campaign = esc(dv(row, 1))
    ad_group = esc(dv(row, 2))
    ad_content = esc(dv(row, 3))
    sql = f"""INSERT OR REPLACE INTO ga_ad_creative_metrics
        (date, campaign_name, ad_group, ad_content, sessions, active_users, new_users, bounce_rate, engagement_rate, key_events)
        VALUES ('{date}', '{campaign}', '{ad_group}', '{ad_content}', {mv(row,0)}, {mv(row,1)}, {mv(row,2)}, {mv(row,3)}, {mv(row,4)}, {int(float(mv(row,5)))})"""
    run_sql(sql); c += 1
print(f"   {c} rows")
inserted["ga_ad_creative_metrics"] = c

# 8. ga_events_by_campaign
print("8. ga_events_by_campaign")
c = 0
for row in get_rows(batch2[2]):
    date = fmt_date(dv(row, 0))
    campaign = esc(dv(row, 1))
    event = esc(dv(row, 2))
    sql = f"""INSERT OR REPLACE INTO ga_events_by_campaign
        (date, campaign_name, event_name, event_count)
        VALUES ('{date}', '{campaign}', '{event}', {mv(row,0)})"""
    run_sql(sql); c += 1
print(f"   {c} rows")
inserted["ga_events_by_campaign"] = c

# 9. ga_events_by_creative
print("9. ga_events_by_creative")
c = 0
for row in get_rows(batch2[3]):
    date = fmt_date(dv(row, 0))
    campaign = esc(dv(row, 1))
    ad_group = esc(dv(row, 2))
    ad_content = esc(dv(row, 3))
    event = esc(dv(row, 4))
    sql = f"""INSERT OR REPLACE INTO ga_events_by_creative
        (date, campaign_name, ad_group, ad_content, event_name, event_count)
        VALUES ('{date}', '{campaign}', '{ad_group}', '{ad_content}', '{event}', {mv(row,0)})"""
    run_sql(sql); c += 1
print(f"   {c} rows")
inserted["ga_events_by_creative"] = c

print("\n✅ All tables updated:")
for table, count in inserted.items():
    print(f"   {table}: {count} rows")
