# Purpose:    Fetch GA4 batch reports via Composio and save to /tmp/ga4_batch_results.json
# Stale when: GA4 property changes or report dimensions/metrics are updated

import json
import sys

START_DATE = sys.argv[1]  # e.g. 2026-06-04
END_DATE   = sys.argv[2]  # e.g. 2026-06-06
PROPERTY   = "properties/531750988"
OUT_PATH   = "/tmp/ga4_batch_results.json"

batch1, err1 = run_composio_tool("GOOGLE_ANALYTICS_BATCH_RUN_REPORTS", {
    "property": PROPERTY,
    "requests": [
        {
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [{"name": "date"}],
            "metrics": [
                {"name": "sessions"}, {"name": "activeUsers"}, {"name": "newUsers"},
                {"name": "screenPageViews"}, {"name": "bounceRate"},
                {"name": "averageSessionDuration"}, {"name": "engagementRate"}, {"name": "keyEvents"}
            ],
            "orderBys": [{"desc": False, "dimension": {"dimensionName": "date"}}]
        },
        {
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [{"name": "date"}, {"name": "sessionDefaultChannelGroup"}],
            "metrics": [
                {"name": "sessions"}, {"name": "activeUsers"}, {"name": "newUsers"},
                {"name": "keyEvents"}, {"name": "engagementRate"}
            ],
            "orderBys": [{"desc": False, "dimension": {"dimensionName": "date"}}]
        },
        {
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [{"name": "date"}, {"name": "sessionSourceMedium"}],
            "limit": 100,
            "metrics": [{"name": "sessions"}, {"name": "activeUsers"}, {"name": "keyEvents"}],
            "orderBys": [{"desc": True, "metric": {"metricName": "sessions"}}]
        },
        {
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [{"name": "date"}, {"name": "landingPage"}],
            "limit": 100,
            "metrics": [
                {"name": "sessions"}, {"name": "activeUsers"},
                {"name": "bounceRate"}, {"name": "keyEvents"}
            ],
            "orderBys": [{"desc": True, "metric": {"metricName": "sessions"}}]
        },
        {
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [{"name": "date"}, {"name": "deviceCategory"}],
            "metrics": [
                {"name": "sessions"}, {"name": "activeUsers"},
                {"name": "bounceRate"}, {"name": "keyEvents"}
            ],
            "orderBys": [{"desc": False, "dimension": {"dimensionName": "date"}}]
        }
    ]
})

if err1:
    print(f"BATCH1_ERROR: {err1}", file=sys.stderr)
    sys.exit(1)

batch2, err2 = run_composio_tool("GOOGLE_ANALYTICS_BATCH_RUN_REPORTS", {
    "property": PROPERTY,
    "requests": [
        {
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [{"name": "date"}, {"name": "sessionCampaignName"}],
            "limit": 100,
            "metrics": [
                {"name": "sessions"}, {"name": "activeUsers"}, {"name": "newUsers"},
                {"name": "screenPageViews"}, {"name": "bounceRate"},
                {"name": "averageSessionDuration"}, {"name": "engagementRate"}, {"name": "keyEvents"}
            ],
            "orderBys": [{"desc": True, "metric": {"metricName": "sessions"}}]
        },
        {
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensions": [
                {"name": "date"}, {"name": "sessionCampaignName"},
                {"name": "sessionGoogleAdsAdGroupName"}, {"name": "sessionGoogleAdsCreativeId"}
            ],
            "limit": 100,
            "metrics": [
                {"name": "sessions"}, {"name": "activeUsers"}, {"name": "newUsers"},
                {"name": "bounceRate"}, {"name": "engagementRate"}, {"name": "keyEvents"}
            ],
            "orderBys": [{"desc": True, "metric": {"metricName": "sessions"}}]
        },
        {
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensionFilter": {"filter": {"fieldName": "eventName", "inListFilter": {
                "values": ["session_start", "form_start", "sign_up", "checkout_initiated", "purchase"]
            }}},
            "dimensions": [{"name": "date"}, {"name": "sessionCampaignName"}, {"name": "eventName"}],
            "limit": 100,
            "metrics": [{"name": "eventCount"}],
            "orderBys": [{"desc": True, "metric": {"metricName": "eventCount"}}]
        },
        {
            "dateRanges": [{"startDate": START_DATE, "endDate": END_DATE}],
            "dimensionFilter": {"filter": {"fieldName": "eventName", "inListFilter": {
                "values": ["session_start", "form_start", "sign_up", "checkout_initiated", "purchase"]
            }}},
            "dimensions": [
                {"name": "date"}, {"name": "sessionCampaignName"},
                {"name": "sessionGoogleAdsAdGroupName"}, {"name": "sessionGoogleAdsCreativeId"},
                {"name": "eventName"}
            ],
            "limit": 100,
            "metrics": [{"name": "eventCount"}],
            "orderBys": [{"desc": True, "metric": {"metricName": "eventCount"}}]
        }
    ]
})

if err2:
    print(f"BATCH2_ERROR: {err2}", file=sys.stderr)
    sys.exit(1)

combined = {"batch1": batch1, "batch2": batch2}
with open(OUT_PATH, "w") as f:
    json.dump(combined, f)

print(json.dumps({"status": "ok", "output": OUT_PATH}))
