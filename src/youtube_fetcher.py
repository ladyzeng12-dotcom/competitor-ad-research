"""
Step 2: YouTube Data API Fetcher
Fetches video details (title, duration, views, likes, etc.) for a list of YouTube video IDs.

Usage:
    python src/youtube_fetcher.py <brand_key> [--config config.json] [--data-dir data/]

Requires:
    - YouTube Data API access (via Composio or direct API key)
    - Input: data/tc_intercept_{brand}.json (from tc_scraper.py)

Output:
    data/video_stats_{brand}.json — full video metadata
"""
import json
import re
import sys
import os
import argparse
from datetime import datetime


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch YouTube video details for discovered ad IDs")
    parser.add_argument("brand", help="Brand key (e.g., 'lovable')")
    parser.add_argument("--data-dir", default="data", help="Data directory")
    parser.add_argument("--input", default=None, help="Override input file path")
    parser.add_argument("--api-key", default=None, help="YouTube Data API key (if not using Composio)")
    return parser.parse_args()


def parse_duration(iso_duration):
    """Convert ISO 8601 duration (PT1M30S) to seconds."""
    m = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', iso_duration or '')
    if not m:
        return 0
    h, mi, s = m.groups(default='0')
    return int(h) * 3600 + int(mi) * 60 + int(s)


def classify_ad_type(duration_s):
    """Classify ad type based on video duration."""
    if duration_s <= 7:
        return {"type": "Bumper", "billing": "CPM", "funnel": "TOFU"}
    elif duration_s <= 16:
        return {"type": "Non-skippable", "billing": "CPM", "funnel": "TOFU/MOFU"}
    elif duration_s <= 60:
        return {"type": "Skippable In-stream", "billing": "CPV", "funnel": "MOFU/BOFU"}
    else:
        return {"type": "Long-form / Discovery", "billing": "CPV/CPC", "funnel": "BOFU"}


def estimate_budget(duration_s, views, budget_config):
    """Estimate ad spend based on ad type and view count."""
    if duration_s <= 16:
        # CPM-based (Bumper / Non-skippable)
        cpm_low, cpm_high = budget_config.get("bumper_cpm_range", [8, 15])
        impressions = views / 1000
        return {"low": round(impressions * cpm_low), "high": round(impressions * cpm_high), "model": "CPM"}
    else:
        # CPV-based (Skippable)
        cpv_low, cpv_high = budget_config.get("skippable_cpv_range", [0.02, 0.06])
        return {"low": round(views * cpv_low), "high": round(views * cpv_high), "model": "CPV"}


def parse_api_response(api_response):
    """
    Parse YouTube Data API response.
    Supports both raw API response and Composio-wrapped response.
    """
    # Try Composio wrapper format
    if isinstance(api_response, dict) and "data" in api_response:
        try:
            items = api_response["data"]["results"][0]["response"]["data"]["items"]
            return items
        except (KeyError, IndexError):
            pass

    # Try direct API format
    if isinstance(api_response, dict) and "items" in api_response:
        return api_response["items"]

    # Try list of items directly
    if isinstance(api_response, list):
        return api_response

    raise ValueError("Unrecognized API response format")


def process_video_items(items, brand_name, budget_config):
    """Process raw API items into structured video records."""
    videos = []
    for item in items:
        vid_id = item.get("id", "")
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        content = item.get("contentDetails", {})
        status = item.get("status", {})

        duration_s = parse_duration(content.get("duration", ""))
        views = int(stats.get("viewCount", 0))
        likes = int(stats.get("likeCount", 0))
        comments = int(stats.get("commentCount", 0))

        ad_info = classify_ad_type(duration_s)
        budget = estimate_budget(duration_s, views, budget_config)

        videos.append({
            "id": vid_id,
            "brand": brand_name,
            "title": snippet.get("title", ""),
            "channel": snippet.get("channelTitle", ""),
            "channel_id": snippet.get("channelId", ""),
            "published": snippet.get("publishedAt", "")[:10],
            "duration_s": duration_s,
            "views": views,
            "likes": likes,
            "comments": comments,
            "engagement_rate": round((likes + comments) / views * 100, 4) if views > 0 else 0,
            "privacy": status.get("privacyStatus", ""),
            "ad_type": ad_info["type"],
            "billing_model": ad_info["billing"],
            "funnel_position": ad_info["funnel"],
            "estimated_spend": budget,
            "url": f"https://www.youtube.com/watch?v={vid_id}"
        })

    # Sort by views descending
    videos.sort(key=lambda v: v["views"], reverse=True)
    return videos


def print_summary(videos, brand_name):
    """Print a summary table to stdout."""
    total_views = sum(v["views"] for v in videos)
    total_spend_low = sum(v["estimated_spend"]["low"] for v in videos)
    total_spend_high = sum(v["estimated_spend"]["high"] for v in videos)

    print(f"\n{'='*60}")
    print(f"{brand_name}: {len(videos)} videos | {total_views:,} total views")
    print(f"Estimated total spend: ${total_spend_low:,} - ${total_spend_high:,}")
    print(f"{'='*60}")
    print(f"{'#':>2} | {'Title':50} | {'Dur':>5} | {'Views':>10} | {'Type':15}")
    print("-" * 100)
    for i, v in enumerate(videos[:20], 1):
        dur = f"{v['duration_s']//60}:{v['duration_s']%60:02d}"
        views_fmt = f"{v['views']/1e6:.1f}M" if v['views'] >= 1e6 else f"{v['views']/1e3:.1f}K" if v['views'] >= 1e3 else str(v['views'])
        print(f"{i:2} | {v['title'][:50]:50} | {dur:>5} | {views_fmt:>10} | {v['ad_type']:15}")
    if len(videos) > 20:
        print(f"  ... and {len(videos) - 20} more")


if __name__ == "__main__":
    args = parse_args()

    # Load intercept data
    input_path = args.input or os.path.join(args.data_dir, f"tc_intercept_{args.brand}.json")
    if not os.path.exists(input_path):
        print(f"Error: Input file not found: {input_path}")
        print("Run tc_scraper.py first to extract YouTube IDs from TC.")
        sys.exit(1)

    with open(input_path) as f:
        intercept_data = json.load(f)

    youtube_ids = intercept_data.get("youtube_ids", [])
    brand_name = intercept_data.get("brand_name", args.brand)

    if not youtube_ids:
        print(f"No YouTube IDs found for {brand_name}. Nothing to fetch.")
        sys.exit(0)

    print(f"Found {len(youtube_ids)} YouTube IDs for {brand_name}")
    print(f"IDs: {', '.join(youtube_ids[:10])}{'...' if len(youtube_ids) > 10 else ''}")
    print(f"\nTo fetch video details, call YouTube Data API with these IDs.")
    print(f"Example Composio tool: YOUTUBE_GET_VIDEO_DETAILS_BATCH")
    print(f"  video_ids: {','.join(youtube_ids)}")
    print(f"\nAfter fetching, save API response to: {args.data_dir}/api_response_{args.brand}.json")
    print(f"Then run: python src/youtube_fetcher.py {args.brand} --input {args.data_dir}/api_response_{args.brand}.json")
