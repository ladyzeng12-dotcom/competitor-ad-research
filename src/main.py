"""
Competitor YouTube Ad Research — Main Pipeline

End-to-end orchestration: TC scraping → YouTube API → classification → report → Notion.

Usage:
    python src/main.py [brand ...] [--config config.json] [--skip-notion]

Examples:
    python src/main.py lovable manus            # Run for specific brands
    python src/main.py --all                     # Run for all brands in config
    python src/main.py lovable --skip-notion     # Run without Notion output
"""
import asyncio
import json
import os
import sys
import argparse
from datetime import datetime

from tc_scraper import intercept_tc_ads, load_config
from youtube_fetcher import process_video_items, print_summary
from report_builder import build_report
from notion_writer import build_notion_table, build_page_title


def parse_args():
    parser = argparse.ArgumentParser(description="Run competitor YouTube ad research pipeline")
    parser.add_argument("brands", nargs="*", help="Brand keys to analyze (from config.json)")
    parser.add_argument("--all", action="store_true", help="Run for all brands in config")
    parser.add_argument("--config", default="config.json", help="Config file path")
    parser.add_argument("--data-dir", default="data", help="Output data directory")
    parser.add_argument("--skip-notion", action="store_true", help="Skip Notion output")
    parser.add_argument("--skip-scrape", action="store_true", help="Skip TC scraping (use existing data)")
    return parser.parse_args()


async def run_pipeline(brands, config, data_dir, skip_notion=False, skip_scrape=False):
    """Run the full pipeline for given brands."""
    os.makedirs(data_dir, exist_ok=True)
    budget_config = config.get("ad_classification", {}).get("budget_estimates", {})
    results = {}

    for brand in brands:
        print(f"\n{'='*60}")
        print(f"  Processing: {brand}")
        print(f"{'='*60}")

        comp = config["competitors"].get(brand)
        if not comp:
            print(f"  ⚠ Brand '{brand}' not in config, skipping")
            continue

        # Step 1: TC Scraping
        intercept_path = os.path.join(data_dir, f"tc_intercept_{brand}.json")
        if skip_scrape and os.path.exists(intercept_path):
            print(f"\n[Step 1] Using existing TC data: {intercept_path}")
            with open(intercept_path) as f:
                tc_data = json.load(f)
        else:
            print(f"\n[Step 1] Scraping Google Ads Transparency Center...")
            tc_data = await intercept_tc_ads(brand, config, data_dir)

        youtube_ids = tc_data.get("youtube_ids", [])
        if not youtube_ids:
            print(f"  ⚠ No YouTube IDs found for {comp['name']}, skipping")
            results[brand] = {"status": "no_ads_found", "youtube_ids": 0}
            continue

        print(f"\n[Step 2] YouTube IDs ready for API fetch: {len(youtube_ids)}")
        print(f"  IDs: {', '.join(youtube_ids[:5])}{'...' if len(youtube_ids) > 5 else ''}")

        # Step 2: YouTube API call
        # This step requires external API call (Composio or direct).
        # The orchestrator should:
        #   1. Call YOUTUBE_GET_VIDEO_DETAILS_BATCH with the IDs
        #   2. Save response to data/api_response_{brand}.json
        #   3. Continue pipeline from step 3

        api_response_path = os.path.join(data_dir, f"api_response_{brand}.json")
        if os.path.exists(api_response_path):
            print(f"  Found cached API response: {api_response_path}")
            with open(api_response_path) as f:
                api_response = json.load(f)

            # Step 3: Process & classify
            print(f"\n[Step 3] Processing video data & classifying ad types...")
            from youtube_fetcher import parse_api_response
            items = parse_api_response(api_response)
            videos = process_video_items(items, comp["name"], budget_config)

            # Save processed stats
            stats_path = os.path.join(data_dir, f"video_stats_{brand}.json")
            with open(stats_path, "w") as f:
                json.dump(videos, f, indent=2, ensure_ascii=False)

            print_summary(videos, comp["name"])

            # Step 4: Build report
            print(f"\n[Step 4] Building report...")
            lookback = config.get("settings", {}).get("lookback_days", 30)
            report = build_report(videos, comp["name"], lookback)
            report_path = os.path.join(data_dir, f"report_{brand}.md")
            with open(report_path, "w") as f:
                f.write(report)
            print(f"  Report saved: {report_path}")

            # Step 5: Notion output
            if not skip_notion:
                print(f"\n[Step 5] Preparing Notion payload...")
                table = build_notion_table(videos)
                title = build_page_title(comp["name"])
                payload = {
                    "page_title": title,
                    "brand": brand,
                    "brand_name": comp["name"],
                    "table_block": table,
                    "video_count": len(videos),
                    "total_views": sum(v["views"] for v in videos)
                }
                payload_path = os.path.join(data_dir, f"notion_payload_{brand}.json")
                with open(payload_path, "w") as f:
                    json.dump(payload, f, indent=2, ensure_ascii=False)
                print(f"  Notion payload saved: {payload_path}")
            else:
                print(f"\n[Step 5] Notion output skipped (--skip-notion)")

            results[brand] = {
                "status": "complete",
                "youtube_ids": len(youtube_ids),
                "videos_processed": len(videos),
                "total_views": sum(v["views"] for v in videos)
            }
        else:
            print(f"\n  ⏸ Waiting for YouTube API response.")
            print(f"  Save API response to: {api_response_path}")
            print(f"  Then re-run with --skip-scrape to continue.")
            results[brand] = {
                "status": "awaiting_api",
                "youtube_ids": len(youtube_ids),
                "ids_file": intercept_path
            }

    # Summary
    print(f"\n{'='*60}")
    print(f"  Pipeline Summary")
    print(f"{'='*60}")
    for brand, r in results.items():
        print(f"  {brand}: {r['status']} | {r.get('videos_processed', r.get('youtube_ids', 0))} videos")

    return results


if __name__ == "__main__":
    args = parse_args()
    config = load_config(args.config)

    if args.all:
        brands = list(config["competitors"].keys())
    elif args.brands:
        brands = args.brands
    else:
        print("Specify brands or use --all. Available:", list(config["competitors"].keys()))
        sys.exit(1)

    asyncio.run(run_pipeline(brands, config, args.data_dir, args.skip_notion, args.skip_scrape))
