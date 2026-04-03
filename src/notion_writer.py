"""
Step 4: Notion Writer
Writes video stats as a formatted table to a Notion page (one page per competitor).

Usage:
    python src/notion_writer.py <brand_key> [--data-dir data/] [--parent-page-id <id>]

Input:
    data/video_stats_{brand}.json (from youtube_fetcher.py)

Output:
    Creates/updates a Notion page under the specified parent page.

Requires:
    Notion API connection (via Composio or direct API token)
"""
import json
import os
import sys
import argparse
from datetime import datetime


def parse_args():
    parser = argparse.ArgumentParser(description="Write video stats to Notion")
    parser.add_argument("brand", help="Brand key (e.g., 'lovable')")
    parser.add_argument("--data-dir", default="data", help="Data directory")
    parser.add_argument("--parent-page-id", default=None, help="Notion parent page ID")
    parser.add_argument("--config", default="config.json", help="Path to config.json")
    return parser.parse_args()


def fmt_views(n):
    if n >= 10_000_000:
        return f"{n/10000:.0f}万"
    if n >= 1_000_000:
        return f"{n/10000:.1f}万"
    if n >= 1_000:
        return f"{n/1000:.1f}K"
    return str(n)


def text_cell(text, link=None):
    """Build a Notion rich text cell."""
    c = {"type": "text", "text": {"content": str(text)}}
    if link:
        c["text"]["link"] = {"url": link}
    return [c]


def fmt_money(v):
    """Format spend estimate."""
    if v >= 1_000_000: return f"${v/1_000_000:.1f}M"
    if v >= 1_000: return f"${v/1000:.1f}K"
    return f"${v:.0f}"


def bold_cell(text):
    """Build a bold Notion rich text cell."""
    return [{"type": "text", "text": {"content": str(text)}, "annotations": {"bold": True}}]


def build_notion_table(videos):
    """Build a Notion table block from video stats.
    
    REQUIRED columns (do NOT omit any):
      #, 视频标题（点击跳转）, 发布日期, 时长, 广告类型, 播放量, 点赞, 互动率, 估算花费
    
    Video title MUST be a clickable YouTube link.
    """
    headers = ["#", "视频标题（点击跳转）", "发布日期", "时长", "广告类型", "播放量", "点赞", "互动率", "估算花费"]

    header_row = {"type": "table_row", "table_row": {"cells": [bold_cell(h) for h in headers]}}
    data_rows = []

    for i, v in enumerate(videos, 1):
        dur = v.get("duration") or f"{v['duration_s']//60}:{v['duration_s']%60:02d}"
        eng = f"{v.get('engagement_rate', v.get('engagement', 0)):.2f}%"
        spend_low = v.get("spend_low", 0)
        spend_high = v.get("spend_high", 0)
        spend_str = f"{fmt_money(spend_low)}~{fmt_money(spend_high)}"

        data_rows.append({"type": "table_row", "table_row": {"cells": [
            text_cell(str(i)),
            text_cell(v["title"][:60], link=v.get("url")),  # clickable title
            text_cell(v.get("published", "?")),
            text_cell(dur),
            text_cell(v.get("ad_type", "?")),
            text_cell(f"{v['views']:,}"),
            text_cell(f"{v.get('likes', 0):,}"),
            text_cell(eng),
            text_cell(spend_str),
        ]}})

    return {
        "type": "table",
        "table": {
            "table_width": len(headers),
            "has_column_header": True,
            "children": [header_row] + data_rows
        }
    }


def build_page_title(brand_name):
    today = datetime.utcnow().strftime("%Y-%m-%d")
    return f"{brand_name} YouTube 广告数据 {today}"


if __name__ == "__main__":
    args = parse_args()

    input_path = os.path.join(args.data_dir, f"video_stats_{args.brand}.json")
    if not os.path.exists(input_path):
        print(f"Error: {input_path} not found. Run youtube_fetcher.py first.")
        sys.exit(1)

    with open(input_path) as f:
        videos = json.load(f)

    brand_name = videos[0]["brand"] if videos else args.brand
    table = build_notion_table(videos)
    title = build_page_title(brand_name)

    # Output the Notion API payload for the orchestrator to execute
    output = {
        "page_title": title,
        "brand": args.brand,
        "brand_name": brand_name,
        "parent_page_id": args.parent_page_id,
        "table_block": table,
        "video_count": len(videos),
        "total_views": sum(v["views"] for v in videos)
    }

    output_path = os.path.join(args.data_dir, f"notion_payload_{args.brand}.json")
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"Notion payload saved to {output_path}")
    print(f"Page title: {title}")
    print(f"Videos: {len(videos)} | Total views: {sum(v['views'] for v in videos):,}")
    print(f"\nTo create Notion page, use Composio NOTION_CREATE_A_NEW_PAGE with:")
    print(f"  parent_page_id: {args.parent_page_id or '(set in config.json)'}")
    print(f"  title: {title}")
    print(f"  children: [table_block from {output_path}]")
