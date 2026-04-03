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


def build_notion_table(videos):
    """Build a Notion table block from video stats."""
    headers = ["#", "视频名称", "YouTube 链接", "时长", "播放量", "点赞", "评论", "互动率", "广告类型", "发布日期"]

    rows = [{"cells": [text_cell(h) for h in headers]}]

    for i, v in enumerate(videos, 1):
        dur = f"{v['duration_s']//60}:{v['duration_s']%60:02d}"
        eng = f"{v['engagement_rate']:.2f}%" if v['engagement_rate'] >= 0.01 else f"{v['engagement_rate']:.4f}%"

        rows.append({"cells": [
            text_cell(str(i)),
            text_cell(v["title"][:80]),
            text_cell(v["url"], link=v["url"]),
            text_cell(dur),
            text_cell(f"{fmt_views(v['views'])}（{v['views']:,}）"),
            text_cell(f"{v['likes']:,}"),
            text_cell(str(v["comments"])),
            text_cell(eng),
            text_cell(v["ad_type"]),
            text_cell(v["published"])
        ]})

    return {
        "type": "table",
        "table": {
            "table_width": len(headers),
            "has_column_header": True,
            "children": rows
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
