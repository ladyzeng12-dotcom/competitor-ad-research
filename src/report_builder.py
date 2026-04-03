"""
Step 3: Report Builder
Generates Markdown reports from video stats, one per competitor.

Usage:
    python src/report_builder.py <brand_key> [--data-dir data/]

Input:
    data/video_stats_{brand}.json (from youtube_fetcher.py)

Output:
    data/report_{brand}.md — formatted Markdown report
"""
import json
import os
import sys
import argparse
from datetime import datetime, timedelta


def parse_args():
    parser = argparse.ArgumentParser(description="Build Markdown report from video stats")
    parser.add_argument("brand", help="Brand key (e.g., 'lovable')")
    parser.add_argument("--data-dir", default="data", help="Data directory")
    parser.add_argument("--lookback-days", type=int, default=30, help="Days for 'recent' section")
    return parser.parse_args()


def fmt_views(n):
    """Format view count for display."""
    if n >= 100_000_000:
        return f"{n/100_000_000:.2f}亿"
    elif n >= 10_000:
        return f"{n/10_000:.0f}万"
    else:
        return f"{n:,}"


def build_report(videos, brand_name, lookback_days=30):
    """Build a Markdown report for one competitor."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    cutoff = (datetime.utcnow() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    total_views = sum(v["views"] for v in videos)
    total_likes = sum(v["likes"] for v in videos)
    total_spend_low = sum(v["estimated_spend"]["low"] for v in videos)
    total_spend_high = sum(v["estimated_spend"]["high"] for v in videos)

    # Count by ad type
    type_counts = {}
    for v in videos:
        t = v["ad_type"]
        type_counts[t] = type_counts.get(t, 0) + 1

    lines = []
    lines.append(f"# {brand_name} YouTube 广告投放数据")
    lines.append(f"> 数据拉取时间: {today} | 来源: YouTube Data API + Google Ads Transparency Center\n")

    # Summary
    lines.append("## 概览\n")
    lines.append(f"| 指标 | 数据 |")
    lines.append(f"|------|------|")
    lines.append(f"| 视频广告总数 | {len(videos)} |")
    lines.append(f"| 总播放量 | {fmt_views(total_views)} |")
    lines.append(f"| 总点赞 | {total_likes:,} |")
    lines.append(f"| 估算总投放预算 | ${total_spend_low:,} - ${total_spend_high:,} |")
    for t, c in sorted(type_counts.items()):
        lines.append(f"| {t} 广告数 | {c} |")
    lines.append("")

    # Full video list
    lines.append("## 视频列表（按播放量排序）\n")
    lines.append("| # | 视频 | 时长 | 播放量 | 点赞 | 互动率 | 广告类型 | 发布日期 |")
    lines.append("|---|------|------|--------|------|--------|----------|----------|")

    for i, v in enumerate(videos, 1):
        title = v["title"].replace("|", "\\|")[:60]
        vid_id = v["id"]
        link = f"[{title}](https://www.youtube.com/watch?v={vid_id})"
        dur = f"{v['duration_s']//60}:{v['duration_s']%60:02d}"
        eng = f"{v['engagement_rate']:.2f}%" if v['engagement_rate'] >= 0.01 else f"{v['engagement_rate']:.4f}%"
        lines.append(f"| {i} | {link} | {dur} | {fmt_views(v['views'])} | {v['likes']:,} | {eng} | {v['ad_type']} | {v['published']} |")

    # Recent activity section
    recent = [v for v in videos if v["published"] >= cutoff]
    if recent:
        lines.append(f"\n## 近{lookback_days}天新发布 ({len(recent)} 个)\n")
        lines.append("| # | 视频 | 时长 | 播放量 | 广告类型 | 发布日期 |")
        lines.append("|---|------|------|--------|----------|----------|")
        for i, v in enumerate(sorted(recent, key=lambda x: x["published"], reverse=True), 1):
            title = v["title"].replace("|", "\\|")[:60]
            link = f"[{title}](https://www.youtube.com/watch?v={v['id']})"
            dur = f"{v['duration_s']//60}:{v['duration_s']%60:02d}"
            lines.append(f"| {i} | {link} | {dur} | {fmt_views(v['views'])} | {v['ad_type']} | {v['published']} |")

    # Budget breakdown
    lines.append("\n## 预算估算\n")
    lines.append("| 广告类型 | 数量 | 总播放量 | 估算花费 |")
    lines.append("|----------|------|----------|----------|")
    for ad_type in sorted(type_counts.keys()):
        type_videos = [v for v in videos if v["ad_type"] == ad_type]
        type_views = sum(v["views"] for v in type_videos)
        type_low = sum(v["estimated_spend"]["low"] for v in type_videos)
        type_high = sum(v["estimated_spend"]["high"] for v in type_videos)
        lines.append(f"| {ad_type} | {len(type_videos)} | {fmt_views(type_views)} | ${type_low:,} - ${type_high:,} |")
    lines.append(f"| **合计** | **{len(videos)}** | **{fmt_views(total_views)}** | **${total_spend_low:,} - ${total_spend_high:,}** |")

    return "\n".join(lines)


if __name__ == "__main__":
    args = parse_args()

    input_path = os.path.join(args.data_dir, f"video_stats_{args.brand}.json")
    if not os.path.exists(input_path):
        print(f"Error: {input_path} not found. Run youtube_fetcher.py first.")
        sys.exit(1)

    with open(input_path) as f:
        videos = json.load(f)

    brand_name = videos[0]["brand"] if videos else args.brand
    report = build_report(videos, brand_name, args.lookback_days)

    output_path = os.path.join(args.data_dir, f"report_{args.brand}.md")
    with open(output_path, "w") as f:
        f.write(report)

    print(f"Report saved to {output_path}")
    print(report[:2000])
