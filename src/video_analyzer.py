"""
Step 2.5: Per-Video Analysis Generator
Generates contextual analysis for each video ad based on ad type, metrics, and cross-video comparison.

Usage:
    Called by report_builder.py and notion_writer.py after youtube_fetcher.py produces video stats.

Analysis dimensions:
    1. 效果判读 — how to interpret metrics given the ad type (CPM vs CPV logic)
    2. 投放策略 — what the advertiser is likely optimizing for
    3. 参考价值 — what's worth learning for our own campaigns
"""


def fmt_money(v):
    if v >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    if v >= 1_000:
        return f"${v/1_000:.1f}K"
    return f"${v:.0f}"


def analyze_video(video, all_videos):
    """
    Generate a structured analysis for one video in context of all videos.
    
    Returns dict with:
        - metric_interpretation: str — 数据怎么读
        - strategy_insight: str — 投放策略判断
        - reference_value: str — 对我们的参考价值
        - verdict: str — 一句话总结 (🟢好/🟡中/🔴差)
    """
    ad_type = video["ad_type"]
    views = video["views"]
    likes = video["likes"]
    comments = video.get("comments", 0)
    engagement = video.get("engagement_rate", 0)
    duration_s = video["duration_s"]
    spend_low = video["estimated_spend"]["low"]
    spend_high = video["estimated_spend"]["high"]
    billing = video.get("billing_model", "")
    funnel = video.get("funnel_position", "")
    published = video.get("published", "")

    # Cross-video context
    total_views = sum(v["views"] for v in all_videos)
    avg_views = total_views / len(all_videos) if all_videos else 0
    max_views = max(v["views"] for v in all_videos) if all_videos else 0
    same_type = [v for v in all_videos if v["ad_type"] == ad_type]
    avg_views_same_type = sum(v["views"] for v in same_type) / len(same_type) if same_type else 0
    view_share = (views / total_views * 100) if total_views > 0 else 0

    # Engagement context for same type
    avg_eng_same_type = sum(v.get("engagement_rate", 0) for v in same_type) / len(same_type) if same_type else 0

    analysis = {}

    # ===== 1. Metric Interpretation =====
    if ad_type in ("Bumper", "Non-skippable"):
        # CPM ads — engagement is meaningless, focus on reach
        cpm_mid = (spend_low + spend_high) / 2 / (views / 1000) if views > 0 else 0
        metric_lines = []
        metric_lines.append(f"CPM 广告（{ad_type}），强制展示不可跳过，互动率低是正常的。")
        if engagement < 0.01:
            metric_lines.append(f"互动率 {engagement:.4f}% — 符合该广告类型预期，不代表效果差。")
        else:
            metric_lines.append(f"互动率 {engagement:.2f}% — 对于强制广告来说偏高，说明创意有一定吸引力。")
        metric_lines.append(f"核心指标是触达量：{views:,} 次展示，占该品牌总播放量的 {view_share:.1f}%。")
        metric_lines.append(f"实际 CPM 约 ${cpm_mid:.1f}（行业基准 $8-15）。")
        analysis["metric_interpretation"] = " ".join(metric_lines)

    elif ad_type == "Skippable In-stream":
        # CPV ads — engagement matters more
        cpv_mid = (spend_low + spend_high) / 2 / views if views > 0 else 0
        metric_lines = []
        metric_lines.append(f"CPV 广告（可跳过），用户主动选择观看才计费，互动数据更有意义。")
        if engagement > 1.0:
            metric_lines.append(f"互动率 {engagement:.2f}% — 优秀，说明内容本身有吸引力，用户选择留下并互动。")
        elif engagement > 0.1:
            metric_lines.append(f"互动率 {engagement:.2f}% — 中等水平，创意有一定留人能力。")
        else:
            metric_lines.append(f"互动率 {engagement:.2f}% — 偏低，可能创意吸引力不足或受众匹配度不高。")
        metric_lines.append(f"实际 CPV 约 ${cpv_mid:.4f}（行业基准 $0.02-0.06）。")
        analysis["metric_interpretation"] = " ".join(metric_lines)

    else:
        # Long-form / Discovery
        metric_lines = []
        metric_lines.append(f"长视频/发现类广告，用户主动点击才会观看，高意向流量。")
        if engagement > 2.0:
            metric_lines.append(f"互动率 {engagement:.2f}% — 很强，说明内容有深度吸引力。")
        elif engagement > 0.5:
            metric_lines.append(f"互动率 {engagement:.2f}% — 正常水平。")
        else:
            metric_lines.append(f"互动率 {engagement:.2f}% — 偏低，内容可能需要优化。")
        analysis["metric_interpretation"] = " ".join(metric_lines)

    # ===== 2. Strategy Insight =====
    strategy_lines = []

    if views > avg_views * 3:
        strategy_lines.append(f"这是该品牌的重点投放素材（播放量是均值的 {views/avg_views:.1f}x），预算倾斜明显。")
    elif views > avg_views:
        strategy_lines.append(f"播放量高于均值（{views/avg_views:.1f}x），属于主力素材。")
    else:
        strategy_lines.append(f"播放量低于均值（{views/avg_views:.1f}x），可能是测试素材或投放时间较短。")

    if duration_s <= 7:
        strategy_lines.append("6 秒 Bumper 定位品牌记忆强化，通常配合长视频做 Frequency 覆盖。")
    elif duration_s <= 16:
        strategy_lines.append(f"{duration_s} 秒 Non-skippable 兼顾品牌传达和信息密度，适合核心卖点传递。")
    elif duration_s <= 30:
        strategy_lines.append(f"{duration_s} 秒 Skippable 是经典 YouTube 广告长度，前 5 秒 hook 决定留存。")
    elif duration_s <= 60:
        strategy_lines.append(f"{duration_s} 秒中长视频，适合产品演示和功能介绍，目标是中下漏斗转化。")
    else:
        strategy_lines.append(f"{duration_s//60} 分+ 长视频，通常作为 Discovery 广告或再营销素材，目标是深度说服。")

    spend_mid = (spend_low + spend_high) / 2
    strategy_lines.append(f"估算花费 {fmt_money(spend_low)}~{fmt_money(spend_high)}。")

    analysis["strategy_insight"] = " ".join(strategy_lines)

    # ===== 3. Reference Value =====
    ref_lines = []
    title_lower = video.get("title", "").lower()

    # Creative pattern detection
    if any(kw in title_lower for kw in ["vibe cod", "success", "story", "$"]):
        ref_lines.append("🎯 用户成功案例类素材 — 适合 MOFU 阶段，建立产品可信度。")
    elif any(kw in title_lower for kw in ["introduce", "introducing", "launch", "meet"]):
        ref_lines.append("🚀 产品发布/介绍类素材 — 适合品牌认知阶段，传递核心价值主张。")
    elif any(kw in title_lower for kw in ["tutorial", "how to", "guide", "demo"]):
        ref_lines.append("📚 教程/演示类素材 — 适合 BOFU 阶段，推动试用转化。")
    elif any(kw in title_lower for kw in ["turn your", "idea", "minutes", "build"]):
        ref_lines.append("💡 价值主张类素材 — 强调 'idea → app' 的核心承诺，品牌广告常用。")
    else:
        ref_lines.append("📋 通用素材 — 分析创意角度和前 5 秒 hook 结构。")

    if views == max_views:
        ref_lines.append("⭐ 该品牌播放量最高的素材，最值得深入拆解创意结构。")
    if engagement > avg_eng_same_type * 2 and engagement > 0.01:
        ref_lines.append("💎 互动率显著高于同类型均值，创意手法值得重点参考。")
    if likes > 1000:
        ref_lines.append(f"👍 {likes:,} 点赞 — 对广告来说很少见，说明内容本身有传播力。")

    analysis["reference_value"] = " ".join(ref_lines)

    # ===== 4. Verdict =====
    if ad_type in ("Bumper", "Non-skippable"):
        if views > avg_views * 2:
            analysis["verdict"] = "🟢 高投放量级的品牌曝光素材，触达效率是核心 — 不看互动看 CPM 和持续投放时长"
        elif views > avg_views:
            analysis["verdict"] = "🟡 中等投放量级，可能处于测试放量阶段"
        else:
            analysis["verdict"] = "🔴 投放量偏低，可能是早期测试或已暂停"
    else:
        if engagement > 1.0 and views > avg_views:
            analysis["verdict"] = "🟢 高播放 + 高互动，投放效果和创意质量双优"
        elif engagement > 0.5 or views > avg_views:
            analysis["verdict"] = "🟡 表现中等，有亮点但也有优化空间"
        else:
            analysis["verdict"] = "🔴 播放和互动均偏弱，创意或定向可能需要调整"

    return analysis


def format_analysis_markdown(analysis):
    """Format analysis dict as Markdown text block."""
    lines = []
    lines.append(f"**📊 效果判读：** {analysis['metric_interpretation']}")
    lines.append(f"**🎯 投放策略：** {analysis['strategy_insight']}")
    lines.append(f"**💡 参考价值：** {analysis['reference_value']}")
    lines.append(f"**结论：** {analysis['verdict']}")
    return "\n".join(lines)


def format_analysis_plain(analysis):
    """Format analysis as plain text (for Notion rich text blocks)."""
    lines = []
    lines.append(f"📊 效果判读：{analysis['metric_interpretation']}")
    lines.append(f"🎯 投放策略：{analysis['strategy_insight']}")
    lines.append(f"💡 参考价值：{analysis['reference_value']}")
    lines.append(f"结论：{analysis['verdict']}")
    return "\n".join(lines)
