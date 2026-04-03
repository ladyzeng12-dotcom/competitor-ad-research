# 🔍 Competitor YouTube Ad Research

自动化竞品 YouTube 广告投放调研工具。通过 Google Ads Transparency Center (TC) 抓取竞品广告数据，结合 YouTube Data API 补充视频详情，自动分类广告类型、估算投放预算，输出结构化报告到 Notion。

## ✨ 核心能力

- **TC 广告抓取**：通过 Chrome DevTools Protocol (CDP) 绕过 Google Ads TC 的 safeframe 隔离，拦截跨域请求提取 YouTube 视频 ID（包括 Unlisted 广告素材）
- **视频数据拉取**：批量获取视频标题、时长、播放量、点赞、评论、发布日期等
- **广告类型自动分类**：根据视频时长自动判断 Bumper / Non-skippable / Skippable / Long-form
- **预算估算**：基于播放量和行业 CPM/CPV 基准估算广告花费
- **Notion 输出**：每个竞品生成独立的 Notion 页面，含完整视频表格和可点击链接

## 📁 项目结构

```
competitor-ad-research/
├── README.md               # 说明文档
├── config.json             # 配置文件（竞品名单、参数）
├── requirements.txt        # Python 依赖
├── .gitignore
├── src/
│   ├── main.py             # 主入口 — 串联完整流水线
│   ├── tc_scraper.py       # Step 1: CDP 拦截 TC 广告
│   ├── youtube_fetcher.py  # Step 2: YouTube API 拉取 + 分类
│   ├── report_builder.py   # Step 3: 生成 Markdown 报告
│   └── notion_writer.py    # Step 4: 构建 Notion 表格数据
├── data/                   # 运行时数据（gitignore）
│   └── .gitkeep
└── examples/
    └── sample_lovable_report.md  # 示例输出
```

## 🚀 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. 配置竞品

编辑 `config.json`，添加竞品的 Google Ads Advertiser ID：

```json
{
  "competitors": {
    "lovable": {
      "name": "Lovable",
      "advertiser_id": "AR00393402838056697857"
    },
    "your_competitor": {
      "name": "Competitor Name",
      "advertiser_id": "AR..."
    }
  }
}
```

> 💡 Advertiser ID 可在 [Google Ads Transparency Center](https://adstransparency.google.com/) 搜索广告主后从 URL 中获取。

### 3. 运行

```bash
# 单个竞品
cd competitor-ad-research
python src/main.py lovable

# 多个竞品
python src/main.py lovable manus

# 全部竞品
python src/main.py --all

# 跳过 Notion 输出（仅生成本地报告）
python src/main.py lovable --skip-notion
```

### 4. 分步执行

也可以单独运行每个步骤：

```bash
# Step 1: 抓取 TC 广告，提取 YouTube ID
python src/tc_scraper.py lovable

# Step 2: 用提取的 ID 调 YouTube Data API（需要 API 访问）
# 将 API 响应保存到 data/api_response_lovable.json

# Step 3: 生成报告
python src/report_builder.py lovable

# Step 4: 生成 Notion 表格数据
python src/notion_writer.py lovable --parent-page-id <your_page_id>
```

## 📊 广告类型分类规则

| 视频时长 | 广告类型 | 计费模式 | 漏斗位置 |
|----------|----------|----------|----------|
| ≤ 7s | Bumper Ad | CPM | TOFU（品牌曝光）|
| 8-16s | Non-skippable In-stream | CPM | TOFU/MOFU |
| 17-60s | Skippable In-stream | CPV | MOFU/BOFU |
| 60s+ | Long-form / Discovery | CPV/CPC | BOFU / 教育 |

## 💰 预算估算基准

| 类型 | 基准 | 范围 |
|------|------|------|
| Bumper / Non-skippable | CPM | $8 - $15 |
| Skippable In-stream | CPV | $0.02 - $0.06 |

可在 `config.json` 的 `ad_classification.budget_estimates` 中调整。

## 🔧 技术说明

### TC Safeframe 绕过

Google Ads TC 将广告创意渲染在 safeframe iframe 中，常规 DOM 操作无法跨域访问。本工具通过 Playwright 的 CDP (Chrome DevTools Protocol) 在浏览器层面拦截所有网络请求，包括 safeframe 内部的跨域请求，从而提取 `ytimg.com` 缩略图和 YouTube 嵌入 URL 中的视频 ID。

### YouTube API 集成

Step 2 需要 YouTube Data API 访问权限。支持两种方式：
- **Composio**: 使用 `YOUTUBE_GET_VIDEO_DETAILS_BATCH` 工具
- **直接 API**: 使用 YouTube Data API v3 的 `videos.list` 端点

### Notion 集成

Step 4 生成 Notion API 格式的表格数据，需配合 Notion API 或 Composio 写入。

## 📋 使用场景

- **每周竞品广告监控**：定时跑，只抓最近新投放的广告
- **竞品广告策略分析**：了解竞品的创意方向、投放节奏、预算规模
- **自家广告策略参考**：发现值得借鉴的创意模式和投放策略

## ⚠️ 注意事项

- TC 的页面结构可能随时变化，CDP 拦截策略可能需要适配
- YouTube API 有配额限制（每天 10,000 units），大规模抓取需注意
- 预算估算基于行业平均值，实际花费可能有较大偏差
- 本工具仅用于公开信息的合法调研

## License

MIT
