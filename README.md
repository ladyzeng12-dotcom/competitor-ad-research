# 🔍 Competitor YouTube Ad Research

自动化竞品 YouTube 广告投放调研工具。通过 Google Ads Transparency Center (TC) 抓取竞品广告数据，结合 YouTube Data API 补充视频详情，自动分类广告类型、估算投放预算，输出结构化报告到 Notion。

## ✨ 核心能力

- **TC 广告抓取**：通过 Chrome DevTools Protocol (CDP) 绕过 Google Ads TC 的 safeframe 隔离，拦截跨域请求提取 YouTube 视频 ID（包括 Unlisted 广告素材）
- **Creative → Video 精确映射**：通过逐一加载 Creative 详情页，捕获 YouTube embed 请求，精确映射每个广告创意对应的视频
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
│   ├── tc_scraper.py       # Step 1: CDP 拦截 TC 广告（三阶段）
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
# Step 1: 抓取 TC 广告，提取 YouTube ID + Creative 映射
python src/tc_scraper.py lovable

# 跳过 Phase 2/3（仅做列表页拦截，不逐个加载 Creative 详情页）
python src/tc_scraper.py lovable --skip-detail

# Step 2: 用提取的 ID 调 YouTube Data API
python src/youtube_fetcher.py lovable

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

### TC Safeframe 绕过（三阶段方案）

Google Ads TC 将广告创意渲染在 safeframe iframe 中，常规 DOM 操作无法跨域访问。本工具通过 **三阶段网络请求拦截** 提取 YouTube 视频 ID 并建立精确的 Creative → Video 映射：

**Phase 1: 列表页 CDP 拦截**
- 通过 Playwright 的 CDP (Chrome DevTools Protocol) 在浏览器层面拦截所有网络请求
- 捕获 `ytimg.com/vi/{VIDEO_ID}/` 缩略图请求和 `youtube.com/embed/{VIDEO_ID}` 嵌入请求
- 列表页会一次性加载所有广告的缩略图，因此能获取完整的视频 ID 集合
- 同时监听 API 响应体，从中提取额外的视频 ID

**Phase 2: Creative ID 提取**
- 从列表页的 `<a href="...creative/CR...">` 链接中提取所有 Creative ID (CR格式)
- 每个 CR ID 对应 Google Ads 中的一条独立广告创意

**Phase 3: Creative → Video 精确映射**（核心创新）
- 逐一加载每个 Creative 的详情页 (`/advertiser/{AR_ID}/creative/{CR_ID}`)
- 详情页只会为**当前 Creative** 触发 YouTube embed 请求
- 通过捕获 `youtube.com/embed/{VIDEO_ID}` 请求，精确映射 CR_ID → VIDEO_ID
- 解决了列表页一次性加载所有缩略图导致无法区分的问题

> 💡 **为什么需要 Phase 3？** 列表页加载时，所有广告的 YouTube 缩略图 (`ytimg.com`) 会同时加载，无法判断哪个视频属于哪个广告。但打开单个 Creative 详情页时，YouTube embed iframe 只会加载该 Creative 对应的视频，从而实现精确映射。

```
列表页请求:
  ytimg.com/vi/VIDEO_A/  ← 所有广告的缩略图混在一起
  ytimg.com/vi/VIDEO_B/
  ytimg.com/vi/VIDEO_C/
  → 无法区分哪个视频属于哪个 Creative

详情页请求（Creative CR_001）:
  youtube.com/embed/VIDEO_A  ← 只有这一个 embed 请求
  ytimg.com/vi/VIDEO_A/      ← 当前 Creative 的缩略图
  ytimg.com/vi/VIDEO_B/      ← 其他 Creative 的缩略图（来自推荐区域）
  → embed 请求 = 精确映射
```

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
- Phase 3 会逐个加载 Creative 详情页，10 个 Creative 约需 2-3 分钟，可用 `--skip-detail` 跳过
- YouTube API 有配额限制（每天 10,000 units），大规模抓取需注意
- 预算估算基于行业平均值，实际花费可能有较大偏差
- 本工具仅用于公开信息的合法调研

## License

MIT
