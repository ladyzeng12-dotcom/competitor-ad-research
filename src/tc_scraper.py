"""
Step 1: Google Ads Transparency Center Scraper
Uses Playwright + Chrome DevTools Protocol (CDP) to intercept network requests
from Google Ads TC, bypassing safeframe iframe isolation to extract YouTube video IDs.

Enhanced with Creative Detail Page technique (2026-04-08):
- Phase 1: Load advertiser listing page, intercept YouTube IDs from CDP requests
- Phase 2: Extract Creative IDs (CR...) from page links
- Phase 3: Load each creative detail page individually, capture YouTube embed URL
  to map specific CR_ID → video_ID (the embed URL only fires for the active creative)

Usage:
    python src/tc_scraper.py <brand_key> [--config config.json] [--output data/]

Output:
    data/tc_intercept_{brand}.json — raw intercepted data with YouTube IDs + creative mapping
"""
import asyncio
import json
import re
import sys
import os
import argparse
from datetime import datetime, timedelta
from playwright.async_api import async_playwright


def load_config(config_path="config.json"):
    with open(config_path) as f:
        return json.load(f)


def parse_args():
    parser = argparse.ArgumentParser(description="Scrape Google Ads Transparency Center for YouTube ad video IDs")
    parser.add_argument("brand", help="Brand key from config.json (e.g., 'lovable', 'manus')")
    parser.add_argument("--config", default="config.json", help="Path to config.json")
    parser.add_argument("--output", default="data", help="Output directory")
    parser.add_argument("--skip-detail", action="store_true", help="Skip Phase 2/3 (creative detail pages)")
    return parser.parse_args()


async def phase1_listing_page(page, cdp, url, settings):
    """
    Phase 1: Load advertiser listing page, intercept all YouTube IDs from CDP requests.
    Returns: (youtube_ids, video_urls, api_calls, all_requests, page_data)
    """
    video_urls = []
    youtube_ids = set()
    api_calls = []
    all_requests = []

    def on_request(params):
        req_url = params.get("request", {}).get("url", "")
        all_requests.append(req_url)
        low = req_url.lower()

        if any(kw in low for kw in ['youtube', 'googlevideo', 'videoplayback', 'ytimg', '.mp4', '.webm', 'video']):
            video_urls.append(req_url)
            for pattern in [
                r'(?:youtube\.com/(?:watch\?v=|embed/|v/)|youtu\.be/)([a-zA-Z0-9_-]{11})',
                r'ytimg\.com/vi/([a-zA-Z0-9_-]{11})'
            ]:
                for m in re.findall(pattern, req_url):
                    youtube_ids.add(m)

        if any(kw in low for kw in ['adstransparency', 'batchexecute', 'creative', 'transparencyreport']):
            api_calls.append(req_url)

    cdp.on("Network.requestWillBeSent", on_request)

    # Track API response bodies for deeper extraction
    response_bodies = {}

    def on_response(params):
        req_url = params.get("response", {}).get("url", "")
        req_id = params.get("requestId", "")
        if any(kw in req_url.lower() for kw in ['batchexecute', 'creative', 'adstransparency']):
            response_bodies[req_id] = req_url

    cdp.on("Network.responseReceived", on_response)

    # Load the page
    print("  Opening listing page...")
    try:
        await page.goto(url, wait_until="networkidle", timeout=settings["page_load_timeout_ms"])
    except Exception as e:
        print(f"  Page load warning: {e}")

    print("  Waiting for dynamic content...")
    await asyncio.sleep(5)

    # Scroll to trigger lazy-loaded ad cards
    print(f"  Scrolling ({settings['scroll_rounds']} rounds)...")
    for i in range(settings["scroll_rounds"]):
        await page.evaluate("window.scrollBy(0, 800)")
        await asyncio.sleep(settings["scroll_wait_sec"])

    # Click on ad creative elements to trigger video loading
    print("  Clicking ad creative elements...")
    creative_selectors = [
        'creative-preview', 'div[role="listitem"]', '.creative-card',
        '[data-creative-id]', 'material-card', 'a[href*="creative"]',
        'img[src*="ytimg"]', 'img[src*="thumbnail"]',
        'div.ad-card', '.video-preview', 'creative-card'
    ]
    for selector in creative_selectors:
        try:
            elements = await page.query_selector_all(selector)
            if elements:
                print(f"    Found {len(elements)} elements: '{selector}'")
                for el in elements[:3]:
                    try:
                        await el.click()
                        await asyncio.sleep(2)
                    except:
                        pass
        except:
            pass

    # Extract data from page DOM
    print("  Extracting page data...")
    page_data = await page.evaluate("""() => {
        const html = document.documentElement.innerHTML;
        const thumbs = html.match(/https?:\\/\\/[^"']*ytimg\\.com[^"']*/g);
        const vidIds = new Set();
        if (thumbs) {
            for (const t of thumbs) {
                const match = t.match(/\\/vi\\/([a-zA-Z0-9_-]{11})/);
                if (match) vidIds.add(match[1]);
            }
        }
        const creativeIds = html.match(/CR\\d{10,}/g);
        const creativeLinks = Array.from(document.querySelectorAll('a[href*="creative/CR"]'))
            .map(a => {
                const m = a.href.match(/creative\\/(CR\\d+)/);
                return m ? m[1] : null;
            })
            .filter(Boolean);
        return {
            thumbnails: thumbs ? [...new Set(thumbs)] : [],
            thumbnail_video_ids: [...vidIds],
            creative_ids: creativeIds ? [...new Set(creativeIds)] : [],
            creative_links: [...new Set(creativeLinks)],
            iframe_count: document.querySelectorAll('iframe').length
        };
    }""")

    # Merge page-extracted IDs
    for vid in page_data.get("thumbnail_video_ids", []):
        youtube_ids.add(vid)

    # Read API response bodies for additional video IDs
    print("  Reading API response bodies...")
    for req_id, req_url in list(response_bodies.items())[:10]:
        try:
            body = await cdp.send("Network.getResponseBody", {"requestId": req_id})
            body_text = body.get("body", "")
            yt_in_body = re.findall(
                r'(?:youtube\.com/(?:watch\?v=|embed/)|youtu\.be/|ytimg\.com/vi/)([a-zA-Z0-9_-]{11})',
                body_text
            )
            if yt_in_body:
                youtube_ids.update(yt_in_body)
                print(f"    +{len(set(yt_in_body))} IDs from API response")
        except:
            pass

    return youtube_ids, video_urls, api_calls, all_requests, page_data


async def phase2_extract_creative_ids(page, url, settings):
    """
    Phase 2: Load listing page again (or reuse) to extract Creative IDs from page links.
    CR IDs are in the format: CR + 20-digit number (e.g., CR14391829148492365825)
    
    Returns: list of CR IDs
    """
    print("\n  Phase 2: Extracting Creative IDs...")
    try:
        await page.goto(url, wait_until="networkidle", timeout=settings["page_load_timeout_ms"])
    except Exception as e:
        print(f"  Page load warning: {e}")

    await asyncio.sleep(3)

    # Scroll to load all ads
    for _ in range(settings["scroll_rounds"]):
        await page.evaluate("window.scrollBy(0, 800)")
        await asyncio.sleep(1)

    # Extract CR IDs from links
    cr_ids = await page.evaluate("""() => {
        const links = Array.from(document.querySelectorAll('a[href*="creative/CR"]'));
        const ids = links.map(a => {
            const m = a.href.match(/creative\\/(CR\\d+)/);
            return m ? m[1] : null;
        }).filter(Boolean);
        return [...new Set(ids)];
    }""")

    print(f"  Found {len(cr_ids)} Creative IDs:")
    for cr in cr_ids:
        print(f"    {cr}")

    return cr_ids


async def phase3_map_creatives_to_videos(context, adv_id, cr_ids, region, wait_sec=8):
    """
    Phase 3: Load each creative detail page individually and capture the YouTube
    embed URL. The embed request (youtube.com/embed/{VIDEO_ID}) only fires for the
    specific creative being viewed, allowing us to map CR_ID → video_ID precisely.
    
    Key insight: The listing page loads ALL video thumbnails (ytimg.com/vi/{ID})
    for all ads at once, making it impossible to know which video belongs to which
    creative. But when loading an individual creative detail page, the YouTube
    embed request only fires for THAT specific creative's video.
    
    Returns: dict of {cr_id: {"embed_id": video_id, "thumbnail_ids": [...]}}
    """
    print(f"\n  Phase 3: Mapping {len(cr_ids)} creatives to videos...")
    creative_map = {}

    for i, cr_id in enumerate(cr_ids):
        detail_url = f"https://adstransparency.google.com/advertiser/{adv_id}/creative/{cr_id}?region={region}&format=VIDEO"
        print(f"    [{i+1}/{len(cr_ids)}] Loading {cr_id}...")

        page = await context.new_page()
        embed_ids = []
        thumbnail_ids = set()

        def make_handler(embed_list, thumb_set):
            async def on_request(request):
                req_url = request.url
                embed_match = re.search(r'youtube\.com/embed/([a-zA-Z0-9_-]{11})', req_url)
                if embed_match:
                    embed_list.append(embed_match.group(1))
                thumb_match = re.search(r'ytimg\.com/vi/([a-zA-Z0-9_-]{11})/', req_url)
                if thumb_match:
                    thumb_set.add(thumb_match.group(1))
            return on_request

        handler = make_handler(embed_ids, thumbnail_ids)
        page.on("request", handler)

        try:
            await page.goto(detail_url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(wait_sec)
        except Exception as e:
            print(f"      Page load error: {e}")

        # The embed_id is the definitive mapping for this creative
        unique_embeds = list(set(embed_ids))
        creative_map[cr_id] = {
            "embed_id": unique_embeds[0] if unique_embeds else None,
            "all_embed_ids": unique_embeds,
            "thumbnail_ids": sorted(thumbnail_ids),
            "detail_url": detail_url
        }

        if unique_embeds:
            print(f"      ✅ Embed: {unique_embeds[0]} → https://www.youtube.com/watch?v={unique_embeds[0]}")
        else:
            print(f"      ⚠️  No embed captured (page may not have rendered video)")

        await page.close()

    # Summary
    mapped = sum(1 for v in creative_map.values() if v["embed_id"])
    print(f"\n  Phase 3 complete: {mapped}/{len(cr_ids)} creatives mapped to videos")

    return creative_map


async def intercept_tc_ads(brand, config, output_dir, skip_detail=False):
    """
    Full pipeline: Phase 1 (listing intercept) → Phase 2 (CR ID extraction) → Phase 3 (creative mapping)
    """
    competitors = config["competitors"]
    settings = config["settings"]

    if brand not in competitors:
        print(f"Error: Brand '{brand}' not found in config. Available: {list(competitors.keys())}")
        sys.exit(1)

    comp = competitors[brand]
    adv_id = comp["advertiser_id"]
    region = settings.get("region", "anywhere")
    url = f"https://adstransparency.google.com/advertiser/{adv_id}?region={region}&format={settings['format']}"

    print(f"=== TC Scraper: {comp['name']} ===")
    print(f"URL: {url}\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-web-security']
        )
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        )

        # Phase 1: Listing page intercept
        print("Phase 1: Listing page intercept")
        page = await context.new_page()
        cdp = await context.new_cdp_session(page)
        await cdp.send("Network.enable")

        youtube_ids, video_urls, api_calls, all_requests, page_data = await phase1_listing_page(
            page, cdp, url, settings
        )

        print(f"\n  Phase 1 results: {len(youtube_ids)} YouTube IDs from listing page")
        await page.close()

        creative_map = {}
        cr_ids = []

        if not skip_detail:
            # Phase 2: Extract Creative IDs
            page2 = await context.new_page()
            cr_ids = await phase2_extract_creative_ids(page2, url, settings)
            await page2.close()

            # Also merge CR IDs from Phase 1 page data
            if page_data.get("creative_links"):
                for cr in page_data["creative_links"]:
                    if cr not in cr_ids:
                        cr_ids.append(cr)

            # Phase 3: Map creatives to videos
            if cr_ids:
                creative_map = await phase3_map_creatives_to_videos(
                    context, adv_id, cr_ids, region
                )

                # Merge any new video IDs found in Phase 3
                for cr_data in creative_map.values():
                    if cr_data["embed_id"]:
                        youtube_ids.add(cr_data["embed_id"])
                    youtube_ids.update(cr_data.get("thumbnail_ids", []))

        await browser.close()

    # Build output
    os.makedirs(output_dir, exist_ok=True)
    output = {
        "brand": brand,
        "brand_name": comp["name"],
        "advertiser_id": adv_id,
        "scraped_at": datetime.utcnow().isoformat() + "Z",
        "youtube_ids": sorted(youtube_ids),
        "creative_ids": cr_ids,
        "creative_to_video_map": {
            cr_id: data["embed_id"]
            for cr_id, data in creative_map.items()
            if data["embed_id"]
        },
        "creative_details": creative_map,
        "video_urls": video_urls[:50],
        "api_calls": list(set(api_calls))[:50],
        "page_data": page_data,
        "stats": {
            "total_requests": len(all_requests),
            "video_requests": len(video_urls),
            "youtube_ids_found": len(youtube_ids),
            "creative_ids_found": len(cr_ids),
            "creatives_mapped": sum(1 for v in creative_map.values() if v.get("embed_id"))
        }
    }

    output_path = os.path.join(output_dir, f"tc_intercept_{brand}.json")
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n{'='*50}")
    print(f"Results: {len(youtube_ids)} YouTube video IDs found")
    for vid_id in sorted(youtube_ids):
        print(f"  https://www.youtube.com/watch?v={vid_id}")

    if creative_map:
        print(f"\nCreative → Video Mapping ({sum(1 for v in creative_map.values() if v.get('embed_id'))}/{len(cr_ids)}):")
        for cr_id, data in creative_map.items():
            if data["embed_id"]:
                print(f"  {cr_id} → {data['embed_id']} (https://youtube.com/watch?v={data['embed_id']})")
            else:
                print(f"  {cr_id} → ⚠️ unmapped")

    print(f"\nSaved to {output_path}")

    return output


if __name__ == "__main__":
    args = parse_args()
    config = load_config(args.config)
    asyncio.run(intercept_tc_ads(args.brand, config, args.output, args.skip_detail))
