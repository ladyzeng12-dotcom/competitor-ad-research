"""
Step 1: Google Ads Transparency Center Scraper
Uses Playwright + Chrome DevTools Protocol (CDP) to intercept network requests
from Google Ads TC, bypassing safeframe iframe isolation to extract YouTube video IDs.

Usage:
    python src/tc_scraper.py <brand_key> [--config config.json] [--output data/]

Output:
    data/tc_intercept_{brand}.json — raw intercepted data with YouTube IDs
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
    return parser.parse_args()


async def intercept_tc_ads(brand, config, output_dir):
    """
    Open the Google Ads TC page for a given advertiser and use CDP network
    interception to capture YouTube video IDs from cross-origin safeframe requests.
    """
    competitors = config["competitors"]
    settings = config["settings"]

    if brand not in competitors:
        print(f"Error: Brand '{brand}' not found in config. Available: {list(competitors.keys())}")
        sys.exit(1)

    comp = competitors[brand]
    adv_id = comp["advertiser_id"]
    url = f"https://adstransparency.google.com/advertiser/{adv_id}?region={settings['region']}&format={settings['format']}"

    print(f"=== TC Scraper: {comp['name']} ===")
    print(f"URL: {url}\n")

    video_urls = []
    youtube_ids = set()
    api_calls = []
    all_requests = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-web-security']
        )
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        # Enable CDP-level network monitoring (bypasses safeframe isolation)
        cdp = await context.new_cdp_session(page)
        await cdp.send("Network.enable")

        def on_request(params):
            req_url = params.get("request", {}).get("url", "")
            all_requests.append(req_url)
            low = req_url.lower()

            if any(kw in low for kw in ['youtube', 'googlevideo', 'videoplayback', 'ytimg', '.mp4', '.webm', 'video']):
                video_urls.append(req_url)
                # Extract YouTube video IDs from various URL patterns
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
        print("Opening page...")
        try:
            await page.goto(url, wait_until="networkidle", timeout=settings["page_load_timeout_ms"])
        except Exception as e:
            print(f"Page load warning: {e}")

        print("Page loaded. Waiting for dynamic content...")
        await asyncio.sleep(5)

        # Scroll to trigger lazy-loaded ad cards
        print(f"Scrolling ({settings['scroll_rounds']} rounds)...")
        for i in range(settings["scroll_rounds"]):
            await page.evaluate("window.scrollBy(0, 800)")
            await asyncio.sleep(settings["scroll_wait_sec"])

        # Click on ad creative elements to trigger video loading
        print("Clicking ad creative elements...")
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
                    print(f"  Found {len(elements)} elements: '{selector}'")
                    for el in elements[:3]:
                        try:
                            await el.click()
                            await asyncio.sleep(2)
                        except:
                            pass
            except:
                pass

        # Extract data from page DOM
        print("Extracting page data...")
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
            return {
                thumbnails: thumbs ? [...new Set(thumbs)] : [],
                thumbnail_video_ids: [...vidIds],
                creative_ids: creativeIds ? [...new Set(creativeIds)] : [],
                iframe_count: document.querySelectorAll('iframe').length
            };
        }""")

        # Merge page-extracted IDs
        for vid in page_data.get("thumbnail_video_ids", []):
            youtube_ids.add(vid)

        # Read API response bodies for additional video IDs
        print("Reading API response bodies...")
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
                    print(f"  +{len(set(yt_in_body))} IDs from API response")
            except:
                pass

        await browser.close()

    # Save output
    os.makedirs(output_dir, exist_ok=True)
    output = {
        "brand": brand,
        "brand_name": comp["name"],
        "advertiser_id": adv_id,
        "scraped_at": datetime.utcnow().isoformat() + "Z",
        "youtube_ids": sorted(youtube_ids),
        "video_urls": video_urls[:50],
        "api_calls": list(set(api_calls))[:50],
        "page_data": page_data,
        "stats": {
            "total_requests": len(all_requests),
            "video_requests": len(video_urls),
            "youtube_ids_found": len(youtube_ids)
        }
    }

    output_path = os.path.join(output_dir, f"tc_intercept_{brand}.json")
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n{'='*50}")
    print(f"Results: {len(youtube_ids)} YouTube video IDs found")
    for vid_id in sorted(youtube_ids):
        print(f"  https://www.youtube.com/watch?v={vid_id}")
    print(f"\nSaved to {output_path}")

    return output


if __name__ == "__main__":
    args = parse_args()
    config = load_config(args.config)
    asyncio.run(intercept_tc_ads(args.brand, config, args.output))
