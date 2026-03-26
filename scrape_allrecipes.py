"""
Scrape recipes from AllRecipes by cuisine. Each recipe includes ingredients,
instructions, and other details. Uses Playwright to fetch (avoids 403) and
recipe_scrapers to parse.

Install:
  pip install -r requirements_allrecipes.txt
  playwright install chromium

Usage:
  python scrape_allrecipes.py                    # All cuisines, save to allrecipes_by_cuisine.json
  python scrape_allrecipes.py --cuisines 3       # First 3 cuisines only (test)
  python scrape_allrecipes.py --recipes 5       # Max 5 recipes per cuisine (test)
  python scrape_allrecipes.py --resume           # Resume from progress file
"""

import json
import re
import time
import argparse
from pathlib import Path

from recipe_scrapers import scrape_html

# Playwright: optional so script can be run without it if user has another fetcher
try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

BASE = "https://www.allrecipes.com"
# Hub pages to discover ALL cuisines from (world + US)
CUISINE_HUB_URLS = [
    (f"{BASE}/recipes/86/world-cuisine/", "world"),   # World Cuisine
    (f"{BASE}/recipes/236/us-recipes/", "us"),        # U.S. Recipes
]

# Fallback: known cuisine URLs (including Amish and others not always linked from hub)
CUISINE_START_URLS = [
    ("World Cuisine", f"{BASE}/recipes/86/world-cuisine/"),
    ("Asian", f"{BASE}/recipes/227/world-cuisine/asian/"),
    ("European", f"{BASE}/recipes/231/world-cuisine/european/"),
    ("African", f"{BASE}/recipes/226/world-cuisine/african/"),
    ("Middle Eastern", f"{BASE}/recipes/235/world-cuisine/middle-eastern/"),
    ("Latin American", f"{BASE}/recipes/237/world-cuisine/latin-american/"),
    ("Mexican", f"{BASE}/recipes/728/world-cuisine/latin-american/mexican/"),
    ("Italian", f"{BASE}/recipes/723/world-cuisine/european/italian/"),
    ("Chinese", f"{BASE}/recipes/227/world-cuisine/asian/chinese/"),
    ("Indian", f"{BASE}/recipes/233/world-cuisine/asian/indian/"),
    ("Japanese", f"{BASE}/recipes/230/world-cuisine/asian/japanese/"),
    ("Thai", f"{BASE}/recipes/232/world-cuisine/asian/thai/"),
    ("Greek", f"{BASE}/recipes/722/world-cuisine/european/greek/"),
    ("French", f"{BASE}/recipes/721/world-cuisine/european/french/"),
    ("German", f"{BASE}/recipes/724/world-cuisine/european/german/"),
    ("Spanish", f"{BASE}/recipes/725/world-cuisine/european/spanish/"),
    ("Southern United States", f"{BASE}/recipes/228/us-recipes/southern/"),
    ("Cajun and Creole", f"{BASE}/recipes/229/us-recipes/cajun-and-creole/"),
    ("Amish and Mennonite", f"{BASE}/recipes/732/us-recipes/amish-and-mennonite/"),
    ("Jewish", f"{BASE}/recipes/730/us-recipes/jewish/"),
    ("Soul Food", f"{BASE}/recipes/731/us-recipes/soul-food/"),
    ("Tex-Mex", f"{BASE}/recipes/729/us-recipes/tex-mex/"),
    ("New England", f"{BASE}/recipes/2271/us-recipes/new-england/"),
    ("Caribbean", f"{BASE}/recipes/234/world-cuisine/caribbean/"),
    ("Canadian", f"{BASE}/recipes/238/world-cuisine/canadian/"),
    ("Korean", f"{BASE}/recipes/228/world-cuisine/asian/korean/"),
    ("Vietnamese", f"{BASE}/recipes/229/world-cuisine/asian/vietnamese/"),
    ("Filipino", f"{BASE}/recipes/231/world-cuisine/asian/filipino/"),
    ("Irish", f"{BASE}/recipes/726/world-cuisine/european/irish/"),
    ("British", f"{BASE}/recipes/233/world-cuisine/european/british/"),
    ("Portuguese", f"{BASE}/recipes/236/world-cuisine/european/portuguese/"),
    ("Brazilian", f"{BASE}/recipes/733/world-cuisine/latin-american/brazilian/"),
    ("Mediterranean", f"{BASE}/recipes/1564/world-cuisine/mediterranean/"),
    ("Moroccan", f"{BASE}/recipes/227/world-cuisine/african/moroccan/"),
    ("Hawaiian", f"{BASE}/recipes/2272/us-recipes/hawaiian/"),
]

PROGRESS_FILE = "allrecipes_progress.json"
OUTPUT_FILE = "allrecipes_by_cuisine.json"
DELAY = 1.5  # seconds between requests


def get_recipe_links_from_listing_page(html: str, base_url: str) -> list[str]:
    """Extract recipe URLs from a cuisine/listing page."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if not href.startswith("http"):
            href = (base_url.rstrip("/") + "/" + href.lstrip("/")) if base_url else href
        if "/recipe/" in href and "allrecipes.com" in href:
            url = href.split("?")[0].split("#")[0].rstrip("/")
            if url not in links:
                links.append(url)
    return links


def get_next_listing_page_url(html: str, current_url: str) -> str | None:
    """Return URL for next page of recipe listing, or None."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    base = current_url.split("?")[0].rstrip("/")
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        text = a.get_text(strip=True)
        if "next" in text.lower() or (href and "page" in href.lower()):
            if not href.startswith("http"):
                href = base + ("/" + href.lstrip("/") if href.startswith("/") else "?" + href)
            if href != current_url and "/recipe/" not in href:
                return href
    # Try numeric pagination: .../page/2/, .../page/3/
    if "/page/" in current_url:
        match = re.search(r"/page/(\d+)/?$", current_url)
        if match:
            n = int(match.group(1)) + 1
            next_url = re.sub(r"/page/\d+/?$", f"/page/{n}/", current_url)
            if next_url != current_url:
                return next_url
    elif current_url.count("/") >= 4:
        # First page might support /page/2/
        return current_url.rstrip("/") + "/page/2/"
    return None


def get_subcuisine_links(html: str, base_url: str) -> list[tuple[str, str]]:
    """Extract sub-cuisine (name, url) from a world-cuisine or us-recipes hub page."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    out = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if not href.startswith("http"):
            href = (base_url.rstrip("/") + "/" + href.lstrip("/")) if base_url else href
        if "/world-cuisine/" in href or "/us-recipes/" in href:
            if "?" in href or "/recipe/" in href:
                continue
        else:
            continue
        text = a.get_text(strip=True)
        if not text or len(text) > 80:
            continue
        href_clean = href.split("?")[0].split("#")[0].rstrip("/")
        if href_clean not in seen:
            seen.add(href_clean)
            out.append((text, href_clean))
    return out


def discover_cuisines_from_hubs(page, fetch_fn) -> list[tuple[str, str]]:
    """Fetch hub pages and return all (cuisine_name, url). Keeps CUISINE_START_URLS order, appends newly discovered."""
    seen_urls = {url for _, url in CUISINE_START_URLS}
    out = list(CUISINE_START_URLS)
    for hub_url, _ in CUISINE_HUB_URLS:
        time.sleep(DELAY)
        html = fetch_fn(hub_url, page)
        if not html:
            continue
        for name, url in get_subcuisine_links(html, hub_url):
            if url not in seen_urls:
                seen_urls.add(url)
                out.append((name, url))
    return out


def _safe_get(scraper, attr: str, default=None):
    """Get attribute from scraper and call it if callable; return default on any error."""
    try:
        fn = getattr(scraper, attr, None)
        if fn is None:
            return default
        out = fn() if callable(fn) else fn
        return out if out is not None else default
    except Exception:
        return default


def recipe_to_dict(scraper) -> dict:
    """Turn recipe_scrapers result into a full-detail dict (ingredients, instructions, etc.)."""
    out = {}
    out["title"] = _safe_get(scraper, "title") or ""
    out["description"] = _safe_get(scraper, "description")
    out["ingredients"] = list(_safe_get(scraper, "ingredients") or [])
    out["instructions"] = _safe_get(scraper, "instructions")
    out["instructions_list"] = _safe_get(scraper, "instructions_list")
    out["yields"] = _safe_get(scraper, "yields")
    out["total_time"] = _safe_get(scraper, "total_time")
    out["prep_time"] = _safe_get(scraper, "prep_time")
    out["cook_time"] = _safe_get(scraper, "cook_time")
    out["image"] = _safe_get(scraper, "image")
    out["ratings"] = _safe_get(scraper, "ratings")
    out["reviews"] = _safe_get(scraper, "reviews")
    out["cuisine"] = _safe_get(scraper, "cuisine")
    out["category"] = _safe_get(scraper, "category")
    out["author"] = _safe_get(scraper, "author")
    out["host"] = _safe_get(scraper, "host")
    out["canonical_url"] = _safe_get(scraper, "canonical_url")
    out["nutrition"] = _safe_get(scraper, "nutrition")
    return out


def fetch_with_playwright(url: str, page) -> str | None:
    """Load URL in Playwright page and return HTML."""
    try:
        resp = page.goto(url, wait_until="domcontentloaded", timeout=25000)
        if resp and resp.status >= 400:
            return None
        return page.content()
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser(description="Scrape AllRecipes by cuisine")
    ap.add_argument("--cuisines", type=int, default=0, help="Max cuisines to process (0 = all)")
    ap.add_argument("--only-cuisine", type=str, default="", help="Run only this cuisine (e.g. Mexican); full recipe list")
    ap.add_argument("--recipes", type=int, default=0, help="Max recipes per cuisine (0 = all)")
    ap.add_argument("--resume", action="store_true", help="Resume from progress file")
    ap.add_argument("-o", "--output", default=OUTPUT_FILE, help="Output JSON file")
    ap.add_argument("--split", action="store_true", help="Also write one JSON file per cuisine in a folder")
    ap.add_argument("--split-dir", default="allrecipes_by_cuisine", help="Folder for per-cuisine files (with --split)")
    ap.add_argument("--discover", action="store_true", help="Discover all cuisines from hub pages (World + US); use with full scrape")
    args = ap.parse_args()

    if not HAS_PLAYWRIGHT:
        print("Install Playwright: pip install playwright && playwright install chromium")
        return

    # Load or init progress (skip when running only one cuisine)
    if args.only_cuisine:
        results = {}
        done_cuisines = []
    elif args.resume and Path(PROGRESS_FILE).exists():
        with open(PROGRESS_FILE, encoding="utf-8") as f:
            progress = json.load(f)
        results = progress.get("results", {})
        done_cuisines = progress.get("cuisines_done", [])
        print(f"Resuming: {len(done_cuisines)} cuisines done.")
    else:
        results = {}
        done_cuisines = []

    cuisines_to_do = [
        (name, url) for name, url in CUISINE_START_URLS
        if name not in done_cuisines
    ]
    if args.only_cuisine:
        key = args.only_cuisine.strip().lower()
        match = [c for c in CUISINE_START_URLS if key in c[0].lower() or c[0].lower() in key]
        cuisines_to_do = match if match else cuisines_to_do[:1]
    elif args.cuisines > 0:
        cuisines_to_do = cuisines_to_do[: args.cuisines]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        if args.discover and not args.only_cuisine:
            print("Discovering cuisines from hub pages...")
            discovered = discover_cuisines_from_hubs(page, fetch_with_playwright)
            cuisines_to_do = [(n, u) for n, u in discovered if n not in done_cuisines]
            if args.cuisines > 0:
                cuisines_to_do = cuisines_to_do[: args.cuisines]
            print(f"  Found {len(cuisines_to_do)} cuisines to scrape.")

        for ci, (cuisine_name, cuisine_url) in enumerate(cuisines_to_do):
            print(f"\n[{ci+1}/{len(cuisines_to_do)}] {cuisine_name} ...")
            time.sleep(DELAY)
            html = fetch_with_playwright(cuisine_url, page)
            if not html:
                print("  Could not load cuisine page, skipping.")
                continue
            recipe_urls = []
            page_url = cuisine_url
            page_html = html
            while page_html:
                recipe_urls.extend(get_recipe_links_from_listing_page(page_html, page_url))
                next_url = get_next_listing_page_url(page_html, page_url)
                if not next_url or next_url == page_url:
                    break
                time.sleep(DELAY)
                page_url = next_url
                page_html = fetch_with_playwright(page_url, page)
            recipe_urls = list(dict.fromkeys(recipe_urls))  # dedupe order-preserving
            if args.recipes > 0:
                recipe_urls = recipe_urls[: args.recipes]
            print(f"  Found {len(recipe_urls)} recipe links")
            recipes = []
            for i, rec_url in enumerate(recipe_urls):
                time.sleep(DELAY)
                rec_html = fetch_with_playwright(rec_url, page)
                if not rec_html:
                    continue
                try:
                    scraper = scrape_html(rec_html, rec_url)
                    recipes.append(recipe_to_dict(scraper))
                except Exception as e:
                    recipes.append({"_error": str(e), "url": rec_url})
                if (i + 1) % 10 == 0:
                    print(f"  scraped {i+1}/{len(recipe_urls)}")
            results[cuisine_name] = recipes
            done_cuisines.append(cuisine_name)
            if not args.only_cuisine:
                with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
                    json.dump({"cuisines_done": done_cuisines, "results": results}, f, indent=2, ensure_ascii=False)
            print(f"  Saved {len(recipes)} recipes for {cuisine_name}.")

        browser.close()

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nDone. Output: {args.output}")
    print(f"Cuisines: {len(results)}, Total recipes: {sum(len(v) for v in results.values())}")

    if args.split and results:
        out_dir = Path(args.split_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        for cuisine_name, recipe_list in results.items():
            safe_name = re.sub(r'[^\w\s-]', '', cuisine_name).strip().replace(' ', '_') or "recipes"
            path = out_dir / f"{safe_name}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(recipe_list, f, indent=2, ensure_ascii=False)
            print(f"  Wrote {path} ({len(recipe_list)} recipes)")
        print(f"Separate lists: {out_dir}/")


if __name__ == "__main__":
    main()
