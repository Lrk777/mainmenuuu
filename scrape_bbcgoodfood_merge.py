"""
Scrape BBC Good Food recipes by cuisine, then MERGE into existing allrecipes_by_cuisine
lists. Does not overwrite or duplicate: only appends new recipes (deduped by URL).
Recipes that don't map to a known cuisine go to Uncertain_cuisine.json.

Usage:
  python scrape_bbcgoodfood_merge.py              # Scrape all BBC cuisines, then merge
  python scrape_bbcgoodfood_merge.py --cuisines 2 # Limit to 2 cuisines (test)
  python scrape_bbcgoodfood_merge.py --recipes 5  # Max 5 recipes per cuisine (test)
"""

import json
import re
import time
import argparse
from pathlib import Path

from recipe_scrapers import scrape_html

try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

BASE = "https://www.bbcgoodfood.com"
CUISINE_HUB = f"{BASE}/recipes/category/cuisine-collections"
SPLIT_DIR = Path("allrecipes_by_cuisine")
DELAY = 1.5

# Known BBC Good Food cuisine collection URLs (so we get them even if hub parsing misses)
BBC_CUISINE_COLLECTIONS = [
    ("American", f"{BASE}/recipes/collection/american-recipes"),
    ("British", f"{BASE}/recipes/collection/british-recipes"),
    ("Caribbean", f"{BASE}/recipes/collection/caribbean-recipes"),
    ("Chinese", f"{BASE}/recipes/collection/chinese-recipes"),
    ("French", f"{BASE}/recipes/collection/french-recipes"),
    ("Greek", f"{BASE}/recipes/collection/greek-recipes"),
    ("Japanese", f"{BASE}/recipes/collection/japanese-recipes"),
    ("German", f"{BASE}/recipes/collection/german-recipes"),
    ("Mexican", f"{BASE}/recipes/collection/mexican-recipes"),
    ("Moroccan", f"{BASE}/recipes/collection/moroccan-recipes"),
    ("Spanish", f"{BASE}/recipes/collection/spanish-recipes"),
    ("Thai", f"{BASE}/recipes/collection/thai-recipes"),
    ("Mediterranean", f"{BASE}/recipes/collection/mediterranean-recipes"),
    ("Turkish", f"{BASE}/recipes/collection/turkish-recipes"),
    ("Vietnamese", f"{BASE}/recipes/collection/vietnamese-recipes"),
    ("Middle Eastern", f"{BASE}/recipes/collection/middle-eastern-recipes"),
    ("Scandinavian", f"{BASE}/recipes/collection/scandinavian-recipes"),
    ("Polish", f"{BASE}/recipes/collection/polish-recipes"),
]

# Map BBC Good Food collection/cuisine names to our existing cuisine key (file base name)
# Keys here are normalized (lowercase, no "recipes", no "-style", etc.)
BBC_TO_CUISINE_KEY = {
    "american": "American",
    "british": "British",
    "caribbean": "Caribbean",
    "chinese": "Chinese",
    "french": "French",
    "greek": "Greek",
    "japanese": "Japanese",
    "scandinavian": "Scandinavian",
    "german": "German",
    "mexican": "Mexican",
    "moroccan": "Moroccan",
    "spanish": "Spanish",
    "thai": "Thai",
    "mediterranean": "Mediterranean",
    "turkish": "Turkish",
    "vietnamese": "Vietnamese",
    "middle eastern": "Middle Eastern",
    "polish": "Polish",
}


def _normalize_bbc_name(name: str) -> str:
    s = name.lower().replace("-", " ").strip()
    for x in (" recipes", " recipe", "-style", "-inspired", " recipes"):
        s = s.replace(x, "")
    return s.strip()


def fetch_with_playwright(url: str, page) -> str | None:
    try:
        resp = page.goto(url, wait_until="domcontentloaded", timeout=25000)
        if resp and resp.status >= 400:
            return None
        return page.content()
    except Exception:
        return None


def get_cuisine_links_from_hub(html: str) -> list[tuple[str, str]]:
    """Extract (cuisine_name, collection_url) from cuisine-collections page."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    out = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if not href.startswith("http"):
            href = BASE + href if href.startswith("/") else ""
        if "/recipes/collection/" not in href and "/recipes/category/" not in href:
            continue
        if "/recipe/" in href:  # single recipe, skip
            continue
        text = a.get_text(strip=True)
        if not text or len(text) > 80:
            continue
        href_clean = href.split("?")[0].split("#")[0].rstrip("/")
        if href_clean in seen:
            continue
        seen.add(href_clean)
        out.append((text, href_clean))
    return out


def get_recipe_urls_from_collection(html: str, base_url: str) -> list[str]:
    """Extract recipe URLs from a collection/listing page. BBC uses /recipes/slug/ or /recipes/id/name."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    urls = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if not href.startswith("http"):
            href = (BASE + href) if href.startswith("/") else ""
        if "bbcgoodfood.com" not in href or "/recipes/" not in href:
            continue
        if "/recipes/collection/" in href or "/recipes/category/" in href or href.rstrip("/").endswith("/recipes"):
            continue
        # Recipe: /recipes/slug/ or /recipes/123/name
        u = href.split("?")[0].split("#")[0].rstrip("/")
        if u not in urls:
            urls.append(u)
    return urls


def get_next_page_url(html: str, current_url: str) -> str | None:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if "page" in href.lower() and "/recipe/" not in href:
            if not href.startswith("http"):
                base = current_url.rsplit("/", 1)[0]
                href = base + "/" + href.lstrip("/")
            if href != current_url:
                return href.split("?")[0].rstrip("/")
    return None


def _safe_get(scraper, attr: str, default=None):
    try:
        fn = getattr(scraper, attr, None)
        if fn is None:
            return default
        out = fn() if callable(fn) else fn
        return out if out is not None else default
    except Exception:
        return default


def recipe_to_dict(scraper, source: str = "bbcgoodfood.com") -> dict:
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
    out["cuisine"] = _safe_get(scraper, "cuisine")
    out["category"] = _safe_get(scraper, "category")
    out["author"] = _safe_get(scraper, "author")
    out["host"] = _safe_get(scraper, "host") or source
    out["canonical_url"] = _safe_get(scraper, "canonical_url")
    out["nutrition"] = _safe_get(scraper, "nutrition")
    out["source"] = source
    return out


def bbc_cuisine_to_key(bbc_name: str) -> str | None:
    """Map BBC cuisine/collection name to our cuisine key, or None for uncertain."""
    norm = _normalize_bbc_name(bbc_name)
    # Direct map
    if norm in BBC_TO_CUISINE_KEY:
        return BBC_TO_CUISINE_KEY[norm]
    # Partial: e.g. "Greek-style recipes" -> greek
    for key in BBC_TO_CUISINE_KEY:
        if key in norm or norm in key:
            return BBC_TO_CUISINE_KEY[key]
    # Dish-based or unknown
    if "tagine" in norm or "katsu" in norm or "chicken" in norm:
        return None
    return None


def load_existing_by_cuisine() -> dict[str, list[dict]]:
    """Load all existing JSON files from allrecipes_by_cuisine/."""
    data = {}
    if not SPLIT_DIR.exists():
        return data
    for f in SPLIT_DIR.glob("*.json"):
        if f.name.startswith("."):
            continue
        try:
            with open(f, encoding="utf-8") as fp:
                data[f.stem] = json.load(fp)
            if not isinstance(data[f.stem], list):
                data[f.stem] = []
        except Exception:
            pass
    return data


def _normalize_title(title: str) -> str:
    """Normalize recipe title for duplicate check: lowercase, single spaces, no extra punctuation."""
    if not title or not isinstance(title, str):
        return ""
    return " ".join(title.lower().strip().split())[:200]


def all_existing_urls_and_titles(data: dict[str, list[dict]]) -> tuple[set[str], set[str]]:
    """All URLs and normalized titles across every cuisine - so we never add the same recipe twice."""
    urls = set()
    titles = set()
    for recipes in data.values():
        for r in recipes:
            u = r.get("canonical_url") or r.get("url")
            if u:
                urls.add(u.strip())
            t = _normalize_title(r.get("title") or "")
            if t:
                titles.add(t)
    return urls, titles


def merge_bbc_into_existing(bbc_by_key: dict[str, list[dict]], uncertain: list[dict]) -> dict[str, list[dict]]:
    """Merge BBC recipes into existing. No overwrite. No duplicate by URL or by recipe title."""
    existing = load_existing_by_cuisine()
    seen_urls, seen_titles = all_existing_urls_and_titles(existing)

    for cuisine_key, bbc_recipes in bbc_by_key.items():
        if cuisine_key not in existing:
            existing[cuisine_key] = []
        for r in bbc_recipes:
            u = r.get("canonical_url") or r.get("url")
            if u and u in seen_urls:
                continue
            t = _normalize_title(r.get("title") or "")
            if t and t in seen_titles:
                continue
            existing[cuisine_key].append(r)
            if u:
                seen_urls.add(u)
            if t:
                seen_titles.add(t)

    for r in uncertain:
        u = r.get("canonical_url") or r.get("url")
        if u and u in seen_urls:
            continue
        t = _normalize_title(r.get("title") or "")
        if t and t in seen_titles:
            continue
        existing.setdefault("Uncertain_cuisine", []).append(r)
        if u:
            seen_urls.add(u)
        if t:
            seen_titles.add(t)

    return existing


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cuisines", type=int, default=0)
    ap.add_argument("--recipes", type=int, default=0)
    args = ap.parse_args()

    if not HAS_PLAYWRIGHT:
        print("Install: pip install playwright && playwright install chromium")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        })

        # Use known cuisine collections (reliable); optionally add any from hub
        cuisine_links = list(BBC_CUISINE_COLLECTIONS)
        try:
            time.sleep(DELAY)
            hub_html = fetch_with_playwright(CUISINE_HUB, page)
            if hub_html:
                hub_links = get_cuisine_links_from_hub(hub_html)
                seen_u = {u for _, u in cuisine_links}
                for name, url in hub_links:
                    if url not in seen_u and "/collection/" in url and any(c in name.lower() for c in ["american", "british", "french", "chinese", "greek", "japanese", "mexican", "thai", "indian", "italian", "spanish", "german", "caribbean", "moroccan", "vietnamese", "mediterranean", "turkish", "middle eastern", "polish", "scandinavian"]):
                        seen_u.add(url)
                        cuisine_links.append((name, url))
        except Exception:
            pass

        if args.cuisines > 0:
            cuisine_links = cuisine_links[: args.cuisines]

        print(f"Using {len(cuisine_links)} cuisine collections.")

        bbc_by_key = {}
        uncertain = []

        for ci, (bbc_name, coll_url) in enumerate(cuisine_links):
            # Use name as key if it's already our cuisine key (e.g. from BBC_CUISINE_COLLECTIONS)
            key = bbc_name if bbc_name in BBC_TO_CUISINE_KEY.values() else bbc_cuisine_to_key(bbc_name)
            label = key or "Uncertain"
            print(f"\n[{ci+1}/{len(cuisine_links)}] {bbc_name} -> {label}")

            time.sleep(DELAY)
            html = fetch_with_playwright(coll_url, page)
            if not html:
                continue

            recipe_urls = []
            page_url = coll_url
            page_html = html
            while page_html:
                recipe_urls.extend(get_recipe_urls_from_collection(page_html, page_url))
                next_url = get_next_page_url(page_html, page_url)
                if not next_url or next_url == page_url:
                    break
                time.sleep(DELAY)
                page_url = next_url
                page_html = fetch_with_playwright(page_url, page)

            recipe_urls = list(dict.fromkeys(recipe_urls))
            if args.recipes > 0:
                recipe_urls = recipe_urls[: args.recipes]
            print(f"  {len(recipe_urls)} recipe links")

            for i, rec_url in enumerate(recipe_urls):
                time.sleep(DELAY)
                rec_html = fetch_with_playwright(rec_url, page)
                if not rec_html:
                    continue
                try:
                    scraper = scrape_html(rec_html, rec_url)
                    r = recipe_to_dict(scraper)
                    if key:
                        bbc_by_key.setdefault(key, []).append(r)
                    else:
                        uncertain.append(r)
                except Exception:
                    uncertain.append({"_error": "parse failed", "url": rec_url, "source": "bbcgoodfood.com"})
                if (i + 1) % 5 == 0:
                    print(f"  scraped {i+1}/{len(recipe_urls)}")

        browser.close()

    # Merge into existing
    print("\nMerging into existing cuisine lists (no overwrite, no duplicates)...")
    merged = merge_bbc_into_existing(bbc_by_key, uncertain)

    SPLIT_DIR.mkdir(parents=True, exist_ok=True)
    for cuisine_name, recipe_list in merged.items():
        safe = re.sub(r"[^\w\s-]", "", cuisine_name).strip().replace(" ", "_") or "recipes"
        path = SPLIT_DIR / f"{safe}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(recipe_list, f, indent=2, ensure_ascii=False)
        print(f"  {path.name}: {len(recipe_list)} recipes")

    # Update combined file
    with open("allrecipes_by_cuisine.json", "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    print(f"\nDone. Merged data in {SPLIT_DIR}/ and allrecipes_by_cuisine.json")
    print(f"Uncertain: {len(merged.get('Uncertain_cuisine', []))} recipes in Uncertain_cuisine.json")


if __name__ == "__main__":
    main()
