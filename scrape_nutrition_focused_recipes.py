#!/usr/bin/env python3
"""
Scrape nutrition-focused recipe pages and build a filterable benefit index.

What this does:
  1) Crawls recipe listing pages (seed URLs below or custom --seed-file)
  2) Collects recipe URLs (with pagination)
  3) Scrapes each recipe using recipe_scrapers (+ optional Playwright for anti-bot pages)
  4) Keeps recipes with nutrition data (label)
  5) Adds nutrition benefit tags and diet/allergen tags
  6) Writes:
       - nutrition_recipes.json
       - nutrition_benefit_index.json

Install:
  pip install recipe-scrapers beautifulsoup4 requests
  pip install playwright && playwright install chromium   # optional but recommended

Usage:
  python scrape_nutrition_focused_recipes.py
  python scrape_nutrition_focused_recipes.py --recipes 100
  python scrape_nutrition_focused_recipes.py --resume
  python scrape_nutrition_focused_recipes.py --seed-file my_seed_urls.txt
"""

from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from recipe_scrapers import scrape_html

try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (compatible; nutrition-recipe-scraper)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

PROGRESS_FILE = "nutrition_scrape_progress.json"
OUTPUT_RECIPES = "nutrition_recipes.json"
OUTPUT_INDEX = "nutrition_benefit_index.json"
DELAY_SEC = 1.2

# Add/edit seed listing pages here. Keep nutrition-oriented pages.
DEFAULT_SEED_URLS = [
    "https://www.allrecipes.com/recipes/84/healthy-recipes/",
    "https://www.allrecipes.com/recipes/17562/dinner/healthy/",
    "https://www.allrecipes.com/recipes/78/breakfast-and-brunch/",
    "https://www.allrecipes.com/recipes/95/pasta-and-noodles/",
    "https://www.bbcgoodfood.com/recipes/collection/high-protein-recipes",
    "https://www.bbcgoodfood.com/recipes/collection/healthy-recipes",
]


def fetch_requests(url: str) -> str | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=25)
        r.raise_for_status()
        return r.text
    except requests.RequestException:
        return None


def fetch_playwright(url: str, page) -> str | None:
    try:
        resp = page.goto(url, wait_until="domcontentloaded", timeout=30000)
        if resp and resp.status >= 400:
            return None
        return page.content()
    except Exception:
        return None


def get_recipe_links_from_listing(html: str, page_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    found = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if not href:
            continue
        url = urljoin(page_url, href).split("?")[0].split("#")[0].rstrip("/")
        host_ok = ("allrecipes.com" in url) or ("bbcgoodfood.com" in url)
        if not host_ok:
            continue
        # AllRecipes recipe pattern
        is_allrecipes_recipe = "allrecipes.com" in url and "/recipe/" in url
        # BBC recipe pattern
        is_bbc_recipe = (
            "bbcgoodfood.com" in url
            and "/recipes/" in url
            and "/recipes/collection/" not in url
            and "/recipes/category/" not in url
        )
        if not (is_allrecipes_recipe or is_bbc_recipe):
            continue
        if url not in seen:
            seen.add(url)
            found.append(url)
    return found


def get_next_listing_url(html: str, current_url: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")

    # explicit rel=next
    link_next = soup.find("link", rel="next")
    if link_next and link_next.get("href"):
        return urljoin(current_url, link_next["href"])

    # text/button next
    for a in soup.find_all("a", href=True):
        txt = a.get_text(" ", strip=True).lower()
        if "next" in txt:
            nxt = urljoin(current_url, a["href"])
            if nxt != current_url and "/recipe/" not in nxt:
                return nxt

    # fallback /page/N pattern
    if "/page/" in current_url:
        m = re.search(r"/page/(\d+)/?$", current_url)
        if m:
            n = int(m.group(1)) + 1
            return re.sub(r"/page/\d+/?$", f"/page/{n}/", current_url)
    return current_url.rstrip("/") + "/page/2/"


def _safe_get(scraper, attr: str, default=None):
    try:
        fn = getattr(scraper, attr, None)
        if fn is None:
            return default
        out = fn() if callable(fn) else fn
        return out if out is not None else default
    except Exception:
        return default


def recipe_to_dict(scraper) -> dict[str, Any]:
    out = {
        "title": _safe_get(scraper, "title") or "",
        "description": _safe_get(scraper, "description"),
        "ingredients": list(_safe_get(scraper, "ingredients") or []),
        "instructions": _safe_get(scraper, "instructions"),
        "instructions_list": _safe_get(scraper, "instructions_list"),
        "yields": _safe_get(scraper, "yields"),
        "total_time": _safe_get(scraper, "total_time"),
        "prep_time": _safe_get(scraper, "prep_time"),
        "cook_time": _safe_get(scraper, "cook_time"),
        "image": _safe_get(scraper, "image"),
        "ratings": _safe_get(scraper, "ratings"),
        "reviews": _safe_get(scraper, "reviews"),
        "cuisine": _safe_get(scraper, "cuisine"),
        "category": _safe_get(scraper, "category"),
        "author": _safe_get(scraper, "author"),
        "host": _safe_get(scraper, "host"),
        "canonical_url": _safe_get(scraper, "canonical_url"),
        "nutrition": _safe_get(scraper, "nutrition"),
    }
    return out


def parse_nutrition_string(nutrition: str | dict | None) -> dict[str, float]:
    """
    Parse nutrition text into numeric metrics if possible.
    Handles formats like:
      "Per Serving: 412 calories; protein 29.8g; carbohydrates 21.2g; fat 22.1g"
    """
    out = {
        "calories": 0.0,
        "protein_g": 0.0,
        "carbs_g": 0.0,
        "fat_g": 0.0,
        "fiber_g": 0.0,
        "sugar_g": 0.0,
        "sodium_mg": 0.0,
    }
    if not nutrition:
        return out
    s = nutrition if isinstance(nutrition, str) else json.dumps(nutrition)
    s = s.lower()

    patterns = {
        "calories": r"(\d+(?:\.\d+)?)\s*calories",
        "protein_g": r"protein[^0-9]*?(\d+(?:\.\d+)?)\s*g",
        "carbs_g": r"(?:carbohydrates|carbs?)[^0-9]*?(\d+(?:\.\d+)?)\s*g",
        "fat_g": r"fat[^0-9]*?(\d+(?:\.\d+)?)\s*g",
        "fiber_g": r"fiber[^0-9]*?(\d+(?:\.\d+)?)\s*g",
        "sugar_g": r"sugars?[^0-9]*?(\d+(?:\.\d+)?)\s*g",
        "sodium_mg": r"sodium[^0-9]*?(\d+(?:\.\d+)?)\s*mg",
    }
    for k, p in patterns.items():
        m = re.search(p, s)
        if m:
            out[k] = float(m.group(1))
    return out


def _to_float(x: Any) -> float:
    if x is None:
        return 0.0
    s = str(x)
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    return float(m.group(1)) if m else 0.0


def extract_nutrition_from_jsonld(html: str) -> dict[str, float]:
    """
    Fallback parser for JSON-LD nutrition blocks (NutritionInformation).
    """
    soup = BeautifulSoup(html, "html.parser")
    out = {"calories": 0.0, "protein_g": 0.0, "carbs_g": 0.0, "fat_g": 0.0, "fiber_g": 0.0, "sugar_g": 0.0, "sodium_mg": 0.0}
    for sc in soup.find_all("script", type="application/ld+json"):
        txt = (sc.string or sc.get_text() or "").strip()
        if not txt:
            continue
        try:
            data = json.loads(txt)
        except Exception:
            continue
        nodes = data if isinstance(data, list) else [data]
        for node in nodes:
            if not isinstance(node, dict):
                continue
            # sometimes it's @graph
            graph = node.get("@graph")
            scan = graph if isinstance(graph, list) else [node]
            for item in scan:
                if not isinstance(item, dict):
                    continue
                nutr = item.get("nutrition")
                if not isinstance(nutr, dict):
                    continue
                out["calories"] = max(out["calories"], _to_float(nutr.get("calories")))
                out["protein_g"] = max(out["protein_g"], _to_float(nutr.get("proteinContent")))
                out["carbs_g"] = max(out["carbs_g"], _to_float(nutr.get("carbohydrateContent")))
                out["fat_g"] = max(out["fat_g"], _to_float(nutr.get("fatContent")))
                out["fiber_g"] = max(out["fiber_g"], _to_float(nutr.get("fiberContent")))
                out["sugar_g"] = max(out["sugar_g"], _to_float(nutr.get("sugarContent")))
                out["sodium_mg"] = max(out["sodium_mg"], _to_float(nutr.get("sodiumContent")))
    return out


def infer_diet_flags(ingredients: list[str]) -> dict[str, bool]:
    txt = " ".join(ingredients).lower()

    def has(words: list[str]) -> bool:
        return any(w in txt for w in words)

    has_meat = has(["chicken", "beef", "pork", "lamb", "fish", "shrimp", "bacon", "anchovy", "gelatin", "meat"])
    has_dairy = has(["milk", "butter", "cheese", "cream", "yogurt", "ghee", "whey"])
    has_egg = has(["egg", "eggs", "mayonnaise", "mayo"])
    has_honey = has(["honey"])
    has_gluten = has(["flour", "wheat", "bread", "pasta", "noodle", "barley", "rye", "soy sauce", "vermicelli"])
    has_nut = has(["almond", "walnut", "cashew", "pecan", "hazelnut", "pistachio", "macadamia", "peanut", "nut"])

    vegetarian = not has_meat
    vegan = vegetarian and (not has_dairy) and (not has_egg) and (not has_honey)

    return {
        "vegetarian": vegetarian,
        "vegan": vegan,
        "gluten_free": not has_gluten,
        "nut_free": not has_nut,
        "dairy_free": not has_dairy,
        "egg_free": not has_egg,
    }


def nutrition_benefits(n: dict[str, float]) -> list[str]:
    tags = []
    cal = n["calories"]
    p = n["protein_g"]
    c = n["carbs_g"]
    f = n["fat_g"]
    fib = n["fiber_g"]
    sug = n["sugar_g"]

    if p >= 20:
        tags.append("high_protein")
    if c > 0 and c <= 20:
        tags.append("low_carb")
    if f > 0 and f <= 12:
        tags.append("low_fat")
    if fib >= 5:
        tags.append("high_fiber")
    if sug > 0 and sug <= 8:
        tags.append("low_sugar")
    if cal > 0 and cal <= 400:
        tags.append("under_400_calories")
    if cal > 0 and cal <= 600:
        tags.append("under_600_calories")
    return tags


def load_seed_urls(seed_file: str | None) -> list[str]:
    if not seed_file:
        return list(DEFAULT_SEED_URLS)
    path = Path(seed_file)
    urls = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if s and not s.startswith("#"):
            urls.append(s)
    return urls


def main() -> None:
    ap = argparse.ArgumentParser(description="Scrape nutrition-labeled recipes and build benefit index.")
    ap.add_argument("--seed-file", default="", help="Optional text file with listing URLs (one per line)")
    ap.add_argument("--pages-per-seed", type=int, default=20, help="Max listing pages per seed URL")
    ap.add_argument("--recipes", type=int, default=0, help="Max recipes to scrape (0 = all discovered)")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--delay", type=float, default=DELAY_SEC)
    ap.add_argument("--output-recipes", default=OUTPUT_RECIPES)
    ap.add_argument("--output-index", default=OUTPUT_INDEX)
    args = ap.parse_args()

    if not HAS_PLAYWRIGHT:
        print("Playwright not installed; scraping may miss anti-bot pages. Install for best results.")

    if args.resume and Path(PROGRESS_FILE).exists():
        progress = json.loads(Path(PROGRESS_FILE).read_text(encoding="utf-8"))
        done_urls = set(progress.get("done_urls", []))
        collected = progress.get("recipes", [])
    else:
        done_urls = set()
        collected = []

    existing_by_url = {r.get("canonical_url") or r.get("source_url"): r for r in collected}

    seeds = load_seed_urls(args.seed_file or None)
    discovered_recipe_urls: list[str] = []
    discovered_set = set()

    def add_recipe_url(u: str):
        if u not in discovered_set:
            discovered_set.add(u)
            discovered_recipe_urls.append(u)

    if HAS_PLAYWRIGHT:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page()
            _run_pipeline(args, seeds, done_urls, existing_by_url, discovered_recipe_urls, add_recipe_url, page)
            browser.close()
    else:
        _run_pipeline(args, seeds, done_urls, existing_by_url, discovered_recipe_urls, add_recipe_url, None)

    recipes = list(existing_by_url.values())

    # Build user-facing filter index
    idx = {
        "benefit_tags": {},
        "diet_flags": {},
        "stats": {"recipes_total": len(recipes)},
    }
    for i, r in enumerate(recipes):
        rid = r.get("canonical_url") or r.get("source_url") or f"recipe_{i}"
        for t in r.get("benefit_tags", []):
            idx["benefit_tags"].setdefault(t, []).append(rid)
        for flag, val in (r.get("diet_flags") or {}).items():
            if val:
                idx["diet_flags"].setdefault(flag, []).append(rid)

    Path(args.output_recipes).write_text(json.dumps(recipes, indent=2), encoding="utf-8")
    Path(args.output_index).write_text(json.dumps(idx, indent=2), encoding="utf-8")
    Path(PROGRESS_FILE).write_text(
        json.dumps(
            {
                "done_urls": sorted(done_urls),
                "recipes": recipes,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"Saved recipes: {args.output_recipes} ({len(recipes)} recipes)")
    print(f"Saved index:   {args.output_index}")
    print(f"Progress:      {PROGRESS_FILE}")


class _dummy_context:
    def __enter__(self):
        return None

    def __exit__(self, exc_type, exc, tb):
        return False


def _run_pipeline(args, seeds, done_urls, existing_by_url, discovered_recipe_urls, add_recipe_url, page):
    def fetch(url: str) -> str | None:
        html = fetch_playwright(url, page) if page else None
        if html:
            return html
        return fetch_requests(url)

    # Discover recipe URLs
    for seed in seeds:
        current = seed.rstrip("/")
        for _ in range(args.pages_per_seed):
            if current in done_urls:
                break
            time.sleep(args.delay)
            html = fetch(current)
            done_urls.add(current)
            if not html:
                break
            for u in get_recipe_links_from_listing(html, current):
                add_recipe_url(u)
            nxt = get_next_listing_url(html, current)
            if not nxt or nxt == current:
                break
            current = nxt

    # Scrape recipes
    for idx, url in enumerate(discovered_recipe_urls, start=1):
        if args.recipes and idx > args.recipes:
            break
        if url in existing_by_url:
            continue
        time.sleep(args.delay)
        html = fetch(url)
        if not html:
            continue
        try:
            scraper = scrape_html(html=html, org_url=url, wild_mode=True)
            rec = recipe_to_dict(scraper)
            rec["source_url"] = url

            # Keep only recipes with nutrition label info
            n = parse_nutrition_string(rec.get("nutrition"))
            if not any(v > 0 for v in n.values()):
                n = extract_nutrition_from_jsonld(html)
            has_nutrition_label = any(v > 0 for v in n.values())
            if not has_nutrition_label:
                continue

            flags = infer_diet_flags(rec.get("ingredients") or [])
            benefit_tags = nutrition_benefits(n)
            rec["nutrition_metrics"] = n
            rec["diet_flags"] = flags
            rec["benefit_tags"] = benefit_tags

            key = rec.get("canonical_url") or rec.get("source_url")
            existing_by_url[key] = rec
        except Exception:
            continue

        if idx % 25 == 0:
            Path(PROGRESS_FILE).write_text(
                json.dumps(
                    {
                        "done_urls": sorted(done_urls),
                        "recipes": list(existing_by_url.values()),
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )


if __name__ == "__main__":
    main()

