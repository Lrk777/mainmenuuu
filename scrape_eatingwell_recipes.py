#!/usr/bin/env python3
"""
Scrape ALL recipes from EatingWell recipes hub and save separate lists.

Important: EatingWell uses bot protection that often blocks plain HTTP and even
headless browsers. This script uses Playwright with a *persistent browser profile*
so you can solve any "Just a moment..." / challenge ONCE, then the scraper can
continue with your session cookies.

What it produces:
  eatingwell_all_recipes.json             (true recipe rows only: full JSON-LD + key fields)
  eatingwell_nonrecipe_pages.json         (topic/roundup URLs mistakenly queued or 404 stubs)
  eatingwell_by_category/<Category>.json   (recipes only, split by breadcrumb category)
  eatingwell_progress.json                (resume; may include non-recipe rows still marked done)
  eatingwell_recipe_html/*.html           (optional: full page HTML per recipe, --save-html)

How it avoids skipping:
  - Discovers recipe URLs from the /recipes/ hub and all linked category pages
  - Walks pagination on each category until pages stop yielding new recipes
  - Dedupes by canonical URL

Install:
  pip install playwright beautifulsoup4 requests
  playwright install chromium

Run (recommended first run, interactive so you can pass any challenge):
  python3 scrape_eatingwell_recipes.py --headful --manual

Resume later (headless usually works after cookies exist, but headful is safer):
  python3 scrape_eatingwell_recipes.py --resume

Notes:
  - This script is designed to run on your machine (not on remote servers).
  - If EatingWell changes protections, you may need to rerun with --headful --manual.
"""

from __future__ import annotations

import argparse
import hashlib
import html as html_module
import json
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


BASE = "https://www.eatingwell.com"
START_HUB = "https://www.eatingwell.com/recipes/"

PROFILE_DIR = Path("eatingwell_profile")
OUT_DIR = Path("eatingwell_by_category")
MASTER_OUT = Path("eatingwell_all_recipes.json")
NONRECIPE_OUT = Path("eatingwell_nonrecipe_pages.json")
PROGRESS = Path("eatingwell_progress.json")
HTML_DIR = Path("eatingwell_recipe_html")

DELAY_SEC = 1.0


def _release_profile_singleton_locks(profile: Path) -> None:
    for name in ("SingletonLock", "SingletonSocket", "SingletonCookie"):
        path = profile / name
        if path.exists() or path.is_symlink():
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass


def _launch_persistent_context(p, *, user_data_dir: Path, headless: bool, viewport: dict[str, int]):
    kw: dict[str, Any] = {
        "user_data_dir": str(user_data_dir),
        "headless": headless,
        "viewport": viewport,
    }
    try:
        return p.chromium.launch_persistent_context(**kw)
    except Exception as e:
        err = str(e)
        if "ProcessSingleton" in err or "already in use" in err.lower():
            print(
                "[profile] Stale profile lock — clearing Singleton* and retrying once… "
                "If this fails, close other Chrome-for-Testing windows or: pkill -f 'Google Chrome for Testing'",
                flush=True,
            )
            _release_profile_singleton_locks(user_data_dir)
            return p.chromium.launch_persistent_context(**kw)
        raise


def _clean_url(u: str) -> str:
    u = u.split("#")[0].split("?")[0].rstrip("/")
    return u


def _last_path_segment(url: str) -> str:
    path = (urlparse(url).path or "").rstrip("/")
    return path.split("/")[-1].lower() if path else ""


def _is_probably_collection_or_topic_url(u: str) -> bool:
    """
    EatingWell uses the same ...-123456 slug style for roundups, diets, and articles.
    Skip these during discovery so we only queue real dish pages.
    """
    seg = _last_path_segment(u)
    if re.search(r"-recipes-\d{6,}$", seg):
        return True
    if "recipes-for-" in seg:
        return True
    if re.search(r"meal-plans-\d{6,}$", seg):
        return True
    if "recipe-ideas" in seg:
        return True
    if re.search(r"-ideas-\d{6,}$", seg) and "recipe" not in seg:
        return True
    return False


def _is_recipe_url(u: str) -> bool:
    if not re.search(r"-\d{6,}$", u):
        return False
    if _is_probably_collection_or_topic_url(u):
        return False
    return True


def _record_is_real_recipe(r: dict[str, Any]) -> bool:
    """True if this row has Recipe JSON-LD or both title and ingredients."""
    if not isinstance(r, dict) or r.get("error") == "404_not_found":
        return False
    ing = r.get("ingredients")
    title = r.get("title")
    if title and ing:
        return True
    rjl = r.get("recipe_json_ld")
    if isinstance(rjl, dict):
        t = rjl.get("@type")
        return t == "Recipe" or (isinstance(t, list) and "Recipe" in t)
    return False


def _build_recipe_record(
    url: str,
    recipe_schema: dict[str, Any],
    bc: list[str] | None,
    category: str,
    html: str | None,
    *,
    save_html_dir: Path | None,
) -> dict[str, Any]:
    """Flatten common fields and keep the full Schema.org Recipe object."""
    name = recipe_schema.get("name")
    rec: dict[str, Any] = {
        "url": url,
        "title": html_module.unescape(name) if isinstance(name, str) else name,
        "description": recipe_schema.get("description"),
        "ingredients": recipe_schema.get("recipeIngredient"),
        "instructions": recipe_schema.get("recipeInstructions"),
        "nutrition": recipe_schema.get("nutrition"),
        "image": recipe_schema.get("image"),
        "prep_time": recipe_schema.get("prepTime"),
        "cook_time": recipe_schema.get("cookTime"),
        "total_time": recipe_schema.get("totalTime"),
        "yield": recipe_schema.get("recipeYield"),
        "recipe_category": recipe_schema.get("recipeCategory"),
        "recipe_cuisine": recipe_schema.get("recipeCuisine"),
        "keywords": recipe_schema.get("keywords"),
        "author": recipe_schema.get("author"),
        "date_published": recipe_schema.get("datePublished"),
        "aggregate_rating": recipe_schema.get("aggregateRating"),
        "video": recipe_schema.get("video"),
        "breadcrumbs": bc,
        "category": category,
        "recipe_json_ld": recipe_schema,
    }
    if save_html_dir is not None and html:
        save_html_dir.mkdir(parents=True, exist_ok=True)
        key = hashlib.sha256(url.encode("utf-8")).hexdigest()[:20]
        hpath = save_html_dir / f"{key}.html"
        hpath.write_text(html, encoding="utf-8")
        rec["saved_html_rel"] = str(hpath.as_posix())
    return rec


def _extract_jsonld(soup: BeautifulSoup) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for sc in soup.find_all("script", type="application/ld+json"):
        txt = (sc.string or sc.get_text() or "").strip()
        if not txt:
            continue
        try:
            data = json.loads(txt)
        except Exception:
            continue
        if isinstance(data, list):
            out.extend([x for x in data if isinstance(x, dict)])
        elif isinstance(data, dict):
            # sometimes in @graph
            graph = data.get("@graph")
            if isinstance(graph, list):
                out.extend([x for x in graph if isinstance(x, dict)])
            out.append(data)
    return out


def _find_recipe_schema(jsonlds: list[dict[str, Any]]) -> dict[str, Any] | None:
    for obj in jsonlds:
        t = obj.get("@type")
        if t == "Recipe" or (isinstance(t, list) and "Recipe" in t):
            return obj
    return None


def _breadcrumbs(jsonlds: list[dict[str, Any]]) -> list[str] | None:
    for obj in jsonlds:
        if obj.get("@type") == "BreadcrumbList":
            items = obj.get("itemListElement") or []
            names = []
            for it in items:
                if isinstance(it, dict):
                    name = it.get("name") or (it.get("item") or {}).get("name")
                    if name:
                        names.append(str(name))
            return names or None
    return None


def _category_from_breadcrumbs(bc: list[str] | None) -> str:
    if not bc:
        return "Uncategorized"
    # Typical: ["Home","Recipes","Main Dish Recipes","..."]
    # Use the first breadcrumb after "Recipes" if present; else last.
    lowered = [b.lower() for b in bc]
    if "recipes" in lowered:
        i = lowered.index("recipes")
        if i + 1 < len(bc):
            return bc[i + 1]
    return bc[-1]


def get_links(soup: BeautifulSoup, current_url: str) -> list[str]:
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full = _clean_url(urljoin(current_url, href))
        if full.startswith(BASE):
            links.append(full)
    return links


def find_category_pages(hub_html: str) -> list[str]:
    soup = BeautifulSoup(hub_html, "html.parser")
    urls = []
    for u in get_links(soup, START_HUB):
        if u.startswith(f"{BASE}/recipes/") and not _is_recipe_url(u):
            urls.append(u)
    # Ensure hub included
    urls.append(_clean_url(START_HUB))
    # Dedup keep order
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def next_page_url(soup: BeautifulSoup, current_url: str) -> str | None:
    # rel=next
    link = soup.find("link", rel="next")
    if link and link.get("href"):
        return _clean_url(urljoin(current_url, link["href"]))
    # anchor with next
    for a in soup.find_all("a", href=True):
        txt = a.get_text(" ", strip=True).lower()
        if txt == "next" or txt.endswith("next"):
            u = _clean_url(urljoin(current_url, a["href"]))
            if u != _clean_url(current_url):
                return u
    return None


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def _is_bot_challenge_page(page) -> bool:
    try:
        t = (page.title() or "").lower()
    except Exception:
        return True
    return "just a moment" in t or "attention required" in t or "verify you are human" in t


def _wait_out_challenge(page, max_wait_s: float = 180.0) -> bool:
    """Poll until challenge title clears (automatic — no skipping)."""
    deadline = time.time() + max_wait_s
    while time.time() < deadline:
        if not _is_bot_challenge_page(page):
            return True
        page.wait_for_timeout(2000)
    return not _is_bot_challenge_page(page)


def goto_eatingwell(
    page,
    url: str,
    *,
    expect_recipe_jsonld: bool,
    navigation_timeout_ms: int = 120_000,
) -> str | None:
    """
    Keep navigating / waiting / reloading until the page is usable — do not return
    failure for transient timeouts or bot challenges. Returns HTML, or None only
    if the server responds with 404.
    """
    wave = 0
    while True:
        wave += 1
        resp = None
        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=navigation_timeout_ms)
        except Exception as e:
            wait = min(15 + wave * 2, 120)
            print(f"Wave {wave}: navigation error for {url[:80]}… — {e!s}. Retrying in {wait}s.")
            time.sleep(wait)
            continue

        if resp is not None and resp.status == 404:
            print(f"404 — giving up on {url}")
            return None

        if not _wait_out_challenge(page, max_wait_s=180.0):
            print(f"Wave {wave}: challenge still present after 180s, reloading {url[:80]}…")
            time.sleep(5)
            continue

        html = page.content()
        if expect_recipe_jsonld:
            soup = BeautifulSoup(html, "html.parser")
            if _find_recipe_schema(_extract_jsonld(soup)):
                return html
            # Extra wait for late-injected JSON-LD
            for _ in range(5):
                page.wait_for_timeout(2000)
                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                if _find_recipe_schema(_extract_jsonld(soup)):
                    return html
            wait = min(10 + wave * 3, 90)
            print(f"Wave {wave}: no Recipe JSON-LD yet for {url[:80]}… — retrying in {wait}s.")
            time.sleep(wait)
            continue

        return html


def main() -> None:
    ap = argparse.ArgumentParser(description="Scrape all EatingWell recipes with Playwright session.")
    ap.add_argument("--resume", action="store_true", help="Resume from progress file")
    ap.add_argument("--headful", action="store_true", help="Run visible browser (recommended)")
    ap.add_argument("--manual", action="store_true", help="Pause for manual challenge solving at start")
    ap.add_argument(
        "--save-html",
        action="store_true",
        help="Save full recipe page HTML under eatingwell_recipe_html/ (large on disk)",
    )
    ap.add_argument("--delay", type=float, default=DELAY_SEC)
    args = ap.parse_args()
    html_dir: Path | None = HTML_DIR if args.save_html else None

    if args.resume and PROGRESS.exists():
        prog = json.loads(PROGRESS.read_text(encoding="utf-8"))
        done_pages = set(prog.get("done_pages", []))
        done_recipes = set(prog.get("done_recipes", []))
        discovered_recipe_urls = list(prog.get("discovered_recipe_urls", []))
        all_records = prog.get("records", [])
    else:
        done_pages = set()
        done_recipes = set()
        discovered_recipe_urls = []
        all_records = []

    records_by_url = {r["url"]: r for r in all_records if isinstance(r, dict) and r.get("url")}

    with sync_playwright() as p:
        ctx = _launch_persistent_context(
            p,
            user_data_dir=PROFILE_DIR,
            headless=not args.headful,
            viewport={"width": 1280, "height": 900},
        )
        page = ctx.new_page()

        hub_html = goto_eatingwell(page, START_HUB, expect_recipe_jsonld=False)
        if hub_html is None:
            print("ERROR: hub returned 404.")
            ctx.close()
            return
        if args.manual:
            # Let user solve any challenge/captcha once.
            print("If you see a challenge page, solve it now in the browser window.")
            print("Then press Enter here to continue...")
            input()

        categories = find_category_pages(hub_html)
        print(f"Discovered {len(categories)} category/listing pages from hub.")

        discovered_set = set()
        discovered_set.update(discovered_recipe_urls)

        def add_recipe(u: str):
            if u not in discovered_set and _is_recipe_url(u):
                discovered_set.add(u)
                discovered_recipe_urls.append(u)

        # Walk category listings with pagination
        for cat_url in categories:
            current = cat_url
            seen_in_cat_walk: set[str] = set()
            while current:
                if current in seen_in_cat_walk:
                    print(f"Pagination loop detected for category, stopping at {current[:80]}…")
                    break
                seen_in_cat_walk.add(current)
                time.sleep(args.delay)
                html = goto_eatingwell(page, current, expect_recipe_jsonld=False)
                if html is None:
                    done_pages.add(current)
                    print(f"Listing page 404, moving on: {current}")
                    break
                soup = BeautifulSoup(html, "html.parser")
                # New pages: collect links; already-done pages: only advance pagination (resume)
                if current not in done_pages:
                    for u in get_links(soup, current):
                        add_recipe(u)
                    done_pages.add(current)
                nxt = next_page_url(soup, current)
                if not nxt or nxt == current:
                    break
                # Advance across already-finished pages so resume can reach new listing URLs
                if nxt in done_pages:
                    current = nxt
                    continue
                current = nxt

        print(f"Discovered {len(discovered_recipe_urls)} recipe URLs so far.")

        # Scrape each recipe page
        for i, url in enumerate(discovered_recipe_urls, start=1):
            if url in done_recipes:
                continue
            time.sleep(args.delay)
            html = goto_eatingwell(page, url, expect_recipe_jsonld=True)
            if html is None:
                records_by_url[url] = {
                    "url": url,
                    "title": None,
                    "error": "404_not_found",
                    "category": "Uncategorized",
                }
                done_recipes.add(url)
                continue
            soup = BeautifulSoup(html, "html.parser")
            jsonlds = _extract_jsonld(soup)
            recipe_schema = _find_recipe_schema(jsonlds)
            bc = _breadcrumbs(jsonlds)
            category = _category_from_breadcrumbs(bc)
            assert recipe_schema is not None  # goto_eatingwell guarantees Recipe JSON-LD
            rec = _build_recipe_record(
                url, recipe_schema, bc, category, html, save_html_dir=html_dir
            )
            records_by_url[url] = rec
            done_recipes.add(url)

            if i % 50 == 0:
                save_json(
                    PROGRESS,
                    {
                        "done_pages": sorted(done_pages),
                        "done_recipes": sorted(done_recipes),
                        "discovered_recipe_urls": discovered_recipe_urls,
                        "records": list(records_by_url.values()),
                    },
                )
                print(f"Progress: recipes scraped {len(done_recipes)}/{len(discovered_recipe_urls)}")

        ctx.close()

    # Write outputs: master + per-category = real recipes only; everything else on the side
    all_rows = list(records_by_url.values())
    recipes_only = [r for r in all_rows if _record_is_real_recipe(r)]
    nonrecipe_rows = [r for r in all_rows if not _record_is_real_recipe(r)]
    save_json(MASTER_OUT, recipes_only)
    save_json(NONRECIPE_OUT, nonrecipe_rows)

    by_cat: dict[str, list[dict[str, Any]]] = {}
    for r in recipes_only:
        by_cat.setdefault(r.get("category") or "Uncategorized", []).append(r)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for cat, items in by_cat.items():
        safe = re.sub(r"[^A-Za-z0-9_-]+", "_", cat).strip("_") or "Uncategorized"
        save_json(OUT_DIR / f"{safe}.json", items)

    save_json(
        PROGRESS,
            {
                "done_pages": sorted(done_pages),
                "done_recipes": sorted(done_recipes),
                "discovered_recipe_urls": discovered_recipe_urls,
                "records": all_rows,
            },
    )

    print(f"Saved master list: {MASTER_OUT} ({len(recipes_only)} recipes)")
    print(f"Non-recipe / empty rows: {NONRECIPE_OUT} ({len(nonrecipe_rows)})")
    print(f"Saved per-category lists: {OUT_DIR}/ ({len(by_cat)} files)")
    print(f"Progress: {PROGRESS}")


if __name__ == "__main__":
    main()

