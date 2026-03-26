#!/usr/bin/env python3
"""
EatingWell recipe scraper driven by seed URL(s).

Default: Discover from https://www.eatingwell.com/recipes/ (entire linked section under
/recipes/ plus recipe-detail URLs linked from there — including roundup pages, which are
crawled for links but not emitted as recipes).

You can add subsection URLs for speed or precision:
  --url https://www.eatingwell.com/recipes/17965/main-dishes/
  --urls-file my_sections.txt   # one URL per line, # comments allowed

Outputs (under --out, default eatingwell_scrape_out/):
  recipes.jsonl          one JSON object per line — only rows with full Recipe schema
  recipes.json             pretty array (rewritten after each fetch batch)
  discovered_recipes.json  sorted URL list after discovery (checkpoint)
  state.json               resume: seen pages, queued recipes, completed
  failed_recipes.json      URLs that still lack Recipe JSON-LD after retries (re-run with --resume)

Rules:
  - No empty recipe rows in recipes.jsonl: failures go to failed_recipes.json, not the main list.
  - Uses Playwright persistent profile (eatingwell_profile) — pass --headful --pause-once to clear bots.

Install: pip install playwright beautifulsoup4 && playwright install chromium
"""

from __future__ import annotations

import argparse
import hashlib
import html as html_module
import json
import re
import time
from collections import deque
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

BASE = "https://www.eatingwell.com"
DEFAULT_SEED = "https://www.eatingwell.com/recipes/"

PROFILE_DIR = Path("eatingwell_profile")
DEFAULT_OUT = Path("eatingwell_scrape_out")


def _release_profile_singleton_locks(profile: Path) -> None:
    """Clear Chromium singleton files so a new launch can bind the profile (stale lock after crash)."""
    for name in ("SingletonLock", "SingletonSocket", "SingletonCookie"):
        path = profile / name
        if path.exists() or path.is_symlink():
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass


def _launch_persistent_context(p, *, user_data_dir: Path, headless: bool, viewport: dict[str, int]):
    """launch_persistent_context with one retry after removing stale profile locks."""
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
                "[profile] Profile locked (leftover Chrome or stale lock). "
                "Removing Singleton* files and retrying once…\n"
                "If it still fails: quit other Playwright/Chromium windows, or run:\n"
                "  pkill -f 'Google Chrome for Testing'\n",
                flush=True,
            )
            _release_profile_singleton_locks(user_data_dir)
            return p.chromium.launch_persistent_context(**kw)
        raise


def _clean_url(u: str) -> str:
    return u.split("#")[0].split("?")[0].rstrip("/")


def _last_path_segment(url: str) -> str:
    path = (urlparse(url).path or "").rstrip("/")
    return path.split("/")[-1].lower() if path else ""


def _is_probably_collection_or_topic_url(u: str) -> bool:
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
    if not _clean_url(u).startswith(BASE):
        return False
    if not re.search(r"-\d{6,}$", _clean_url(u)):
        return False
    if _is_probably_collection_or_topic_url(u):
        return False
    return True


def _should_crawl_for_links(u: str) -> bool:
    """Follow this page to find more recipe/listing links (not static / off-site)."""
    c = _clean_url(u)
    if not c.startswith(BASE):
        return False
    path = (urlparse(c).path or "").lower()
    skip = (
        "/account",
        "/login",
        "/newsletter",
        "/video/",
        "/videos/",
        ".pdf",
        "/product",
        "/shop",
    )
    if any(s in path for s in skip):
        return False
    if _is_recipe_url(c):
        return False
    # Listing under hub
    if path.startswith("/recipes"):
        return True
    # Roundups / galleries at site root that contain recipe cards
    if _is_probably_collection_or_topic_url(c):
        return True
    if "/gallery/" in path and "recipe" in path:
        return True
    return False


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
            names: list[str] = []
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
    lowered = [b.lower() for b in bc]
    if "recipes" in lowered:
        i = lowered.index("recipes")
        if i + 1 < len(bc):
            return bc[i + 1]
    return bc[-1]


def _itemlist_urls(jsonlds: list[dict[str, Any]], base_url: str) -> list[str]:
    out: list[str] = []
    for obj in jsonlds:
        if obj.get("@type") != "ItemList":
            continue
        for it in obj.get("itemListElement") or []:
            if not isinstance(it, dict):
                continue
            item = it.get("item")
            u = it.get("url")
            if isinstance(item, str):
                u = item
            elif isinstance(item, dict):
                u = item.get("@id") or item.get("url")
            if not isinstance(u, str) or not u.strip():
                continue
            u = u.strip()
            if u.startswith("/"):
                u = _clean_url(urljoin(BASE, u))
            elif u.startswith("http"):
                u = _clean_url(u)
            else:
                u = _clean_url(urljoin(base_url, u))
            if u.startswith(BASE):
                out.append(u)
    return out


def _html_anchor_urls(soup: BeautifulSoup, page_url: str) -> list[str]:
    found: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("#"):
            continue
        full = _clean_url(urljoin(page_url, href))
        if full.startswith(BASE):
            found.append(full)
    return found


def _next_listing_page(soup: BeautifulSoup, current_url: str) -> str | None:
    link = soup.find("link", rel="next")
    if link and link.get("href"):
        return _clean_url(urljoin(current_url, link["href"]))
    for a in soup.find_all("a", href=True):
        txt = a.get_text(" ", strip=True).lower()
        if txt == "next" or txt.endswith(" next"):
            u = _clean_url(urljoin(current_url, a["href"]))
            if u != _clean_url(current_url):
                return u
    return None


def _is_bot_challenge(page) -> bool:
    try:
        t = (page.title() or "").lower()
    except Exception:
        return True
    return "just a moment" in t or "attention required" in t or "verify you are human" in t


def _wait_challenge(page, max_wait_s: float = 120.0) -> bool:
    deadline = time.time() + max_wait_s
    while time.time() < deadline:
        if not _is_bot_challenge(page):
            return True
        page.wait_for_timeout(2000)
    return not _is_bot_challenge(page)


def fetch_html(
    page,
    url: str,
    *,
    expect_recipe: bool,
    nav_timeout: int = 120_000,
    recipe_wave_cap: int | None = None,
    nav_wave_cap: int | None = None,
) -> str | None:
    """
    Return page HTML, or None on hard 404.
    Listing: retry navigation until HTML loads (after bot page clears).
    Recipe: retry until Recipe JSON-LD appears, or recipe_wave_cap outer waves (then None).
    """
    nav_wave = 0
    recipe_try = 0
    while True:
        nav_wave += 1
        if nav_wave_cap is not None and nav_wave > nav_wave_cap:
            return None
        resp = None
        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout)
        except Exception as e:
            print(f"[fetch] nav wave {nav_wave} error {url[:72]}… {e!s}")
            time.sleep(min(10 + nav_wave * 2, 90))
            continue
        if resp is not None and resp.status == 404:
            return None
        if not _wait_challenge(page, 120.0):
            print(f"[fetch] nav wave {nav_wave} still challenge {url[:72]}…")
            time.sleep(5)
            continue
        html = page.content()
        if not expect_recipe:
            return html
        soup = BeautifulSoup(html, "html.parser")
        if _find_recipe_schema(_extract_jsonld(soup)):
            return html
        for _ in range(6):
            page.wait_for_timeout(2500)
            html = page.content()
            soup = BeautifulSoup(html, "html.parser")
            if _find_recipe_schema(_extract_jsonld(soup)):
                return html
        recipe_try += 1
        cap = recipe_wave_cap if recipe_wave_cap is not None else 10**9
        if recipe_try >= cap:
            return None
        print(f"[fetch] recipe try {recipe_try}/{cap} no JSON-LD yet {url[:72]}…")
        time.sleep(min(8 + recipe_try * 2, 90))


def _build_record(
    url: str,
    recipe: dict[str, Any],
    bc: list[str] | None,
    category: str,
    html: str | None,
    html_dir: Path | None,
) -> dict[str, Any]:
    name = recipe.get("name")
    rec: dict[str, Any] = {
        "url": url,
        "title": html_module.unescape(name) if isinstance(name, str) else name,
        "description": recipe.get("description"),
        "ingredients": recipe.get("recipeIngredient"),
        "instructions": recipe.get("recipeInstructions"),
        "nutrition": recipe.get("nutrition"),
        "image": recipe.get("image"),
        "prep_time": recipe.get("prepTime"),
        "cook_time": recipe.get("cookTime"),
        "total_time": recipe.get("totalTime"),
        "yield": recipe.get("recipeYield"),
        "recipe_category": recipe.get("recipeCategory"),
        "recipe_cuisine": recipe.get("recipeCuisine"),
        "keywords": recipe.get("keywords"),
        "author": recipe.get("author"),
        "date_published": recipe.get("datePublished"),
        "aggregate_rating": recipe.get("aggregateRating"),
        "video": recipe.get("video"),
        "breadcrumbs": bc,
        "category": category,
        "recipe_json_ld": recipe,
    }
    if html_dir is not None and html:
        html_dir.mkdir(parents=True, exist_ok=True)
        key = hashlib.sha256(url.encode("utf-8")).hexdigest()[:20]
        p = html_dir / f"{key}.html"
        p.write_text(html, encoding="utf-8")
        rec["saved_html_rel"] = str(p.as_posix())
    return rec


def _load_urls_file(path: Path) -> list[str]:
    lines: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        s = raw.strip()
        if not s or s.startswith("#"):
            continue
        lines.append(_clean_url(s))
    return lines


def _read_jsonl_urls(path: Path) -> set[str]:
    if not path.exists():
        return set()
    seen: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            o = json.loads(line)
            u = o.get("url")
            if u:
                seen.add(_clean_url(u))
        except Exception:
            pass
    return seen


def discover_recipe_urls(
    page,
    seeds: list[str],
    *,
    delay: float,
    out_dir: Path,
    state: dict[str, Any],
) -> list[str]:
    """BFS listing + roundup pages; collect recipe URLs only."""
    seen_pages: set[str] = set(str(x) for x in state.get("seen_discovery_pages", []))
    recipe_urls: set[str] = set(str(x) for x in state.get("recipe_urls_found", []))
    saved_q = state.get("discovery_queue") or []
    q: deque[str] = deque(str(x) for x in saved_q if x)
    if not q:
        for s in seeds:
            c = _clean_url(s)
            if c not in seen_pages:
                q.append(c)

    state_path = out_dir / "state.json"
    failed_discovery: dict[str, str] = {str(k): v for k, v in (state.get("failed_discovery_pages") or {}).items()}
    n = 0
    while q:
        url = q.popleft()
        c = _clean_url(url)
        if c in seen_pages:
            continue
        seen_pages.add(c)
        n += 1
        time.sleep(delay)
        print(f"[discover] {n} parse {c[:88]}… ({len(recipe_urls)} recipes)")
        html = fetch_html(page, c, expect_recipe=False, nav_wave_cap=18)
        if html is None:
            print(f"[discover] 404 skip {c}")
            failed_discovery[c] = "nav_failed_or_404"
            state["failed_discovery_pages"] = failed_discovery
            continue
        soup = BeautifulSoup(html, "html.parser")
        j = _extract_jsonld(soup)
        for u in _itemlist_urls(j, c):
            if _is_recipe_url(u):
                recipe_urls.add(_clean_url(u))
        for u in _html_anchor_urls(soup, c):
            cu = _clean_url(u)
            if _is_recipe_url(cu):
                recipe_urls.add(cu)
            elif _should_crawl_for_links(cu) and cu not in seen_pages:
                q.append(cu)
        nxt = _next_listing_page(soup, c)
        if nxt and nxt not in seen_pages and _should_crawl_for_links(nxt):
            q.append(nxt)

        state["seen_discovery_pages"] = sorted(seen_pages)
        state["recipe_urls_found"] = sorted(recipe_urls)
        state["discovery_queue"] = list(q)
        state["failed_discovery_pages"] = failed_discovery
        if n % 10 == 0:
            state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
            disc = out_dir / "discovered_recipes.json"
            disc.write_text(json.dumps(sorted(recipe_urls), indent=2), encoding="utf-8")

    state["seen_discovery_pages"] = sorted(seen_pages)
    state["recipe_urls_found"] = sorted(recipe_urls)
    state["discovery_queue"] = []
    state["failed_discovery_pages"] = failed_discovery
    state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    (out_dir / "discovered_recipes.json").write_text(
        json.dumps(sorted(recipe_urls), indent=2), encoding="utf-8"
    )
    print(f"[discover] done — {len(recipe_urls)} recipe URLs")
    return sorted(recipe_urls)


def fetch_recipes(
    page,
    urls: list[str],
    *,
    delay: float,
    out_dir: Path,
    state: dict[str, Any],
    html_dir: Path | None,
    max_attempts: int,
) -> None:
    completed: set[str] = set(str(x) for x in state.get("completed_recipes", []))
    failed: dict[str, str] = {str(k): v for k, v in (state.get("failed_recipes") or {}).items()}
    jsonl_path = out_dir / "recipes.jsonl"
    recipes_json = out_dir / "recipes.json"

    jsonl_ok = _read_jsonl_urls(jsonl_path)
    completed |= jsonl_ok

    urls_todo = [u for u in urls if _clean_url(u) not in completed]

    for i, url in enumerate(urls_todo, start=1):
        c = _clean_url(url)
        time.sleep(delay)
        print(f"[recipe] {i}/{len(urls_todo)} {c[:80]}…")

        html_final: str | None = None
        rec_obj: dict[str, Any] | None = None
        for attempt in range(1, max_attempts + 1):
            html_final = fetch_html(
                page,
                c,
                expect_recipe=True,
                recipe_wave_cap=60,
                nav_wave_cap=24,
            )
            if html_final is None:
                if attempt == max_attempts:
                    failed[c] = "404_or_no_jsonld_after_retries"
                time.sleep(min(5 + attempt * 2, 60))
                continue
            soup = BeautifulSoup(html_final, "html.parser")
            j = _extract_jsonld(soup)
            rec_obj = _find_recipe_schema(j)
            if rec_obj:
                break
            failed[c] = "no_recipe_jsonld"
            time.sleep(min(5 + attempt * 2, 60))
        else:
            failed[c] = f"gave_up_after_{max_attempts}_outer_passes"

        if rec_obj and html_final:
            soup_done = BeautifulSoup(html_final, "html.parser")
            bc = _breadcrumbs(_extract_jsonld(soup_done))
            record = _build_record(
                c,
                rec_obj,
                bc,
                _category_from_breadcrumbs(bc),
                html_final,
                html_dir,
            )
            line = json.dumps(record, ensure_ascii=False) + "\n"
            with open(jsonl_path, "a", encoding="utf-8") as fp:
                fp.write(line)
            completed.add(c)
            failed.pop(c, None)

        state["completed_recipes"] = sorted(completed)
        state["failed_recipes"] = failed
        (out_dir / "state.json").write_text(json.dumps(state, indent=2), encoding="utf-8")

        if i % 25 == 0:
            _rewrite_recipes_array(jsonl_path, recipes_json)
            print(f"[recipe] checkpoint {len(completed)} saved")

    _rewrite_recipes_array(jsonl_path, recipes_json)
    (out_dir / "failed_recipes.json").write_text(
        json.dumps(failed, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"[fetch] finished — ok {len(completed)} failed {len(failed)}")


def _rewrite_recipes_array(jsonl_path: Path, recipes_json: Path) -> None:
    rows: list[dict[str, Any]] = []
    if jsonl_path.exists():
        for line in jsonl_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except Exception:
                    pass
    recipes_json.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="EatingWell seed-based recipe scraper")
    ap.add_argument(
        "--url",
        action="append",
        dest="urls",
        default=[],
        help="Seed URL (repeatable). Default hub used if none set and no file.",
    )
    ap.add_argument("--urls-file", type=Path, help="Text file: one seed URL per line")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Output directory")
    ap.add_argument("--profile", type=Path, default=PROFILE_DIR, help="Playwright user data dir")
    ap.add_argument("--headful", action="store_true", help="Visible browser")
    ap.add_argument(
        "--pause-once",
        action="store_true",
        help="Wait for Enter after first page (solve captcha / bot check)",
    )
    ap.add_argument("--delay", type=float, default=1.0)
    ap.add_argument("--save-html", action="store_true", help="Save raw HTML per recipe")
    ap.add_argument("--discover-only", action="store_true", help="Only build discovered_recipes.json")
    ap.add_argument("--fetch-only", action="store_true", help="Only fetch from discovered_recipes.json")
    ap.add_argument(
        "--max-attempts",
        type=int,
        default=80,
        help="Outer fetch passes per recipe URL before marking failed (default 80)",
    )
    args = ap.parse_args()

    out_dir = args.out
    out_dir.mkdir(parents=True, exist_ok=True)
    state_path = out_dir / "state.json"
    state: dict[str, Any] = {}
    if state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))

    seeds: list[str] = list(args.urls or [])
    if args.urls_file:
        seeds.extend(_load_urls_file(args.urls_file))
    if not seeds:
        seeds = [DEFAULT_SEED]
    seeds = [_clean_url(s) for s in seeds]

    html_dir: Path | None = (out_dir / "recipe_html") if args.save_html else None

    with sync_playwright() as p:
        ctx = _launch_persistent_context(
            p,
            user_data_dir=args.profile,
            headless=not args.headful,
            viewport={"width": 1400, "height": 900},
        )
        pg = ctx.new_page()

        html0 = fetch_html(pg, seeds[0], expect_recipe=False)
        if html0 is None:
            print("ERROR: first seed returned 404")
            ctx.close()
            return
        if args.pause_once:
            print("Solve any challenge in the browser, then press Enter…")
            input()

        if args.fetch_only:
            disc = out_dir / "discovered_recipes.json"
            if not disc.exists():
                print(f"ERROR: {disc} missing — run without --fetch-only first")
                ctx.close()
                return
            urls = json.loads(disc.read_text(encoding="utf-8"))
        else:
            urls = discover_recipe_urls(pg, seeds, delay=args.delay, out_dir=out_dir, state=state)

        if not args.discover_only:
            fetch_recipes(
                pg,
                urls,
                delay=args.delay,
                out_dir=out_dir,
                state=state,
                html_dir=html_dir,
                max_attempts=args.max_attempts,
            )

        ctx.close()

    print(f"Output directory: {out_dir.resolve()}")


if __name__ == "__main__":
    main()
