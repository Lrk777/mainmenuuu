#!/usr/bin/env python3
"""
MainMenus.com full HTML scraper — final script.

Gets the COMPLETE detailed output you asked for:
  - Every state/province on mainmenus.com
  - Every city in each state
  - Every restaurant in each city
  - FULL HTML saved for each restaurant (overview page + menu page)
  - Separate folder AND separate list per state (no detail skipped)

Output structure:
  mainmenus_html/
    MASTER_INDEX.json          <- One file listing every state, city, restaurant + paths
    SCRAPE_REPORT.txt          <- Human-readable summary (counts, paths)
    <State>/                   <- Separate folder per state
      _index.json              <- List of all cities & restaurants in this state
      <City>/
        <restaurant>_overview.html
        <restaurant>_menu.html

Usage:
  python3 scrape_mainmenus_full_html.py              # Full run, all states (resume-safe)
  python3 scrape_mainmenus_full_html.py --resume     # Resume from last run
  python3 scrape_mainmenus_full_html.py --states 1 --cities 2 --restaurants 5   # Test run
"""

import re
import json
import time
import argparse
from pathlib import Path
from urllib.parse import urljoin
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://mainmenus.com"
HOMEPAGE = "https://mainmenus.com/"
DELAY_SEC = 1.5
OUT_DIR = Path("mainmenus_html")
PROGRESS_FILE = "mainmenus_html_progress.json"
MASTER_INDEX_FILE = OUT_DIR / "MASTER_INDEX.json"
REPORT_FILE = OUT_DIR / "SCRAPE_REPORT.txt"

STATE_SLUGS = {
    "ab-restaurants", "bc-restaurants", "florida-restaurants", "mb-restaurants",
    "nb-restaurants", "nl-restaurants", "northwest-territories-restaurants",
    "ns-restaurants", "on-restaurants", "prince-edward-island-restaurants",
    "qc-restaurants", "sk-restaurants", "yukon-restaurants",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (compatible; research scraper)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}


def fetch(url: str) -> str | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=25)
        r.raise_for_status()
        return r.text
    except requests.RequestException as e:
        print(f"  Error: {e}")
        return None


def get_state_urls() -> list[tuple[str, str]]:
    """(state_name, state_url) from mainmenus.com homepage."""
    html = fetch(HOMEPAGE)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if not href.startswith("http"):
            href = urljoin(BASE_URL, href)
        path = href.replace("https://", "").replace("http://", "").rstrip("/")
        parts = path.split("/")
        if len(parts) != 2 or parts[1] not in STATE_SLUGS:
            continue
        name = a.get_text(strip=True)
        if not name or len(name) > 80:
            continue
        out.append((name, href.rstrip("/")))
    return list(dict.fromkeys([(n, u) for n, u in out]))


def get_cities_from_state(state_url: str, state_slug: str) -> list[tuple[str, str]]:
    """(city_name, city_url) from state page."""
    time.sleep(DELAY_SEC)
    html = fetch(state_url)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if not href.startswith("http"):
            href = urljoin(BASE_URL, href)
        if state_slug not in href or "/page/" in href:
            continue
        text = a.get_text(strip=True)
        m = re.match(r"^(.+?)(\d+)\s*places?$", text, re.I)
        if not m:
            continue
        city_name = m.group(1).strip()
        href_clean = href.split("?")[0].split("#")[0].rstrip("/")
        out.append((city_name, href_clean))
    return list(dict.fromkeys([(n, u) for n, u in out]))


def _normalize_overview_url(href: str, city_base: str) -> str | None:
    if not href or "?" in href.split("#")[0] or city_base not in href:
        return None
    href = href.split("#")[0].rstrip("/")
    if href.endswith("/menu"):
        return href[:-5]
    parts = [p for p in href.replace("https://", "").replace("http://", "").split("/") if p]
    if len(parts) == 4 and parts[0] == "mainmenus.com" and parts[1].endswith("-restaurants"):
        return href
    return None


def get_restaurant_urls_from_city(city_url: str) -> set[str]:
    """All unique restaurant overview URLs from a city (all pagination).
    Uses sequential page numbers (1, 2, 3, ...) so we never miss a page even if
    rel=next is missing or the server returns different HTML (e.g. under load).
    """
    seen = set()
    current = city_url.rstrip("/")
    city_base = current.replace("https://", "").replace("http://", "").split("/")[1]
    max_pages = 200  # safety cap per city
    for page in range(1, max_pages + 1):
        if page == 1:
            page_url = current + "/"
        else:
            time.sleep(DELAY_SEC)
            page_url = f"{current}/page/{page}/"
        page_html = fetch(page_url)
        if not page_html:
            break
        time.sleep(DELAY_SEC)
        soup = BeautifulSoup(page_html, "html.parser")
        before = len(seen)
        for a in soup.select('a.button.button-green[href*="/menu/"]'):
            href = a.get("href")
            overview = _normalize_overview_url(urljoin(BASE_URL, href or ""), city_base)
            if overview:
                seen.add(overview)
        for a in soup.find_all("a", href=True):
            overview = _normalize_overview_url(urljoin(BASE_URL, a.get("href", "")), city_base)
            if overview and "/page/" not in overview:
                seen.add(overview)
        # Stop when this page added no new URLs (we've reached the end)
        if len(seen) == before:
            break
    return seen


def slug(s: str) -> str:
    return re.sub(r"[^\w\s-]", "", s).strip().replace(" ", "_").replace("__", "_") or "unknown"


def write_master_index_and_report(master_index: dict) -> None:
    """Write MASTER_INDEX.json and SCRAPE_REPORT.txt with full detailed output."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(MASTER_INDEX_FILE, "w", encoding="utf-8") as f:
        json.dump(master_index, f, indent=2, ensure_ascii=False)

    total_states = len(master_index["states"])
    total_cities = sum(s["city_count"] for s in master_index["states"])
    total_restaurants = sum(s["restaurant_count"] for s in master_index["states"])
    total_pages = total_restaurants * 2  # overview + menu per restaurant

    lines = [
        "MainMenus.com — Full HTML scrape report",
        "=" * 60,
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "Summary",
        "-" * 40,
        f"States/Provinces: {total_states}",
        f"Cities:          {total_cities}",
        f"Restaurants:      {total_restaurants}",
        f"HTML pages:      {total_pages} (overview + menu per restaurant)",
        "",
        "Output root: " + str(OUT_DIR.resolve()),
        "",
        "Per-state breakdown (separate list per state)",
        "-" * 40,
    ]
    for s in master_index["states"]:
        lines.append(f"  {s['state']}")
        lines.append(f"    Folder: {s['folder']}")
        lines.append(f"    Cities: {s['city_count']}, Restaurants: {s['restaurant_count']}")
        lines.append(f"    Index:  {s['folder']}/_index.json")
        lines.append("")
    lines.append("Master index (all states, cities, restaurants + paths):")
    lines.append("  " + str(MASTER_INDEX_FILE.resolve()))
    REPORT_FILE.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nDetailed report: {REPORT_FILE}")
    print(f"Master index:    {MASTER_INDEX_FILE}")


def main():
    ap = argparse.ArgumentParser(
        description="Scrape full HTML from every restaurant on MainMenus (all states) — detailed output."
    )
    ap.add_argument("--states", type=int, default=0, help="Limit to first N states (0 = all)")
    ap.add_argument("--cities", type=int, default=0, help="Limit cities per state (0 = all)")
    ap.add_argument("--restaurants", type=int, default=0, help="Limit restaurants per city (0 = all)")
    ap.add_argument("--resume", action="store_true", help="Resume from progress file")
    args = ap.parse_args()

    if args.resume and Path(PROGRESS_FILE).exists():
        with open(PROGRESS_FILE, encoding="utf-8") as f:
            progress = json.load(f)
        states_done = set(progress.get("states_done", []))
        state_cities_done = progress.get("state_cities_done", {})
        print(f"Resuming: {len(states_done)} states already done.")
    else:
        states_done = set()
        state_cities_done = {}

    # Load existing master index so final report includes previously scraped states
    if Path(MASTER_INDEX_FILE).exists():
        try:
            with open(MASTER_INDEX_FILE, encoding="utf-8") as f:
                master_index = json.load(f)
            master_index.setdefault("states", [])
        except Exception:
            master_index = {"source": BASE_URL, "scraped_at": "", "states": []}
    else:
        master_index = {"source": BASE_URL, "scraped_at": datetime.now(timezone.utc).isoformat(), "states": []}

    print("Fetching state list from mainmenus.com...")
    states = get_state_urls()
    if not states:
        print("Could not get state list.")
        return
    print(f"Found {len(states)} states/provinces.")

    states_to_do = [(n, u) for n, u in states if n not in states_done]
    if args.states > 0:
        states_to_do = states_to_do[: args.states]

    states_by_name = {s["state"]: s for s in master_index["states"]}

    for si, (state_name, state_url) in enumerate(states_to_do):
        state_slug = state_url.split("/")[-1]
        state_dir = OUT_DIR / slug(state_name)
        state_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n[{si+1}/{len(states_to_do)}] {state_name} ({state_slug})")
        cities = get_cities_from_state(state_url, state_slug)
        cities_done = state_cities_done.get(state_name, [])
        cities_to_do = [(n, u) for n, u in cities if n not in cities_done]
        if args.cities > 0:
            cities_to_do = cities_to_do[: args.cities]
        print(f"  Cities to process: {len(cities_to_do)}")

        state_index = {
            "state": state_name,
            "state_url": state_url,
            "state_slug": state_slug,
            "folder": str(state_dir.name),
            "cities": [],
        }
        state_restaurant_count = 0

        for ci, (city_name, city_url) in enumerate(cities_to_do):
            city_dir = state_dir / slug(city_name)
            city_dir.mkdir(parents=True, exist_ok=True)
            time.sleep(DELAY_SEC)
            restaurant_urls = get_restaurant_urls_from_city(city_url)
            if args.restaurants > 0:
                restaurant_urls = set(list(restaurant_urls)[: args.restaurants])

            city_restaurants = []
            for rec_url in restaurant_urls:
                parts = rec_url.rstrip("/").split("/")
                rec_slug = parts[-1] if len(parts) >= 2 else "restaurant"
                overview_path = city_dir / f"{rec_slug}_overview.html"
                menu_path = city_dir / f"{rec_slug}_menu.html"
                if overview_path.exists() and menu_path.exists():
                    city_restaurants.append({
                        "slug": rec_slug,
                        "url": rec_url,
                        "overview_file": overview_path.name,
                        "menu_file": menu_path.name,
                        "overview_path": str(overview_path.relative_to(OUT_DIR)),
                        "menu_path": str(menu_path.relative_to(OUT_DIR)),
                    })
                    state_restaurant_count += 1
                    continue
                time.sleep(DELAY_SEC)
                overview_html = fetch(rec_url)
                if overview_html:
                    overview_path.write_text(overview_html, encoding="utf-8")
                time.sleep(DELAY_SEC)
                menu_html = fetch(rec_url.rstrip("/") + "/menu/")
                if menu_html:
                    menu_path.write_text(menu_html, encoding="utf-8")
                city_restaurants.append({
                    "slug": rec_slug,
                    "url": rec_url,
                    "overview_file": overview_path.name,
                    "menu_file": menu_path.name,
                    "overview_path": str(overview_path.relative_to(OUT_DIR)),
                    "menu_path": str(menu_path.relative_to(OUT_DIR)),
                })
                state_restaurant_count += 1

            state_index["cities"].append({
                "name": city_name,
                "dir": slug(city_name),
                "restaurant_count": len(city_restaurants),
                "restaurants": city_restaurants,
            })
            state_cities_done.setdefault(state_name, []).append(city_name)
            if (ci + 1) % 5 == 0 or (ci + 1) == len(cities_to_do):
                print(f"    Cities done: {ci+1}/{len(cities_to_do)} (restaurants so far: {state_restaurant_count})")

        state_index["city_count"] = len(state_index["cities"])
        state_index["restaurant_count"] = state_restaurant_count
        (state_dir / "_index.json").write_text(
            json.dumps(state_index, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        states_by_name[state_name] = {
            "state": state_name,
            "state_url": state_url,
            "folder": state_index["folder"],
            "city_count": state_index["city_count"],
            "restaurant_count": state_index["restaurant_count"],
            "index_path": str((state_dir / "_index.json").relative_to(OUT_DIR)),
            "cities": [
                {"name": c["name"], "dir": c["dir"], "restaurant_count": c["restaurant_count"]}
                for c in state_index["cities"]
            ],
        }
        states_done.add(state_name)
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump({"states_done": list(states_done), "state_cities_done": state_cities_done}, f, indent=2)
        print(f"  Saved {state_name}: {state_index['city_count']} cities, {state_restaurant_count} restaurants.")

    master_index["states"] = list(states_by_name.values())
    master_index["scraped_at"] = datetime.now(timezone.utc).isoformat()
    write_master_index_and_report(master_index)
    print("\nDone. You have:")
    print(f"  - Full HTML for every restaurant: <State>/<City>/<slug>_overview.html, _menu.html")
    print(f"  - Separate list per state:       <State>/_index.json")
    print(f"  - One master list (all states):  {MASTER_INDEX_FILE}")
    print(f"  - Detailed report:               {REPORT_FILE}")


if __name__ == "__main__":
    main()
