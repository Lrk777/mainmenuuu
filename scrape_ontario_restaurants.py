"""
Scrape all restaurant details and menus for every city in Ontario from mainmenus.com.

Output: ontario_restaurants.json (and progress save for resume).

Usage:
  python scrape_ontario_restaurants.py                    # Full run (saves progress)
  python scrape_ontario_restaurants.py --limit-cities 2   # Test: 2 cities only
  python scrape_ontario_restaurants.py --limit-restaurants 10  # Test: 10 restaurants per city
  python scrape_ontario_restaurants.py --resume           # Resume from last progress file
"""

import re
import json
import time
import argparse
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://mainmenus.com"
ONTARIO_URL = "https://mainmenus.com/on-restaurants/"
DELAY_SEC = 1.5  # Be polite
PROGRESS_FILE = "scrape_progress_ontario.json"
OUTPUT_FILE = "ontario_restaurants.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (compatible; research scraper)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}


def fetch(url: str) -> str | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return r.text
    except requests.RequestException as e:
        print(f"  Error: {e}")
        return None


def scrape_ontario_cities() -> list[dict]:
    """Get all city names, counts, and URLs from Ontario page."""
    html = fetch(ONTARIO_URL)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if "on-restaurants" not in href or "/page/" in href:
            continue
        text = a.get_text(strip=True)
        m = re.match(r"^(.+?)(\d+)\s*places$", text, re.I)
        if not m:
            continue
        city_name = m.group(1).strip()
        count = int(m.group(2))
        full_url = urljoin(BASE_URL, href)
        out.append({"city": city_name, "count": count, "url": full_url})
    return out


def _normalize_overview_url(href: str, city_base: str) -> str | None:
    """If href is a restaurant overview or menu URL for this city, return overview URL; else None."""
    if not href or "?" in href.split("#")[0] or city_base not in href:
        return None
    href = href.split("#")[0].rstrip("/")
    # .../city-on-restaurants/cuisine-slug/restaurant-slug/menu -> overview
    if href.endswith("/menu"):
        return href[:-5]  # strip /menu
    # Overview: exactly domain + city + cuisine + restaurant (4 path parts)
    parts = [p for p in href.replace("https://", "").replace("http://", "").split("/") if p]
    if len(parts) == 4 and parts[0] == "mainmenus.com" and parts[1].endswith("-on-restaurants"):
        return href
    return None


def get_restaurant_urls_from_city_page(city_url: str, city_name: str) -> set[str]:
    """Collect all unique restaurant overview URLs from a city (all pagination)."""
    seen = set()
    current = city_url.rstrip("/")
    city_base = current.replace("https://", "").replace("http://", "").split("/")[1]  # e.g. ajax-on-restaurants
    page = 1
    while True:
        url = f"{current}/" if page == 1 else f"{current}/page/{page}/"
        html = fetch(url)
        time.sleep(DELAY_SEC)
        if not html:
            break
        soup = BeautifulSoup(html, "html.parser")
        # Primary: "View Menu" buttons
        for a in soup.select('a.button.button-green[href*="/menu/"]'):
            href = a.get("href")
            overview = _normalize_overview_url(href, city_base)
            if overview:
                seen.add(overview)
        # Fallback: any link to a restaurant page (catches cards without View Menu, e.g. 1-place cities)
        for a in soup.find_all("a", href=True):
            href = urljoin(BASE_URL, a.get("href", ""))
            overview = _normalize_overview_url(href, city_base)
            if overview and "/page/" not in overview:
                seen.add(overview)
        # Next page?
        next_link = soup.find("link", rel="next")
        if not next_link or not next_link.get("href"):
            break
        next_href = next_link["href"]
        if "/page/" not in next_href:
            break
        page += 1
    return seen


def parse_overview(html: str, url: str) -> dict:
    """Parse restaurant overview: name, cuisines, status, address, hours, description, phone."""
    soup = BeautifulSoup(html, "html.parser")
    data = {
        "url": url,
        "name": "",
        "cuisines": [],
        "location": "",  # Ontario, City
        "status": "",  # Open Now / Closed
        "address": "",
        "hours_today": "",
        "hours_full": "",
        "description": "",
        "phone": "",
    }
    h1 = soup.select_one(".restaurant-title__name h1, .listing-head h1")
    if h1:
        data["name"] = h1.get_text(strip=True)
    data["cuisines"] = [a.get_text(strip=True) for a in soup.select(".restaurant-title__type a")]
    loc = soup.select_one(".restaurants__location")
    if loc:
        data["location"] = loc.get_text(strip=True)
    state_open = soup.select_one(".restaurants__state .open.active")
    state_close = soup.select_one(".restaurants__state .close.active")
    if state_open:
        data["status"] = "Open Now"
    elif state_close:
        data["status"] = "Closed"
    loc_div = soup.select_one(".restaurant-contact__location")
    if loc_div:
        data["address"] = loc_div.get_text(strip=True)
    time_work = soup.select_one(".restaurant-contact__time-work > div:first-of-type")
    if time_work:
        data["hours_today"] = time_work.get_text(strip=True)
    time_full = soup.select_one(".restaurant-contact__time-work--full")
    if time_full:
        data["hours_full"] = time_full.get_text(separator=" ", strip=True)
    block = soup.select_one(".restaurants__text .block__content p")
    if block:
        data["description"] = block.get_text(strip=True)
    phone_a = soup.select_one('.restaurant-contact__phone a[href^="tel:"]')
    if phone_a and phone_a.get("href", "").strip() != "tel:":
        data["phone"] = (phone_a.get("href") or "").replace("tel:", "").strip()
    return data


def parse_menu(html: str) -> list[dict]:
    """Parse menu: list of {category, items: [{name, price, description, tags}]}."""
    soup = BeautifulSoup(html, "html.parser")
    sections = []
    wrapper = soup.select_one(".menu-text__wrapper")
    if not wrapper:
        return sections
    current_category = ""
    current_items = []
    for item in wrapper.select(".menu-text__item"):
        cat = item.select_one(".menu-text__category-title")
        if cat:
            if current_category:
                sections.append({"category": current_category, "items": current_items})
            current_category = cat.get_text(strip=True)
            current_items = []
        dish = item.select_one(".menu-text__dish.dish, .dish")
        if dish:
            name_el = dish.select_one(".dish__name, h4")
            price_el = dish.select_one(".dish__price span, [itemprop=price]")
            desc_el = dish.select_one(".dish__description")
            name = name_el.get_text(strip=True) if name_el else ""
            price = price_el.get_text(strip=True) if price_el else ""
            desc = desc_el.get_text(strip=True) if desc_el else ""
            tags = []
            if desc and (desc.lower() in ("popular", "best seller") or len(desc) < 20):
                tags.append(desc)
                desc = ""
            item_data = {"name": name, "price": price, "description": desc or None, "tags": tags}
            current_items.append(item_data)
    if current_category:
        sections.append({"category": current_category, "items": current_items})
    return sections


def scrape_restaurant(overview_url: str) -> dict | None:
    """Fetch overview + menu for one restaurant; return combined dict."""
    overview_html = fetch(overview_url)
    time.sleep(DELAY_SEC)
    if not overview_html:
        return None
    overview_data = parse_overview(overview_html, overview_url)
    menu_url = overview_url.rstrip("/") + "/menu/"
    menu_html = fetch(menu_url)
    time.sleep(DELAY_SEC)
    overview_data["menu"] = parse_menu(menu_html) if menu_html else []
    return overview_data


def main():
    ap = argparse.ArgumentParser(description="Scrape all Ontario restaurants from mainmenus.com")
    ap.add_argument("--limit-cities", type=int, default=0, help="Max cities to process (0 = all)")
    ap.add_argument("--limit-restaurants", type=int, default=0, help="Max restaurants per city (0 = all)")
    ap.add_argument("--resume", action="store_true", help="Resume from progress file")
    ap.add_argument("-o", "--output", default=OUTPUT_FILE, help="Output JSON file")
    args = ap.parse_args()

    if args.resume and Path(PROGRESS_FILE).exists():
        with open(PROGRESS_FILE, encoding="utf-8") as f:
            progress = json.load(f)
        cities_done = progress.get("cities_done", [])
        results = progress.get("results", {})
        print(f"Resuming: {len(cities_done)} cities already done, {sum(len(r) for r in results.values())} restaurants.")
    else:
        cities_done = []
        results = {}

    print("Fetching Ontario cities list...")
    cities = scrape_ontario_cities()
    time.sleep(DELAY_SEC)
    if not cities:
        print("Could not fetch cities.")
        return
    print(f"Found {len(cities)} cities.")

    cities_to_do = [c for c in cities if c["city"] not in cities_done]
    if args.limit_cities > 0:
        cities_to_do = cities_to_do[: args.limit_cities]

    for ci, city_info in enumerate(cities_to_do):
        city_name = city_info["city"]
        city_url = city_info["url"]
        print(f"\n[{ci+1}/{len(cities_to_do)}] {city_name} ...")
        urls = get_restaurant_urls_from_city_page(city_url, city_name)
        if args.limit_restaurants > 0:
            urls = set(list(urls)[: args.limit_restaurants])
        print(f"  {len(urls)} restaurants")
        city_restaurants = []
        for i, overview_url in enumerate(sorted(urls)):
            r = scrape_restaurant(overview_url)
            if r:
                city_restaurants.append(r)
            if (i + 1) % 5 == 0:
                print(f"  scraped {i+1}/{len(urls)}")
        results[city_name] = city_restaurants
        cities_done.append(city_name)
        # Save progress
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump({"cities_done": cities_done, "results": results}, f, indent=2, ensure_ascii=False)
        print(f"  Saved progress ({len(city_restaurants)} restaurants for {city_name}).")

    # Final output
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nDone. Full data written to {args.output}")
    print(f"Total cities: {len(results)}, Total restaurants: {sum(len(v) for v in results.values())}")


if __name__ == "__main__":
    main()
