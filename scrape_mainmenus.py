"""
Scrape Ontario restaurants data from mainmenus.com/on-restaurants/

Usage:
  python scrape_mainmenus.py              # Scrape cities only, save to CSV
  python scrape_mainmenus.py --json      # Output as JSON instead
"""

import re
import csv
import json
import time
import argparse
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://mainmenus.com"
ONTARIO_URL = "https://mainmenus.com/on-restaurants/"

# Be polite: identify the scraper and avoid hammering the server
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (compatible; research scraper)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}


def fetch_page(url: str) -> str | None:
    """Fetch HTML; returns None on failure."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return r.text
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None


def parse_city_link(link) -> dict | None:
    """
    Parse a link like [Ajax162 places](https://mainmenus.com/ajax-on-restaurants/).
    Returns {'city': 'Ajax', 'count': 162, 'url': '...'} or None.
    """
    href = link.get("href")
    if not href or "on-restaurants" not in href:
        return None
    text = link.get_text(strip=True)
    # Match "CityName" followed by digits and " places"
    match = re.match(r"^(.+?)(\d+)\s*places$", text, re.IGNORECASE)
    if not match:
        return None
    city_name = match.group(1).strip()
    count = int(match.group(2))
    full_url = urljoin(BASE_URL, href)
    return {"city": city_name, "count": count, "url": full_url}


def scrape_ontario_cities() -> list[dict]:
    """Scrape the Ontario restaurants page and return list of city entries."""
    html = fetch_page(ONTARIO_URL)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    results = []
    for a in soup.find_all("a", href=True):
        row = parse_city_link(a)
        if row:
            results.append(row)
    return results


def main():
    parser = argparse.ArgumentParser(description="Scrape MainMenus Ontario restaurants")
    parser.add_argument("--json", action="store_true", help="Output JSON instead of CSV")
    parser.add_argument("-o", "--output", help="Output file path (default: ontario_cities.csv or .json)")
    args = parser.parse_args()

    print("Fetching Ontario restaurants page...")
    data = scrape_ontario_cities()
    print(f"Found {len(data)} cities/regions.")

    out_path = args.output
    if args.json:
        out_path = out_path or "ontario_cities.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Saved to {out_path}")
    else:
        out_path = out_path or "ontario_cities.csv"
        if not data:
            print("No data to write.")
            return
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["city", "count", "url"])
            w.writeheader()
            w.writerows(data)
        print(f"Saved to {out_path}")


if __name__ == "__main__":
    main()
