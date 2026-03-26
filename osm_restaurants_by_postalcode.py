#!/usr/bin/env python3
"""
Keyless restaurant scraper near a Canadian postal code (OSM-based).

Approach (no Google API key):
  1) Geocode the postal code using OpenStreetMap Nominatim (returns center + bbox)
  2) Use Overpass API to fetch restaurants within the bbox radius (covers the mapped postal area)
  3) Output JSON + CSV with restaurant name + address + coordinates (when available)

Notes:
  - This pulls from OpenStreetMap, not Google Maps.
  - To avoid missing items, bbox-derived radius may be padded slightly.
  - OSM results won't be identical to Google Maps coverage/ranking.

Run:
  python3 osm_restaurants_by_postalcode.py --postal-code "L6T 4V7" --country "Canada"

Outputs:
  osm_restaurants_<POSTAL>.json
  osm_restaurants_<POSTAL>.csv
"""

from __future__ import annotations

import os
import re
import csv
import json
import time
import math
import argparse
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import requests


NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
MAPS_CO_GEOCODE_URL = "https://geocode.maps.co/search"
GEOCODER_CA_BASE_URL = "https://geocoder.ca"

# Primary + fallbacks for Overpass API interpreter.
# Using multiple endpoints helps avoid single-DNS or service outages.
OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
]
OVERPASS_URL = OVERPASS_ENDPOINTS[0]

DEFAULT_USER_AGENT = "postalcode-restaurant-scraper/1.0 (contact: you@example.com)"

DEFAULT_OSM_AMENITIES = [
    "restaurant",  # main one
    # include these only if you want more "places that serve food"
    # "fast_food",
    # "food_court",
    # "cafe",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def slug_value(s: str) -> str:
    s = s.strip()
    s = s.replace(" ", "_")
    s = re.sub(r"[^A-Za-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "postal"


def normalize_postal_code(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", (s or "").upper())


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def request_json(url: str, params: dict[str, Any], headers: dict[str, str], timeout: int = 60) -> Any:
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()


def nominatim_geocode(postal_code: str, country: str, user_agent: str) -> dict[str, Any]:
    """
    Returns:
      {
        "lat": float, "lon": float,
        "bbox": {"minlat":..., "minlon":..., "maxlat":..., "maxlon":...} or None
      }
    """
    headers = {"User-Agent": user_agent}

    # Nominatim expects q like: "L6T 4V7, Canada"
    q = f"{postal_code}, {country}"
    params = {
        "q": q,
        "format": "jsonv2",
        "limit": 1,
        "addressdetails": 1,
    }
    results = request_json(NOMINATIM_URL, params=params, headers=headers, timeout=60)
    if not results:
        raise RuntimeError(f"Nominatim found no results for: {q}")
    r0 = results[0]
    lat = float(r0["lat"])
    lon = float(r0["lon"])

    bbox = None
    # Nominatim sometimes returns 'boundingbox': [south, north, west, east] as strings
    b = r0.get("boundingbox")
    if isinstance(b, list) and len(b) == 4:
        # Nominatim order: south, north, west, east
        south, north, west, east = map(float, b)
        bbox = {"minlat": south, "maxlat": north, "minlon": west, "maxlon": east}

    return {"lat": lat, "lon": lon, "bbox": bbox, "raw": r0}


def maps_co_geocode(postal_code: str, country: str) -> dict[str, Any]:
    """
    Fallback geocoder (keyless).
    Usually returns center lat/lon but often no bounding box.
    """
    q = f"{postal_code}, {country}"
    params = {"q": q}
    r = requests.get(
        MAPS_CO_GEOCODE_URL,
        params=params,
        timeout=60,
        headers={"User-Agent": DEFAULT_USER_AGENT},
    )
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list) or not data:
        raise RuntimeError(f"maps.co found no results for: {q}")
    r0 = data[0]
    lat = float(r0["lat"])
    lon = float(r0["lon"])
    return {"lat": lat, "lon": lon, "bbox": None, "raw": r0}


def geocode_postal_code_any(postal_code: str, country: str, user_agent: str) -> dict[str, Any]:
    try:
        return nominatim_geocode(postal_code, country, user_agent)
    except requests.HTTPError as e:
        # Nominatim sometimes blocks with 403. Fall back to maps.co.
        msg = str(e).lower()
        if "403" in msg or "forbidden" in msg:
            # maps.co may also block (401), so use geocoder.ca as the next fallback.
            q = quote(postal_code.strip())
            url = f"{GEOCODER_CA_BASE_URL}/{q}?json=1"
            r = requests.get(
                url,
                timeout=60,
                headers={"User-Agent": user_agent or DEFAULT_USER_AGENT},
            )
            r.raise_for_status()
            data = r.json()
            lat = float(data["latt"])
            lon = float(data["longt"])
            return {"lat": lat, "lon": lon, "bbox": None, "raw": data}
        raise


def bbox_radius_m(lat: float, lon: float, bbox: dict[str, float], pad_ratio: float = 1.05) -> int:
    # Compute max distance from center to bbox corners
    corners = [
        (bbox["minlat"], bbox["minlon"]),
        (bbox["minlat"], bbox["maxlon"]),
        (bbox["maxlat"], bbox["minlon"]),
        (bbox["maxlat"], bbox["maxlon"]),
    ]
    dmax = max(haversine_m(lat, lon, la, lo) for la, lo in corners)
    return int(dmax * pad_ratio)


def overpass_query_restaurants(
    lat: float,
    lon: float,
    radius_m: int,
    amenities: list[str],
    timeout_sec: int = 180,
) -> str:
    # Query nodes/ways/relations with amenity in amenities list.
    # Include "out center;" so ways/relations return a representative center point.
    amenity_filters = "".join([f'["amenity"="{a}"]' for a in amenities])

    # Overpass doesn't support 'amenity' list directly with this formatting approach
    # using our concat, so we build OR blocks:
    blocks = []
    for a in amenities:
        blocks.append(f'node["amenity"="{a}"](around:{radius_m},{lat},{lon});')
        blocks.append(f'way["amenity"="{a}"](around:{radius_m},{lat},{lon});')
        blocks.append(f'relation["amenity"="{a}"](around:{radius_m},{lat},{lon});')

    # Also include optional 'brand' food places? Not requested; keep strict restaurant tags.
    blocks_joined = "\n".join(blocks)

    return f"""
[out:json][timeout:{timeout_sec}];
(
{blocks_joined}
);
out center tags;
""".strip()


def overpass_fetch(query: str, user_agent: str, max_retries: int = 10) -> dict[str, Any]:
    headers = {"User-Agent": user_agent}
    backoff = 2.0
    last_body = None
    for attempt in range(1, max_retries + 1):
        endpoint = OVERPASS_ENDPOINTS[(attempt - 1) % len(OVERPASS_ENDPOINTS)]
        try:
            r = requests.post(endpoint, data={"data": query}, headers=headers, timeout=240)
            last_body = r.text[:2000] if r.text else None
            if r.status_code in (429, 502, 503, 504):
                # Rate limit / gateway errors: retry with exponential backoff.
                wait_s = backoff * (2 ** (attempt - 1))
                wait_s = min(wait_s, 60.0)
                print(f"    Overpass {r.status_code} (attempt {attempt}/{max_retries}); sleeping {wait_s:.1f}s…")
                time.sleep(wait_s)
                backoff = min(backoff * 1.5, 20.0)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            # Connection/DNS problems are retryable too.
            wait_s = min(backoff * (2 ** (attempt - 1)), 60.0)
            print(f"    Overpass request error (attempt {attempt}/{max_retries}): {e}; sleeping {wait_s:.1f}s…")
            time.sleep(wait_s)
            backoff = min(backoff * 1.5, 20.0)
            continue

    raise RuntimeError(f"Overpass request failed after retries. Last response: {last_body}")


def extract_place(item: dict[str, Any]) -> dict[str, Any]:
    # OSM elements have: type/id/tags/lat/lon or center:{lat,lon}
    tags = item.get("tags") or {}
    name = tags.get("name")
    # address fields are usually under addr:*
    addr = {
        "house_number": tags.get("addr:housenumber"),
        "street": tags.get("addr:street"),
        "city": tags.get("addr:city") or tags.get("addr:town") or tags.get("addr:village"),
        "province": tags.get("addr:state"),
        "postal_code": tags.get("addr:postcode"),
        "country": tags.get("addr:country"),
        "full_address": None,
    }
    # Some have 'addr:full'
    if "addr:full" in tags:
        addr["full_address"] = tags.get("addr:full")
    else:
        # Construct a best-effort single line.
        parts = []
        if tags.get("addr:housenumber"):
            parts.append(tags.get("addr:housenumber"))
        if tags.get("addr:street"):
            parts.append(tags.get("addr:street"))
        if tags.get("addr:city") or tags.get("addr:town") or tags.get("addr:village"):
            parts.append(tags.get("addr:city") or tags.get("addr:town") or tags.get("addr:village"))
        if tags.get("addr:state"):
            parts.append(tags.get("addr:state"))
        if tags.get("addr:postcode"):
            parts.append(tags.get("addr:postcode"))
        if tags.get("addr:country"):
            parts.append(tags.get("addr:country"))
        addr["full_address"] = ", ".join([p for p in parts if p]) if parts else None

    lat = item.get("lat")
    lon = item.get("lon")
    center = item.get("center") or {}
    lat = lat if lat is not None else center.get("lat")
    lon = lon if lon is not None else center.get("lon")

    return {
        "osm_type": item.get("type"),
        "osm_id": item.get("id"),
        "place_id": f"{item.get('type')}/{item.get('id')}",
        "name": name,
        "lat": lat,
        "lon": lon,
        "category": "restaurant",
        "address": addr,
        "website": tags.get("website"),
        "phone": tags.get("phone") or tags.get("contact:phone"),
        "raw_tags": {k: tags[k] for k in tags if k.startswith("addr:") or k in ("name", "website", "phone", "contact:phone", "brand") or k.startswith("brand:")},
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Keyless restaurant scraper via OSM (postal code -> restaurants within postal bbox).")
    ap.add_argument("--postal-code", required=True, help="Full Canadian postal code (e.g. L4K 3H2)")
    ap.add_argument("--country", default="Canada", help="Country name for Nominatim (default: Canada)")
    ap.add_argument(
        "--user-agent",
        default=os.environ.get("NOMINATIM_USER_AGENT", DEFAULT_USER_AGENT),
        help="User-Agent header for Nominatim/Overpass (set your contact).",
    )
    ap.add_argument("--pad", type=float, default=1.08, help="Radius padding factor around postal bbox (default 1.08)")
    ap.add_argument("--amenity", action="append", default=DEFAULT_OSM_AMENITIES, help="Add amenity filter (repeatable). Default: restaurant")
    ap.add_argument("--initial-radius-m", type=int, default=1500, help="If no bbox available, start expansion radius (default: 1500m)")
    ap.add_argument("--step-radius-m", type=int, default=1500, help="If no bbox available, increase radius by this step (default: 1500m)")
    ap.add_argument("--max-radius-m", type=int, default=60000, help="If no bbox available, stop expansion at this max radius (default: 60000m)")
    ap.add_argument("--stop-after-no-new", type=int, default=2, help="If no bbox available, stop after N consecutive radii with 0 new results (default: 2)")
    ap.add_argument("--out-json", default=None, help="Output JSON path (default auto)")
    ap.add_argument("--out-csv", default=None, help="Output CSV path (default auto)")
    ap.add_argument("--sleep", type=float, default=1.0, help="Sleep between Nominatim and Overpass (default 1.0)")
    ap.add_argument("--overpass-sleep", type=float, default=1.5, help="Sleep between Overpass calls during expansion (default 1.5)")
    ap.add_argument(
        "--strict-postal-only",
        action="store_true",
        help="Keep only restaurants whose addr:postcode exactly matches the input postal code.",
    )
    args = ap.parse_args()

    postal = args.postal_code.strip()
    base = f"osm_restaurants_{slug_value(postal)}"
    out_json = args.out_json or f"{base}.json"
    out_csv = args.out_csv or f"{base}.csv"

    print(f"Geocoding postal code: {postal} ({args.country})")
    geo = geocode_postal_code_any(postal, args.country, args.user_agent)
    lat = geo["lat"]
    lon = geo["lon"]
    bbox = geo.get("bbox")

    seen: set[str] = set()
    results: list[dict[str, Any]] = []

    time.sleep(args.sleep)

    if bbox:
        used_radius_m = bbox_radius_m(lat, lon, bbox, pad_ratio=args.pad)
        print(f"Center: lat={lat}, lon={lon} | bbox-radius={used_radius_m}m (pad={args.pad})")
        query = overpass_query_restaurants(lat=lat, lon=lon, radius_m=used_radius_m, amenities=args.amenity)
        print("Querying Overpass for restaurants from OSM…")
        data = overpass_fetch(query, args.user_agent)
        elements = data.get("elements") or []
        for el in elements:
            place_id = f"{el.get('type')}/{el.get('id')}"
            if place_id in seen:
                continue
            seen.add(place_id)
            results.append(extract_place(el))
    else:
        # No bbox from geocoder: expand radius until result set stabilizes.
        print("No bbox from geocoder; expanding radius until results stabilize…")
        radius_m = args.initial_radius_m
        used_radius_m = radius_m
        consecutive_no_new = 0
        while radius_m <= args.max_radius_m:
            print(f"  Overpass radius={radius_m}m (collected so far={len(seen)})")
            used_radius_m = radius_m
            query = overpass_query_restaurants(lat=lat, lon=lon, radius_m=radius_m, amenities=args.amenity)
            data = overpass_fetch(query, args.user_agent)
            elements = data.get("elements") or []
            before = len(seen)
            for el in elements:
                place_id = f"{el.get('type')}/{el.get('id')}"
                if place_id in seen:
                    continue
                seen.add(place_id)
                results.append(extract_place(el))
            after = len(seen)
            new_this_round = after - before
            print(f"    New unique places this round: {new_this_round}")
            if new_this_round == 0:
                consecutive_no_new += 1
            else:
                consecutive_no_new = 0
            if consecutive_no_new >= args.stop_after_no_new:
                break
            radius_m += args.step_radius_m
            time.sleep(args.overpass_sleep)

    payload = {
        "source": "OpenStreetMap (Nominatim + Overpass)",
        "postal_code": postal,
        "country": args.country,
        "geocode": {
            "lat": lat,
            "lon": lon,
            "bbox": bbox,
            "nominatim_raw": None,  # keep output smaller; set to geo['raw'] if you want
        },
        "search": {
            "radius_m": used_radius_m if "used_radius_m" in locals() else radius_m,
            "amenities": args.amenity,
        },
        "scraped_at_utc": utc_now_iso(),
        "restaurants_count": len(results),
        "results": results,
    }

    if args.strict_postal_only:
        target_pc = normalize_postal_code(postal)
        strict = []
        for r in results:
            pc = normalize_postal_code(((r.get("address") or {}).get("postal_code") or ""))
            if pc == target_pc:
                strict.append(r)
        payload["strict_postal_only"] = True
        payload["strict_postal_target"] = target_pc
        payload["restaurants_count_before_strict_filter"] = len(results)
        payload["restaurants_count"] = len(strict)
        payload["results"] = strict
        results = strict

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # CSV: flatten a few fields
    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "place_id",
                "name",
                "lat",
                "lon",
                "full_address",
                "city",
                "province",
                "postal_code",
                "website",
                "phone",
            ],
        )
        writer.writeheader()
        for r in results:
            addr = r.get("address") or {}
            writer.writerow(
                {
                    "place_id": r.get("place_id"),
                    "name": r.get("name"),
                    "lat": r.get("lat"),
                    "lon": r.get("lon"),
                    "full_address": addr.get("full_address"),
                    "city": addr.get("city"),
                    "province": addr.get("province"),
                    "postal_code": addr.get("postal_code"),
                    "website": r.get("website"),
                    "phone": r.get("phone"),
                }
            )

    print(f"Saved:\n  JSON: {out_json}\n  CSV:  {out_csv}")


if __name__ == "__main__":
    main()

