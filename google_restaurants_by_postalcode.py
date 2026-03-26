#!/usr/bin/env python3
"""
Get restaurants around a Canadian postal code using Google Maps Platform APIs.

This script is designed to:
  - Geocode a full postal code (e.g. "L4K 3H2") to lat/lng
  - Run Nearby Search for restaurants around that point
  - Paginate using next_page_token (collects all pages the API returns)
  - Expand radius step-by-step until no new place_ids are found for N expansions
  - Optionally fetch Place Details for each place_id (address/phone/website/etc.)

Outputs:
  - JSON with full results
  - CSV with key fields

Prerequisites:
  - Set environment variable: GOOGLE_MAPS_API_KEY
  - Enable in Google Cloud Console:
      * Geocoding API
      * Places API (Nearby Search + Place Details)

Run examples:
  export GOOGLE_MAPS_API_KEY="YOUR_KEY"
  python3 google_restaurants_by_postalcode.py --postal-code "L4K 3H2" --no-details
  python3 google_restaurants_by_postalcode.py --postal-code "L4K 3H2"
"""

from __future__ import annotations

import os
import csv
import json
import time
import math
import argparse
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

import requests


GOOGLE_GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
GOOGLE_NEARBY_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
GOOGLE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

DEFAULT_PLACE_DETAILS_FIELDS = [
    "place_id",
    "name",
    "formatted_address",
    "geometry",
    "international_phone_number",
    "website",
    "url",
    "opening_hours",
    "formatted_phone_number",
]

_LIKELY_PLACEHOLDER_PATTERNS = [
    "YOUR_KEY",
    "YOUR_ACTUAL_KEY",
    "PASTE_FULL_KEY_HERE",
    "PASTE_YOUR_REAL_KEY_HERE",
    "AIza...your",
    "...",
]


def looks_like_google_api_key(k: str) -> bool:
    k = (k or "").strip()
    if not k:
        return False
    upper = k.upper()
    for p in _LIKELY_PLACEHOLDER_PATTERNS:
        if p.upper() in upper:
            return False
    # Most Google API keys start with "AIza" (not a perfect guarantee, but good guardrail)
    if not k.startswith("AIza"):
        return False
    if len(k) < 30:
        return False
    return True


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def request_json(url: str, params: dict[str, Any], max_retries: int = 8) -> dict[str, Any]:
    """
    Requests JSON and retries on common transient errors/limits.
    """
    last_message = None
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, params=params, timeout=30)
            try:
                data = r.json()
            except Exception:
                data = {"_raw_text": r.text}

            status = data.get("status")
            # Some endpoints return status "OK" / "ZERO_RESULTS"; others may omit status.
            if r.status_code == 200 and status in (None, "OK", "ZERO_RESULTS"):
                return data

            # Retryable statuses
            if status in ("OVER_QUERY_LIMIT", "UNKNOWN_ERROR", "RESOURCE_EXHAUSTED"):
                last_message = f"status={status}, attempt={attempt}"
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue

            # Non-retryable error
            if r.status_code == 200 and status == "REQUEST_DENIED":
                err_msg = data.get("error_message") or ""
                extra = ""
                if "provided API key is invalid" in err_msg.lower():
                    extra = (
                        "\nPossible causes:\n"
                        "- The API key string passed is not the full key (no '...').\n"
                        "- The key belongs to a different Google Cloud project than the enabled APIs.\n"
                        "- Geocoding/Places APIs are not enabled for this key/project.\n"
                        "- Billing is not enabled for this project.\n"
                        "- Key restrictions (HTTP referrers / IP / apps) prevent server-to-server usage.\n"
                    )
                raise RuntimeError(
                    f"Google API error: url={url} status={status} http={r.status_code} error_message={err_msg}{extra}\nRaw body: {json.dumps(data)[:500]}"
                )

            raise RuntimeError(
                f"Google API error: url={url} status={status} http={r.status_code} body={json.dumps(data)[:400]}"
            )
        except requests.RequestException as e:
            last_message = f"request_exception={e}, attempt={attempt}"
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue

    raise RuntimeError(f"Failed after retries. Last message: {last_message}")


def geocode_postal_code(postal_code: str, api_key: str, country: str = "Canada") -> tuple[float, float, dict[str, Any] | None]:
    address = f"{postal_code}, {country}"
    params = {"address": address, "key": api_key}
    data = request_json(GOOGLE_GEOCODE_URL, params=params)
    if data.get("status") != "OK":
        raise RuntimeError(f"Geocoding failed for {address}: status={data.get('status')}")

    result0 = data["results"][0]
    loc = result0["geometry"]["location"]
    lat, lng = float(loc["lat"]), float(loc["lng"])
    bounds = result0.get("geometry", {}).get("bounds")
    return lat, lng, bounds


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def radius_from_bounds_m(bounds: dict[str, Any], center_lat: float, center_lng: float) -> int:
    ne = bounds["northeast"]
    sw = bounds["southwest"]
    d1 = haversine_m(center_lat, center_lng, float(ne["lat"]), float(ne["lng"]))
    d2 = haversine_m(center_lat, center_lng, float(sw["lat"]), float(sw["lng"]))
    return int(max(d1, d2))


def nearby_search_all_restaurants(
    lat: float,
    lng: float,
    radius_m: int,
    api_key: str,
    language: str = "en",
    type_keyword: str = "restaurant",
    page_delay_sec: float = 2.5,
) -> dict[str, dict[str, Any]]:
    """
    Collect all results returned by Nearby Search, using next_page_token pagination.
    Returns dict keyed by place_id.
    """
    places_by_id: dict[str, dict[str, Any]] = {}

    next_page_token: str | None = None
    while True:
        params: dict[str, Any] = {
            "key": api_key,
            "location": f"{lat},{lng}",
            "radius": radius_m,
            "type": type_keyword,
            "language": language,
        }
        if next_page_token:
            # For next_page_token requests, token needs a short delay before it becomes valid.
            params["pagetoken"] = next_page_token
            params.pop("radius", None)

        data = request_json(GOOGLE_NEARBY_URL, params=params)
        status = data.get("status")
        if status == "ZERO_RESULTS":
            return places_by_id
        if status != "OK":
            raise RuntimeError(f"Nearby search failed: status={status}")

        results = data.get("results", []) or []
        for p in results:
            pid = p.get("place_id")
            if pid:
                places_by_id[pid] = p

        next_page_token = data.get("next_page_token")
        if not next_page_token:
            break
        time.sleep(page_delay_sec)

    return places_by_id


def place_details(place_id: str, api_key: str, fields: list[str], language: str = "en") -> dict[str, Any] | None:
    params = {
        "key": api_key,
        "place_id": place_id,
        "language": language,
        "fields": ",".join(fields),
    }
    data = request_json(GOOGLE_DETAILS_URL, params=params)
    if data.get("status") != "OK":
        return None
    return data.get("result")


def slug_value(s: str) -> str:
    # Keep it simple for file naming.
    return "".join(ch if ch.isalnum() else "_" for ch in s).strip("_").lower() or "postal"


def main() -> None:
    ap = argparse.ArgumentParser(description="Get restaurants near a Canada postal code (Google Places API).")
    ap.add_argument("--postal-code", required=True, help="Full Canadian postal code (e.g. L4K 3H2)")
    ap.add_argument("--api-key", default=None, help="Google Maps API key (overrides GOOGLE_MAPS_API_KEY env var)")
    ap.add_argument("--api-key-file", default=None, help="Path to a file containing the API key (overrides env var)")
    ap.add_argument("--country", default="Canada", help="Geocoding country (default: Canada)")
    ap.add_argument("--language", default="en", help="Language for Google results (default: en)")

    ap.add_argument("--initial-radius-m", type=int, default=800, help="Start radius in meters (default: 800)")
    ap.add_argument("--step-radius-m", type=int, default=800, help="Radius increase per iteration (default: 800)")
    ap.add_argument("--max-radius-km", type=float, default=50.0, help="Safety cap for radius expansion (default: 50km)")
    ap.add_argument(
        "--stop-after-no-new",
        type=int,
        default=3,
        help="Stop after N consecutive expansions that yield 0 new place_ids (default: 3)",
    )
    ap.add_argument("--page-delay-sec", type=float, default=2.5, help="Delay before using next_page_token (default: 2.5)")

    ap.add_argument("--no-details", action="store_true", help="Skip Place Details calls (faster, fewer fields)")
    ap.add_argument("--sleep-between-details-sec", type=float, default=0.15, help="Delay between details calls (default: 0.15)")

    ap.add_argument("--out-json", default=None, help="Output JSON path (default: auto)")
    ap.add_argument("--out-csv", default=None, help="Output CSV path (default: auto)")
    ap.add_argument(
        "--fields",
        default=None,
        help="Comma-separated Place Details fields override (advanced). If omitted, uses defaults.",
    )
    args = ap.parse_args()

    api_key = (args.api_key or "").strip()
    if not api_key and args.api_key_file:
        p = args.api_key_file.strip()
        if not p:
            raise SystemExit("--api-key-file was empty")
        try:
            with open(p, "r", encoding="utf-8") as f:
                api_key = f.read().strip()
        except Exception as e:
            raise SystemExit(f"Failed to read --api-key-file={p}: {e}")
    api_key = api_key or (os.environ.get("GOOGLE_MAPS_API_KEY") or "").strip()
    if not api_key:
        raise SystemExit(
            "Missing API key. Provide one of:\n"
            '  --api-key "YOUR_KEY"\n'
            "or set environment variable:\n"
            '  export GOOGLE_MAPS_API_KEY="YOUR_KEY"'
        )
    if not looks_like_google_api_key(api_key):
        raise SystemExit(
            "API key does not look valid (still a placeholder or wrong string).\n"
            "Please paste the full real key from Google Cloud Console (no '...').\n"
            "Expected format typically starts with 'AIza'."
        )

    fields = DEFAULT_PLACE_DETAILS_FIELDS
    if args.fields:
        fields = [f.strip() for f in args.fields.split(",") if f.strip()]

    postal = args.postal_code.strip()
    ts = utc_now_iso()
    base_name = f"google_restaurants_{slug_value(postal)}"
    out_json = args.out_json or f"{base_name}.json"
    out_csv = args.out_csv or f"{base_name}.csv"

    print(f"Geocoding postal code: {postal} ({args.country})")
    center_lat, center_lng, bounds = geocode_postal_code(postal, api_key, country=args.country)

    # If we have bounds, use derived radius but never below user-provided initial radius.
    radius_m = args.initial_radius_m
    if bounds:
        derived = radius_from_bounds_m(bounds, center_lat, center_lng)
        radius_m = max(radius_m, derived)

    print(f"Center: lat={center_lat}, lng={center_lng} | starting radius={radius_m}m")

    all_place_ids: set[str] = set()
    all_places_raw: dict[str, dict[str, Any]] = {}

    consecutive_no_new = 0
    max_radius_m = int(args.max_radius_km * 1000)
    iteration = 0

    while radius_m <= max_radius_m:
        iteration += 1
        print(f"Nearby search iteration {iteration}: radius={radius_m}m")

        nearby = nearby_search_all_restaurants(
            lat=center_lat,
            lng=center_lng,
            radius_m=radius_m,
            api_key=api_key,
            language=args.language,
            type_keyword="restaurant",
            page_delay_sec=args.page_delay_sec,
        )

        new_count = 0
        for pid, p in nearby.items():
            if pid not in all_place_ids:
                new_count += 1
                all_place_ids.add(pid)
                all_places_raw[pid] = p

        print(f"  API returned {len(nearby)} places | new unique this round: {new_count} | total unique: {len(all_place_ids)}")

        if new_count == 0:
            consecutive_no_new += 1
        else:
            consecutive_no_new = 0

        if consecutive_no_new >= args.stop_after_no_new:
            print(f"Stopping radius expansion: {consecutive_no_new} consecutive rounds with 0 new places.")
            break

        radius_m += args.step_radius_m
        time.sleep(0.2)

    print(f"Total unique restaurants collected (place_ids): {len(all_place_ids)}")

    results: list[dict[str, Any]] = []
    for i, pid in enumerate(sorted(all_place_ids), start=1):
        base = all_places_raw.get(pid, {})
        nearby_loc = ((base.get("geometry") or {}).get("location") or {}) if base else {}
        lat = nearby_loc.get("lat")
        lng = nearby_loc.get("lng")

        if args.no_details:
            details: dict[str, Any] = {}
        else:
            time.sleep(args.sleep_between_details_sec)
            details = place_details(pid, api_key, fields=fields, language=args.language) or {}

        item = {
            "place_id": pid,
            "name": details.get("name") or base.get("name"),
            "formatted_address": details.get("formatted_address"),
            "lat": ((details.get("geometry") or {}).get("location") or {}).get("lat", lat),
            "lng": ((details.get("geometry") or {}).get("location") or {}).get("lng", lng),
            "international_phone_number": details.get("international_phone_number"),
            "formatted_phone_number": details.get("formatted_phone_number"),
            "website": details.get("website"),
            "url": details.get("url") or base.get("url"),
            "opening_hours": details.get("opening_hours"),
            "types": base.get("types"),
            "vicinity": base.get("vicinity"),
        }

        results.append(item)
        if i % 20 == 0:
            print(f"  Details processed: {i}/{len(all_place_ids)}")

    payload = {
        "source": "Google Maps Platform (Geocoding + Places Nearby Search + Place Details)",
        "postal_code": postal,
        "country": args.country,
        "center": {"lat": center_lat, "lng": center_lng},
        "timestamp_utc": ts,
        "restaurants_count": len(results),
        "results": results,
    }

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "place_id",
                "name",
                "formatted_address",
                "lat",
                "lng",
                "international_phone_number",
                "formatted_phone_number",
                "website",
                "url",
                "opening_hours",
                "vicinity",
                "types",
            ],
        )
        writer.writeheader()
        for row in results:
            writer.writerow(row)

    print("Saved:")
    print(f"  JSON: {out_json}")
    print(f"  CSV:  {out_csv}")


if __name__ == "__main__":
    main()

