#!/usr/bin/env python3
"""
Fetch McDonald's restaurant locations in Canada via the JSON endpoint used by
https://www.mcdonalds.com/ca/en-ca/restaurant-locator.html

API:
  GET https://www.mcdonalds.com/googleappsv2/geolocation
      ?latitude=...&longitude=...&radius=...&maxResults=...&country=ca&language=en-ca

Design (efficient + safe for handoff):
  - Major-city seeds first, then a tiered lat/lon grid (dense south, coarse north).
  - Optional --adaptive: when a response is "saturated" (many features), enqueue
    four sub-cells so dense clusters are less likely to miss edge stores.
  - Dedupe by NSN (national store number) when present; stable fallback otherwise.
  - Each output row includes flat address fields plus the raw API `feature`.
  - Atomic writes + periodic checkpoints so crashes do not corrupt output.
  - Refuses to overwrite the final file with an empty list (unless --allow-empty).

Usage:
  python3 fetch_mcdonalds_canada_locations.py --smoke-test
  python3 fetch_mcdonalds_canada_locations.py --adaptive --out mcdonalds_canada_locations.json
  python3 fetch_mcdonalds_canada_locations.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

API_URL = "https://www.mcdonalds.com/googleappsv2/geolocation"
REFERER = "https://www.mcdonalds.com/ca/en-ca/restaurant-locator.html"
DEFAULT_OUT = Path("mcdonalds_canada_locations.json")

# If default TLS fails once (broken CA store), reuse insecure context for the rest of the run.
_TLS_FALLBACK_CTX: ssl.SSLContext | None = None
_TLS_FALLBACK_WARNED = False

# Above this many features, the "nearest N" response may be saturated — subdivide if --adaptive.
ADAPTIVE_SATURATION_THRESHOLD = 82


def _frange(start: float, stop: float, step: float) -> Iterator[float]:
    x = start
    if step <= 0:
        raise ValueError("step must be > 0")
    while x < stop - 1e-9:
        yield round(x, 6)
        x += step


@dataclass(frozen=True)
class GridTier:
    lat_min: float
    lat_max: float
    lat_step: float
    lon_min: float
    lon_max: float
    lon_step: float


DEFAULT_TIERS = (
    GridTier(42.0, 55.0, 0.75, -128.0, -60.0, 1.0),
    GridTier(55.0, 72.0, 1.5, -132.0, -52.0, 2.5),
)

SEED_POINTS: tuple[tuple[float, float], ...] = (
    (43.6532, -79.3832),
    (45.5017, -73.5673),
    (49.2827, -123.1207),
    (51.0447, -114.0719),
    (53.5461, -113.4938),
    (45.4215, -75.6972),
    (45.9636, -66.6431),
    (44.6488, -63.5752),
    (47.5615, -52.7126),
    (48.4284, -89.2477),
    (50.4452, -104.6189),
    (52.1579, -106.6702),
    (49.8951, -97.1384),
    (46.8139, -71.2080),
    (42.3149, -83.0364),
    (43.4516, -80.4925),
    (44.2312, -76.4860),
    (46.4927, -80.9930),
    (44.3894, -79.6903),
    (62.4540, -114.3718),
    (69.1107, -105.0624),
)


def iter_grid_cells(tiers: tuple[GridTier, ...]) -> Iterator[tuple[float, float]]:
    for t in tiers:
        for lat in _frange(t.lat_min, t.lat_max, t.lat_step):
            for lon in _frange(t.lon_min, t.lon_max, t.lon_step):
                yield lat, lon


def build_cell_list(tiers: tuple[GridTier, ...]) -> list[tuple[float, float]]:
    seen: set[tuple[float, float]] = set()
    out: list[tuple[float, float]] = []
    for lat, lon in (*SEED_POINTS, *iter_grid_cells(tiers)):
        key = (round(lat, 4), round(lon, 4))
        if key in seen:
            continue
        seen.add(key)
        out.append((lat, lon))
    return out


@dataclass
class CellTask:
    lat: float
    lon: float
    depth: int = 0


def _headers() -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Referer": REFERER,
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/plain, */*",
    }


def _ssl_context(insecure_tls: bool) -> ssl.SSLContext | None:
    if insecure_tls:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx
    return None


def fetch_cell(
    lat: float,
    lon: float,
    *,
    radius: int,
    max_results: int,
    timeout: float,
    insecure_tls: bool,
) -> list[dict[str, Any]]:
    global _TLS_FALLBACK_CTX, _TLS_FALLBACK_WARNED
    params = {
        "latitude": f"{lat:.6f}",
        "longitude": f"{lon:.6f}",
        "radius": str(radius),
        "maxResults": str(max_results),
        "country": "ca",
        "language": "en-ca",
    }
    url = f"{API_URL}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers=_headers(), method="GET")
    ctx = _ssl_context(insecure_tls) or _TLS_FALLBACK_CTX
    try:
        resp = urllib.request.urlopen(req, timeout=timeout, context=ctx)
    except urllib.error.URLError as e:
        err = str(e).lower()
        if not insecure_tls and "certificate verify failed" in err and _TLS_FALLBACK_CTX is None:
            _TLS_FALLBACK_CTX = ssl.create_default_context()
            _TLS_FALLBACK_CTX.check_hostname = False
            _TLS_FALLBACK_CTX.verify_mode = ssl.CERT_NONE
            if not _TLS_FALLBACK_WARNED:
                print(
                    "[mcd] WARN: TLS certificate verify failed; using insecure TLS for the rest of this run. "
                    "Install CA certs for Python (e.g. Install Certificates.command on macOS) or pass --insecure-tls."
                )
                _TLS_FALLBACK_WARNED = True
            resp = urllib.request.urlopen(req, timeout=timeout, context=_TLS_FALLBACK_CTX)
        else:
            raise
    with resp:
        raw = resp.read().decode("utf-8", "replace")
    data = json.loads(raw)
    feats = data.get("features")
    if not isinstance(feats, list):
        return []
    return [x for x in feats if isinstance(x, dict)]


def _nsn_key(feature: dict[str, Any]) -> str | None:
    props = feature.get("properties")
    if not isinstance(props, dict):
        return None
    ident = props.get("identifiers")
    if not isinstance(ident, dict):
        return None
    sids = ident.get("storeIdentifier")
    if not isinstance(sids, list):
        return None
    for item in sids:
        if isinstance(item, dict) and item.get("identifierType") == "NSN":
            v = item.get("identifierValue")
            if v is not None:
                return str(v)
    return None


def _fallback_key(feature: dict[str, Any]) -> str:
    props = feature.get("properties") if isinstance(feature.get("properties"), dict) else {}
    pid = props.get("id")
    if pid:
        return f"id:{pid}"
    geom = feature.get("geometry")
    if isinstance(geom, dict):
        c = geom.get("coordinates")
        if isinstance(c, list) and len(c) >= 2:
            return f"coord:{c[0]!s},{c[1]!s}"
    return json.dumps(feature, sort_keys=True)[:200]


def _clean_str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _flat_from_properties(props: dict[str, Any]) -> dict[str, Any]:
    """Human-readable fields for spreadsheets / seniors; source remains in feature."""
    line1 = _clean_str(props.get("addressLine1"))
    line2 = _clean_str(props.get("addressLine2"))
    line3 = _clean_str(props.get("addressLine3"))
    line4 = _clean_str(props.get("addressLine4"))
    custom = _clean_str(props.get("customAddress"))
    parts = [p for p in (line1, line2, line3, line4) if p]
    full = ", ".join(parts) if parts else None
    if not full and custom:
        full = custom
    return {
        "name": _clean_str(props.get("name") or props.get("shortDescription")),
        "address_line1": line1,
        "address_line2": line2,
        "city": line3,
        "region": _clean_str(props.get("subDivision")),
        "postal_code": _clean_str(props.get("postcode")),
        "phone": _clean_str(props.get("telephone")),
        "custom_address": custom,
        "full_address": full,
    }


def record_is_complete(rec: dict[str, Any]) -> bool:
    """Drop rows that would be useless empty shells."""
    if rec.get("latitude") is None or rec.get("longitude") is None:
        return False
    if not rec.get("store_key"):
        return False
    flat = rec.get("flat") if isinstance(rec.get("flat"), dict) else {}
    if (
        not flat.get("postal_code")
        and not flat.get("address_line1")
        and not flat.get("full_address")
        and not flat.get("custom_address")
    ):
        return False
    feat = rec.get("feature")
    if not isinstance(feat, dict) or not isinstance(feat.get("properties"), dict):
        return False
    return True


def normalize_feature(feature: dict[str, Any], *, fetched_at: str) -> dict[str, Any]:
    props = feature.get("properties") if isinstance(feature.get("properties"), dict) else {}
    geom = feature.get("geometry") if isinstance(feature.get("geometry"), dict) else {}
    coords = geom.get("coordinates")
    lon = lat = None
    if isinstance(coords, list) and len(coords) >= 2:
        try:
            lon = float(coords[0])
            lat = float(coords[1])
        except (TypeError, ValueError):
            lon = lat = None
    pid = props.get("id")
    detail_url = None
    if isinstance(pid, str) and pid.strip():
        detail_url = f"https://www.mcdonalds.com/ca/en-ca/location/{urllib.parse.quote(pid, safe=':')}"
    return {
        "chain": "McDonald's",
        "country": "CA",
        "source": "mcdonalds.com_googleappsv2_geolocation",
        "fetched_at_utc": fetched_at,
        "store_key": _nsn_key(feature) or _fallback_key(feature),
        "latitude": lat,
        "longitude": lon,
        "detail_url": detail_url,
        "flat": _flat_from_properties(props),
        "feature": feature,
    }


def atomic_write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def write_sidecar_meta(path: Path, meta: dict[str, Any]) -> None:
    side = path.with_name(path.stem + ".meta.json")
    atomic_write_json(side, meta)


def subdivide_offsets(lat_step_hint: float, lon_step_hint: float) -> tuple[float, float]:
    """Half-step offsets in degrees (approximate sub-cells)."""
    return max(lat_step_hint * 0.35, 0.08), max(lon_step_hint * 0.35, 0.08)


def run_fetch(
    *,
    out: Path,
    radius: int,
    max_results: int,
    delay: float,
    timeout: float,
    retries: int,
    insecure_tls: bool,
    max_cells: int,
    adaptive: bool,
    adaptive_max_depth: int,
    checkpoint_every: int,
    allow_empty: bool,
    smoke_test: bool,
) -> int:
    if smoke_test:
        max_cells = min(max_cells or 35, 35)
        adaptive = True
        adaptive_max_depth = min(adaptive_max_depth, 2)
        print("[mcd] smoke-test: seeds + first ~35 grid cells, adaptive depth<=2")

    base_cells = build_cell_list(DEFAULT_TIERS)
    queue: deque[CellTask] = deque()
    for lat, lon in base_cells:
        queue.append(CellTask(lat, lon, 0))

    total_planned_base = len(base_cells)
    processed = 0
    subdivisions_enqueued = 0

    by_key: dict[str, dict[str, Any]] = {}
    t0 = time.time()
    fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    checkpoint_path = out.with_suffix(out.suffix + ".checkpoint.json")

    lat_hint = DEFAULT_TIERS[0].lat_step
    lon_hint = DEFAULT_TIERS[0].lon_step

    while queue:
        if max_cells and processed >= max_cells:
            break
        task = queue.popleft()
        lat, lon, depth = task.lat, task.lon, task.depth
        processed += 1

        attempt = 0
        feats: list[dict[str, Any]] = []
        while True:
            attempt += 1
            try:
                feats = fetch_cell(
                    lat,
                    lon,
                    radius=radius,
                    max_results=max_results,
                    timeout=timeout,
                    insecure_tls=insecure_tls,
                )
                break
            except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
                if attempt >= retries:
                    print(f"[mcd] FAIL cell {processed} ({lat:.4f},{lon:.4f}): {e!s}")
                    feats = []
                    break
                time.sleep(min(2.0 * attempt, 20.0))

        new = 0
        for f in feats:
            k = _nsn_key(f) or _fallback_key(f)
            rec = normalize_feature(f, fetched_at=fetched_at)
            if not record_is_complete(rec):
                continue
            if k not in by_key:
                new += 1
                by_key[k] = rec

        saturated = adaptive and len(feats) >= ADAPTIVE_SATURATION_THRESHOLD
        if saturated and depth < adaptive_max_depth:
            dlat, dlon = subdivide_offsets(lat_hint, lon_hint)
            for sx, sy in reversed(((1, 1), (1, -1), (-1, 1), (-1, -1))):
                queue.appendleft(CellTask(lat + sx * dlat, lon + sy * dlon, depth + 1))
            subdivisions_enqueued += 4

        if processed % 25 == 0 or new or saturated:
            print(
                f"[mcd] {processed} done, queue={len(queue)} "
                f"({lat:.2f},{lon:.2f}) +feat={len(feats)} +new={new} "
                f"unique={len(by_key)} sat={saturated}"
            )

        if checkpoint_every and by_key and processed % checkpoint_every == 0:
            out_list = sorted(by_key.values(), key=lambda r: (r.get("store_key") or ""))
            atomic_write_json(checkpoint_path, out_list)
            print(f"[mcd] checkpoint -> {checkpoint_path} ({len(out_list)} rows)")

        time.sleep(delay)

    out_list = sorted(by_key.values(), key=lambda r: (r.get("store_key") or ""))

    if not out_list and not allow_empty:
        print(
            "[mcd] ERROR: no valid locations collected; refusing to write empty output. "
            "Fix network/TLS or run with a smaller --max-cells to debug.",
            file=sys.stderr,
        )
        return 1

    atomic_write_json(out, out_list)
    elapsed = time.time() - t0
    meta = {
        "schema_version": 1,
        "generated_at_utc": fetched_at,
        "output_file": str(out.resolve()),
        "unique_locations": len(out_list),
        "cells_processed": processed,
        "base_grid_cells": total_planned_base,
        "adaptive": adaptive,
        "adaptive_max_depth": adaptive_max_depth,
        "subdivision_tasks_enqueued": subdivisions_enqueued,
        "elapsed_seconds": round(elapsed, 1),
        "notes": "Each row includes `flat` (readable fields) and `feature` (raw API).",
    }
    write_sidecar_meta(out, meta)
    print(f"[mcd] done: unique_stores={len(out_list)} -> {out} ({elapsed:.1f}s)")
    print(f"[mcd] meta -> {out.with_name(out.stem + '.meta.json')}")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch McDonald's Canada locations as JSON.")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--radius", type=int, default=250_000)
    ap.add_argument("--max-results", type=int, default=500)
    ap.add_argument("--delay", type=float, default=0.18)
    ap.add_argument("--timeout", type=float, default=45.0)
    ap.add_argument("--retries", type=int, default=4)
    ap.add_argument("--insecure-tls", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--max-cells", type=int, default=0, help="Cap processed cells (0 = no cap).")
    ap.add_argument(
        "--adaptive",
        action="store_true",
        help="Subdivide cells when the API returns a saturated nearest-neighbour page.",
    )
    ap.add_argument("--adaptive-max-depth", type=int, default=3)
    ap.add_argument("--checkpoint-every", type=int, default=75, help="0 disables checkpoints.")
    ap.add_argument(
        "--allow-empty",
        action="store_true",
        help="Write [] if nothing valid was collected (default: exit with error instead).",
    )
    ap.add_argument(
        "--smoke-test",
        action="store_true",
        help="Quick validation run: limited cells + adaptive; writes to --out.",
    )
    args = ap.parse_args()

    base = build_cell_list(DEFAULT_TIERS)
    print(f"[mcd] base grid cells (seeds + deduped): {len(base)}")
    if args.dry_run:
        return

    code = run_fetch(
        out=args.out,
        radius=args.radius,
        max_results=args.max_results,
        delay=args.delay,
        timeout=args.timeout,
        retries=args.retries,
        insecure_tls=args.insecure_tls,
        max_cells=args.max_cells,
        adaptive=args.adaptive or args.smoke_test,
        adaptive_max_depth=args.adaptive_max_depth,
        checkpoint_every=args.checkpoint_every,
        allow_empty=args.allow_empty,
        smoke_test=args.smoke_test,
    )
    raise SystemExit(code)


if __name__ == "__main__":
    main()
