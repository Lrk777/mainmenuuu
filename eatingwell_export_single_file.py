#!/usr/bin/env python3
"""
Merge EatingWell scrape outputs into one combined JSON file.

Primary input (recommended): eatingwell_scrape_out/recipes.jsonl
Each line should be a JSON object for one recipe (the scraper writes this).

This script:
  - Deduplicates by `url`
  - Writes a single JSON array file (default: recipes_all.json)
  - Supports --watch so the combined file stays updated while scraping runs

Safe while scraping: this script only reads recipes.jsonl and writes its own
output file. It does not modify the scraper output directory.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any


def _is_recipe_dict(obj: Any) -> bool:
    if not isinstance(obj, dict):
        return False
    url = obj.get("url")
    if not isinstance(url, str) or not url:
        return False
    # "Real recipe" heuristic: scraper records include either full schema or ingredients list.
    rjl = obj.get("recipe_json_ld")
    if isinstance(rjl, dict):
        return True
    ing = obj.get("ingredients")
    if isinstance(ing, list) and ing:
        return True
    title = obj.get("title")
    if isinstance(title, str) and title.strip():
        # Some records might have title only; accept to avoid accidental drops.
        return True
    return False


def _atomic_write_json(path: Path, data: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def _read_existing_cache(output_file: Path) -> OrderedDict[str, dict[str, Any]]:
    if not output_file.exists():
        return OrderedDict()
    try:
        arr = json.loads(output_file.read_text(encoding="utf-8"))
    except Exception:
        return OrderedDict()
    od: OrderedDict[str, dict[str, Any]] = OrderedDict()
    if isinstance(arr, list):
        for item in arr:
            if _is_recipe_dict(item):
                url = item["url"]
                od[url] = item
    return od


def _load_state(state_file: Path) -> dict[str, Any]:
    if not state_file.exists():
        return {"jsonl_offset": 0}
    try:
        return json.loads(state_file.read_text(encoding="utf-8"))
    except Exception:
        return {"jsonl_offset": 0}


def _save_state(state_file: Path, state: dict[str, Any]) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _merge_from_recipes_jsonl(
    jsonl_path: Path,
    cache: OrderedDict[str, dict[str, Any]],
    *,
    offset: int,
) -> tuple[int, int]:
    """
    Read from jsonl_path starting at byte offset, update cache in-place.
    Returns (new_offset, merged_count).
    """
    merged = 0
    # If file got truncated, we cannot seek past end.
    offset = min(offset, jsonl_path.stat().st_size) if jsonl_path.exists() else offset
    with jsonl_path.open("r", encoding="utf-8", errors="replace") as f:
        f.seek(offset)
        while True:
            pos = f.tell()
            line = f.readline()
            if not line:
                new_offset = f.tell()
                break
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                # If the scraper wrote a partial line (rare), skip it this cycle.
                # Next loop will pick up once a full line exists.
                # Using pos not new_offset keeps offset stable even if parse fails.
                continue

            if _is_recipe_dict(obj):
                url = obj["url"]
                if url not in cache:
                    merged += 1
                cache[url] = obj
                # Maintain stable order for existing URLs; new URLs append.
    return new_offset, merged


def main() -> None:
    ap = argparse.ArgumentParser(description="Merge EatingWell scrape outputs into one file.")
    ap.add_argument(
        "--dir",
        type=Path,
        default=Path("eatingwell_scrape_out"),
        help="Scraper output directory (contains recipes.jsonl).",
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output JSON file. Default: <dir>/recipes_all.json",
    )
    ap.add_argument(
        "--state",
        type=Path,
        default=None,
        help="Merge state file. Default: <dir>/merge_state.json",
    )
    ap.add_argument("--watch", action="store_true", help="Keep updating output while scraper runs.")
    ap.add_argument("--interval", type=float, default=10.0, help="Seconds between merges in --watch mode.")
    ap.add_argument("--wait-for-jsonl", action="store_true", help="Wait until recipes.jsonl appears.")
    ap.add_argument("--reset", action="store_true", help="Start fresh (ignore cached output + offsets).")
    args = ap.parse_args()

    out_dir = args.dir
    jsonl_path = out_dir / "recipes.jsonl"
    output_file = args.output or (out_dir / "recipes_all.json")
    state_file = args.state or (out_dir / "merge_state.json")

    cache: OrderedDict[str, dict[str, Any]]
    offset = 0
    if args.reset:
        cache = OrderedDict()
        offset = 0
        if state_file.exists():
            state_file.unlink()
    else:
        cache = _read_existing_cache(output_file)
        state = _load_state(state_file)
        offset = int(state.get("jsonl_offset", 0) or 0)

    def merge_once() -> None:
        nonlocal offset, cache
        if not jsonl_path.exists():
            return
        new_offset, merged_count = _merge_from_recipes_jsonl(
            jsonl_path, cache, offset=offset
        )
        offset = new_offset
        _atomic_write_json(output_file, list(cache.values()))
        _save_state(
            state_file,
            {
                "jsonl_offset": offset,
                "records": len(cache),
                "merged_new_since_start": merged_count,
            },
        )
        print(f"[export] wrote {len(cache)} recipes -> {output_file} (offset={offset})")

    if args.wait_for_jsonl:
        while not jsonl_path.exists():
            print(f"[export] waiting for {jsonl_path} ...")
            time.sleep(args.interval)

    if not args.watch:
        merge_once()
        return

    # Watch mode.
    while True:
        try:
            merge_once()
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"[export] error: {e!s}")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()

