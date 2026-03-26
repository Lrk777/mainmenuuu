#!/usr/bin/env python3
"""
Normalize and combine Allrecipes "cuisine wise" JSON files.

Input:  /allrecipes_by_cuisine/*.json  (each file is typically a JSON array of recipes)
Output:
  formatted_by_cuisine/<OriginalFile>.json  (normalized recipes, same per-file granularity)
  allrecipes_master_formatted.json           (single deduped list across all cuisines)

Why:
  - Keeps everything in one consistent schema.
  - Produces a single file you can use downstream (nutrition, analysis, etc).
  - Skips non-recipe JSON (ex: skip_to_content, analysis outputs) using heuristics.

Dedupe:
  - Master file dedupes by `canonical_url` when present.
  - Otherwise falls back to a (title, cuisine) key.

Usage:
  python3 format_allrecipes_by_cuisine.py
  python3 format_allrecipes_by_cuisine.py --input-dir allrecipes_by_cuisine --dedupe
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Iterable


DEFAULT_INPUT_DIR = Path("allrecipes_by_cuisine")
DEFAULT_OUT_DIR = Path("formatted_by_cuisine")
DEFAULT_MASTER_OUT = Path("allrecipes_master_formatted.json")


CANONICAL_KEYS = [
    "title",
    "description",
    "ingredients",
    "instructions",
    "instructions_list",
    "yields",
    "total_time",
    "prep_time",
    "cook_time",
    "image",
    "ratings",
    "reviews",
    "cuisine",
    "category",
    "author",
    "host",
    "canonical_url",
    "nutrition",
]


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _candidate_recipes(data: Any) -> list[dict[str, Any]]:
    """
    Return a list of recipe-like dicts from arbitrary JSON content.
    We only accept dict items that look like recipes.
    """
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        # Some dumps might wrap recipes.
        if isinstance(data.get("recipes"), list):
            items = data["recipes"]
        else:
            return []
    else:
        return []

    out: list[dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        # Heuristic: real recipes have ingredients and/or canonical_url
        if it.get("ingredients") or it.get("canonical_url"):
            out.append(it)
    return out


def _normalize_recipe(r: dict[str, Any], *, inferred_cuisine: str | None) -> dict[str, Any]:
    # Keep all existing fields, but ensure the canonical schema exists.
    # Also fill cuisine if missing.
    out: dict[str, Any] = dict(r)

    if inferred_cuisine and not out.get("cuisine"):
        out["cuisine"] = inferred_cuisine

    # If instructions_list missing but instructions exists as a string, split into lines.
    if not out.get("instructions_list") and isinstance(out.get("instructions"), str):
        instr = out["instructions"].strip()
        if instr:
            # Preserve original line breaks; if it's a single paragraph, keep as single item.
            lines = [ln.strip() for ln in instr.splitlines() if ln.strip()]
            out["instructions_list"] = lines or [instr]

    # Ensure at least canonical keys exist (as nulls) for consistent downstream processing.
    for k in CANONICAL_KEYS:
        out.setdefault(k, None)

    return out


def _infer_cuisine_from_filename(filename: str) -> str | None:
    # Examples:
    # - African_Recipes.json -> African
    # - Middle_Eastern.json -> Middle Eastern
    # - Cajun_and_Creole_Recipes.json -> Cajun and Creole
    name = filename
    name = re.sub(r"\.json$", "", name, flags=re.IGNORECASE)
    name = name.replace("_Recipes", "")
    name = name.replace("_", " ")
    name = name.strip()
    return name or None


def _dedupe_key(r: dict[str, Any]) -> str:
    cu = r.get("canonical_url")
    if isinstance(cu, str) and cu.strip():
        return cu.strip()
    title = r.get("title") or ""
    cuisine = r.get("cuisine") or ""
    return f"{title}::{cuisine}"


def iter_json_files(input_dir: Path, *, skip_analyzed: bool) -> Iterable[Path]:
    for p in sorted(input_dir.glob("*.json")):
        low = p.name.lower()
        if skip_analyzed and ("_analyzed.json" in low or "analyzed" in low):
            continue
        if "skip_to_content" in low:
            continue
        yield p


def main() -> None:
    ap = argparse.ArgumentParser(description="Format Allrecipes cuisine JSON files consistently.")
    ap.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    ap.add_argument("--master-out", type=Path, default=DEFAULT_MASTER_OUT)
    ap.add_argument("--dedupe", action="store_true", help="Deduplicate master list by canonical_url/title+cuisine.")
    ap.add_argument("--skip-analyzed", action="store_true", default=True, help="Skip *_analyzed.json files.")
    args = ap.parse_args()

    if not args.input_dir.exists():
        raise SystemExit(f"Input dir not found: {args.input_dir}")

    args.out_dir.mkdir(parents=True, exist_ok=True)
    master: list[dict[str, Any]] = []
    seen: set[str] = set()

    kept_files = 0
    total_recipes = 0

    for jf in iter_json_files(args.input_dir, skip_analyzed=args.skip_analyzed):
        try:
            data = _load_json(jf)
        except Exception as e:
            print(f"[format] skip {jf.name}: JSON load error: {e!s}")
            continue

        recipes = _candidate_recipes(data)
        if not recipes:
            continue

        inferred_cuisine = _infer_cuisine_from_filename(jf.name)
        normalized = [_normalize_recipe(r, inferred_cuisine=inferred_cuisine) for r in recipes]

        kept_files += 1
        total_recipes += len(normalized)

        # Write per-cuisine formatted file
        out_path = args.out_dir / jf.name
        out_path.write_text(json.dumps(normalized, indent=2, ensure_ascii=False), encoding="utf-8")

        # Add to master
        if args.dedupe:
            for r in normalized:
                k = _dedupe_key(r)
                if k in seen:
                    continue
                seen.add(k)
                master.append(r)
        else:
            master.extend(normalized)

        print(f"[format] {jf.name}: {len(recipes)} recipes -> {out_path}")

    args.master_out.parent.mkdir(parents=True, exist_ok=True)
    args.master_out.write_text(json.dumps(master, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"[format] done. Kept files: {kept_files}, total recipes scanned: {total_recipes}, master: {len(master)}")


if __name__ == "__main__":
    main()

