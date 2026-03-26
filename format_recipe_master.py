#!/usr/bin/env python3
"""
Build one unified master recipe file from Allrecipes + BBC JSON files.

Default scan roots (if present):
  - allrecipes_by_cuisine/
  - bbc_by_cuisine/
  - bbc_recipes/

Output:
  - recipes_master_formatted.json

Notes:
  - Keeps one consistent schema for all sources.
  - Dedupe key: canonical_url (preferred) else source+title.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


OUT_DEFAULT = Path("recipes_master_formatted.json")
DEFAULT_INPUT_DIRS = [
    Path("allrecipes_by_cuisine"),
    Path("bbc_by_cuisine"),
    Path("bbc_recipes"),
]

CANON_KEYS = [
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
    "source",
]


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _guess_source(path: Path, rec: dict[str, Any]) -> str:
    s = (str(path).lower(), str(rec.get("host", "")).lower(), str(rec.get("canonical_url", "")).lower())
    txt = " ".join(s)
    if "bbc" in txt:
        return "bbc"
    if "allrecipes" in txt:
        return "allrecipes"
    return "unknown"


def _infer_cuisine_from_filename(name: str) -> str | None:
    n = re.sub(r"\.json$", "", name, flags=re.IGNORECASE)
    n = n.replace("_Recipes", "").replace("_", " ").strip()
    return n or None


def _to_list(value: Any) -> list[str] | None:
    if isinstance(value, list):
        out = [str(x).strip() for x in value if str(x).strip()]
        return out or None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # split on newlines or numbered steps
        parts = [p.strip() for p in re.split(r"(?:\n+|(?<=\.)\s+(?=\d+\.)|(?<=\.)\s+(?=[A-Z]))", s) if p.strip()]
        return parts or [s]
    return None


def _normalize_recipe(path: Path, rec: dict[str, Any]) -> dict[str, Any] | None:
    title = rec.get("title") or rec.get("name")
    ingredients = rec.get("ingredients") or rec.get("recipeIngredient")
    instructions = rec.get("instructions") or rec.get("method") or rec.get("directions")
    canonical_url = rec.get("canonical_url") or rec.get("url") or rec.get("link")

    if not title and not ingredients and not canonical_url:
        return None

    out: dict[str, Any] = dict(rec)
    out["title"] = title
    out["ingredients"] = ingredients if isinstance(ingredients, list) else _to_list(ingredients)
    out["instructions"] = instructions
    out["instructions_list"] = rec.get("instructions_list") or _to_list(instructions)
    out["yields"] = rec.get("yields") or rec.get("serves")
    out["total_time"] = rec.get("total_time") or rec.get("totalTime")
    out["prep_time"] = rec.get("prep_time") or rec.get("prepTime")
    out["cook_time"] = rec.get("cook_time") or rec.get("cookTime")
    out["image"] = rec.get("image")
    out["ratings"] = rec.get("ratings") or rec.get("rating")
    out["reviews"] = rec.get("reviews") or rec.get("review_count")
    out["cuisine"] = rec.get("cuisine") or _infer_cuisine_from_filename(path.name)
    out["category"] = rec.get("category")
    out["author"] = rec.get("author")
    out["host"] = rec.get("host")
    out["canonical_url"] = canonical_url
    out["nutrition"] = rec.get("nutrition")
    out["source"] = rec.get("source") or _guess_source(path, rec)

    # canonical shape keys
    for k in CANON_KEYS:
        out.setdefault(k, None)

    # still not recipe-like
    if not out.get("title") and not out.get("ingredients"):
        return None
    return out


def _iter_recipe_dicts(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict) and isinstance(data.get("recipes"), list):
        return [x for x in data["recipes"] if isinstance(x, dict)]
    return []


def main() -> None:
    ap = argparse.ArgumentParser(description="Format Allrecipes + BBC JSON into one master file.")
    ap.add_argument("--input-dir", action="append", dest="input_dirs", default=[], help="Repeatable input directory")
    ap.add_argument("--out", type=Path, default=OUT_DEFAULT)
    ap.add_argument("--dedupe", action="store_true", default=True)
    args = ap.parse_args()

    input_dirs = [Path(p) for p in args.input_dirs] if args.input_dirs else DEFAULT_INPUT_DIRS
    files: list[Path] = []
    for d in input_dirs:
        if d.exists():
            files.extend(sorted(d.glob("*.json")))

    if not files:
        raise SystemExit("No input JSON files found in configured directories.")

    master: list[dict[str, Any]] = []
    seen: set[str] = set()
    kept_files = 0
    read_rows = 0

    for jf in files:
        low = jf.name.lower()
        if "analyzed" in low or "skip_to_content" in low:
            continue
        try:
            data = _load_json(jf)
        except Exception as e:
            print(f"[skip] {jf}: {e!s}")
            continue
        rows = _iter_recipe_dicts(data)
        if not rows:
            continue
        kept_files += 1
        for r in rows:
            read_rows += 1
            nr = _normalize_recipe(jf, r)
            if not nr:
                continue
            if args.dedupe:
                k = (nr.get("canonical_url") or "").strip()
                if not k:
                    k = f"{nr.get('source','unknown')}::{(nr.get('title') or '').strip().lower()}"
                if k in seen:
                    continue
                seen.add(k)
            master.append(nr)

    args.out.write_text(json.dumps(master, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[done] files={kept_files} rows_scanned={read_rows} master={len(master)} -> {args.out}")


if __name__ == "__main__":
    main()

