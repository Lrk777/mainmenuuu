#!/usr/bin/env python3
"""
Estimate nutrition + diet/allergen tags from recipe ingredients/instructions.

Designed for JSON arrays like:
[
  {
    "title": "...",
    "ingredients": ["2 cups rice", "1 tbsp olive oil", ...],
    "instructions": "...",
    "yields": "8 servings"
  },
  ...
]

What it calculates (estimated):
  - Calories, protein, carbs, fat, fiber, sugar (total and per serving)
  - Health score (0-100) based on density heuristics
  - Dietary flags: vegan, vegetarian, gluten_free, nut_free, dairy_free, egg_free

Notes:
  - This is a heuristic estimator, not a clinical nutrition engine.
  - Unknown ingredients are tracked in output.
"""

from __future__ import annotations

import argparse
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


# -----------------------------
# Nutrition DB (per 100g)
# -----------------------------
NUTRITION_DB = {
    "olive oil": {"kcal": 884, "protein": 0.0, "carbs": 0.0, "fat": 100.0, "fiber": 0.0, "sugar": 0.0},
    "butter": {"kcal": 717, "protein": 0.9, "carbs": 0.1, "fat": 81.1, "fiber": 0.0, "sugar": 0.1},
    "rice": {"kcal": 365, "protein": 7.1, "carbs": 80.0, "fat": 0.7, "fiber": 1.3, "sugar": 0.1},
    "jasmine rice": {"kcal": 365, "protein": 7.1, "carbs": 80.0, "fat": 0.7, "fiber": 1.3, "sugar": 0.1},
    "brown rice": {"kcal": 370, "protein": 7.9, "carbs": 77.0, "fat": 2.9, "fiber": 3.5, "sugar": 0.8},
    "onion": {"kcal": 40, "protein": 1.1, "carbs": 9.3, "fat": 0.1, "fiber": 1.7, "sugar": 4.2},
    "garlic": {"kcal": 149, "protein": 6.4, "carbs": 33.1, "fat": 0.5, "fiber": 2.1, "sugar": 1.0},
    "celery": {"kcal": 16, "protein": 0.7, "carbs": 3.0, "fat": 0.2, "fiber": 1.6, "sugar": 1.3},
    "green onion": {"kcal": 32, "protein": 1.8, "carbs": 7.3, "fat": 0.2, "fiber": 2.6, "sugar": 2.3},
    "raisin": {"kcal": 299, "protein": 3.1, "carbs": 79.0, "fat": 0.5, "fiber": 3.7, "sugar": 59.0},
    "chicken stock": {"kcal": 17, "protein": 0.9, "carbs": 1.0, "fat": 0.7, "fiber": 0.0, "sugar": 0.2},
    "chicken broth": {"kcal": 15, "protein": 1.0, "carbs": 1.0, "fat": 0.5, "fiber": 0.0, "sugar": 0.2},
    "tomato": {"kcal": 18, "protein": 0.9, "carbs": 3.9, "fat": 0.2, "fiber": 1.2, "sugar": 2.6},
    "tomato paste": {"kcal": 82, "protein": 4.3, "carbs": 19.0, "fat": 0.5, "fiber": 4.1, "sugar": 12.0},
    "crushed tomatoes": {"kcal": 32, "protein": 1.6, "carbs": 7.0, "fat": 0.2, "fiber": 2.0, "sugar": 4.8},
    "chickpeas": {"kcal": 164, "protein": 8.9, "carbs": 27.4, "fat": 2.6, "fiber": 7.6, "sugar": 4.8},
    "lentils": {"kcal": 353, "protein": 25.8, "carbs": 60.0, "fat": 1.1, "fiber": 10.7, "sugar": 2.0},
    "flour": {"kcal": 364, "protein": 10.3, "carbs": 76.0, "fat": 1.0, "fiber": 2.7, "sugar": 0.3},
    "all-purpose flour": {"kcal": 364, "protein": 10.3, "carbs": 76.0, "fat": 1.0, "fiber": 2.7, "sugar": 0.3},
    "vermicelli": {"kcal": 371, "protein": 13.0, "carbs": 75.0, "fat": 1.5, "fiber": 3.2, "sugar": 2.7},
    "pasta": {"kcal": 371, "protein": 13.0, "carbs": 75.0, "fat": 1.5, "fiber": 3.2, "sugar": 2.7},
    "parsley": {"kcal": 36, "protein": 3.0, "carbs": 6.3, "fat": 0.8, "fiber": 3.3, "sugar": 0.9},
    "cilantro": {"kcal": 23, "protein": 2.1, "carbs": 3.7, "fat": 0.5, "fiber": 2.8, "sugar": 0.9},
    "lemon": {"kcal": 29, "protein": 1.1, "carbs": 9.3, "fat": 0.3, "fiber": 2.8, "sugar": 2.5},
    "potato": {"kcal": 77, "protein": 2.0, "carbs": 17.0, "fat": 0.1, "fiber": 2.2, "sugar": 0.8},
    "carrot": {"kcal": 41, "protein": 0.9, "carbs": 10.0, "fat": 0.2, "fiber": 2.8, "sugar": 4.7},
    "broccoli": {"kcal": 34, "protein": 2.8, "carbs": 7.0, "fat": 0.4, "fiber": 2.6, "sugar": 1.7},
    "spinach": {"kcal": 23, "protein": 2.9, "carbs": 3.6, "fat": 0.4, "fiber": 2.2, "sugar": 0.4},
    "mushroom": {"kcal": 22, "protein": 3.1, "carbs": 3.3, "fat": 0.3, "fiber": 1.0, "sugar": 2.0},
    "egg": {"kcal": 143, "protein": 12.6, "carbs": 0.7, "fat": 9.5, "fiber": 0.0, "sugar": 0.4},
    "milk": {"kcal": 61, "protein": 3.2, "carbs": 4.8, "fat": 3.3, "fiber": 0.0, "sugar": 5.0},
    "cheese": {"kcal": 402, "protein": 25.0, "carbs": 1.3, "fat": 33.0, "fiber": 0.0, "sugar": 0.5},
    "yogurt": {"kcal": 61, "protein": 3.5, "carbs": 4.7, "fat": 3.3, "fiber": 0.0, "sugar": 4.7},
    "cream": {"kcal": 340, "protein": 2.1, "carbs": 2.8, "fat": 36.0, "fiber": 0.0, "sugar": 2.8},
    "sugar": {"kcal": 387, "protein": 0.0, "carbs": 100.0, "fat": 0.0, "fiber": 0.0, "sugar": 100.0},
    "honey": {"kcal": 304, "protein": 0.3, "carbs": 82.0, "fat": 0.0, "fiber": 0.2, "sugar": 82.1},
    "maple syrup": {"kcal": 260, "protein": 0.0, "carbs": 67.0, "fat": 0.1, "fiber": 0.0, "sugar": 60.0},
    "peanut butter": {"kcal": 588, "protein": 25.0, "carbs": 20.0, "fat": 50.0, "fiber": 6.0, "sugar": 9.0},
    "almond": {"kcal": 579, "protein": 21.2, "carbs": 21.6, "fat": 49.9, "fiber": 12.5, "sugar": 4.4},
    "walnut": {"kcal": 654, "protein": 15.2, "carbs": 13.7, "fat": 65.2, "fiber": 6.7, "sugar": 2.6},
    "chicken": {"kcal": 165, "protein": 31.0, "carbs": 0.0, "fat": 3.6, "fiber": 0.0, "sugar": 0.0},
    "beef": {"kcal": 250, "protein": 26.0, "carbs": 0.0, "fat": 15.0, "fiber": 0.0, "sugar": 0.0},
    "lamb": {"kcal": 294, "protein": 25.6, "carbs": 0.0, "fat": 21.0, "fiber": 0.0, "sugar": 0.0},
    "pork": {"kcal": 242, "protein": 27.0, "carbs": 0.0, "fat": 14.0, "fiber": 0.0, "sugar": 0.0},
    "fish": {"kcal": 206, "protein": 22.0, "carbs": 0.0, "fat": 12.0, "fiber": 0.0, "sugar": 0.0},
    "salmon": {"kcal": 208, "protein": 20.0, "carbs": 0.0, "fat": 13.0, "fiber": 0.0, "sugar": 0.0},
    "shrimp": {"kcal": 99, "protein": 24.0, "carbs": 0.2, "fat": 0.3, "fiber": 0.0, "sugar": 0.0},
    "tofu": {"kcal": 144, "protein": 17.0, "carbs": 3.0, "fat": 8.0, "fiber": 1.0, "sugar": 0.6},
    "black beans": {"kcal": 339, "protein": 21.6, "carbs": 62.0, "fat": 1.4, "fiber": 15.0, "sugar": 2.1},
    "kidney beans": {"kcal": 333, "protein": 23.0, "carbs": 60.0, "fat": 0.8, "fiber": 15.2, "sugar": 2.1},
}


ALIASES = {
    "extra-virgin olive oil": "olive oil",
    "olive oil": "olive oil",
    "all purpose flour": "all-purpose flour",
    "ground beef": "beef",
    "chicken breast": "chicken",
    "chicken thighs": "chicken",
    "vegetable broth": "chicken broth",
}


# grams per 1 unit
UNIT_TO_GRAMS = {
    "g": 1.0,
    "gram": 1.0,
    "grams": 1.0,
    "kg": 1000.0,
    "ml": 1.0,
    "l": 1000.0,
    "cup": 240.0,
    "cups": 240.0,
    "tablespoon": 15.0,
    "tablespoons": 15.0,
    "tbsp": 15.0,
    "teaspoon": 5.0,
    "teaspoons": 5.0,
    "tsp": 5.0,
    "oz": 28.35,
    "ounce": 28.35,
    "ounces": 28.35,
    "lb": 453.6,
    "pound": 453.6,
    "pounds": 453.6,
    "clove": 5.0,
    "cloves": 5.0,
    "stalk": 40.0,
    "stalks": 40.0,
    "rib": 40.0,
    "ribs": 40.0,
    "large": 150.0,
    "medium": 100.0,
    "small": 70.0,
    "can": 400.0,
    "cans": 400.0,
    "egg": 50.0,
    "eggs": 50.0,
}


ALLERGEN_KEYWORDS = {
    "nuts": ["almond", "walnut", "cashew", "pecan", "hazelnut", "pistachio", "macadamia", "nut"],
    "dairy": ["milk", "cheese", "butter", "cream", "yogurt", "ghee", "whey"],
    "eggs": ["egg", "eggs", "mayonnaise", "mayo"],
    "gluten": ["wheat", "flour", "bread", "pasta", "noodle", "barley", "rye", "soy sauce", "vermicelli"],
    "animal": ["chicken", "beef", "pork", "lamb", "fish", "shrimp", "bacon", "meat", "broth", "stock", "egg", "milk", "cheese", "butter", "honey"],
}


@dataclass
class IngredientParsed:
    raw: str
    qty: float
    unit: str
    name: str
    grams: float
    matched_key: str | None


def parse_fraction(num: str) -> float:
    num = num.strip()
    if "/" in num:
        a, b = num.split("/", 1)
        return float(a) / float(b)
    return float(num)


def parse_quantity_prefix(text: str) -> tuple[float, str]:
    text = text.strip()
    # matches: "1", "1.5", "1/2", "1 1/2"
    m = re.match(r"^(\d+(?:\.\d+)?|\d+/\d+)(?:\s+(\d+/\d+))?\s*(.*)$", text)
    if not m:
        return 1.0, text
    first = parse_fraction(m.group(1))
    second = parse_fraction(m.group(2)) if m.group(2) else 0.0
    return first + second, m.group(3).strip()


def normalize_name(name: str) -> str:
    s = name.lower()
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"[^a-z0-9\s-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def best_match_ingredient_key(name_norm: str) -> str | None:
    if name_norm in ALIASES:
        name_norm = ALIASES[name_norm]
    if name_norm in NUTRITION_DB:
        return name_norm
    # contains matching (longest key first)
    for key in sorted(NUTRITION_DB.keys(), key=len, reverse=True):
        if key in name_norm:
            return key
    return None


def parse_ingredient_line(line: str) -> IngredientParsed:
    qty, rest = parse_quantity_prefix(line)
    parts = rest.split()
    unit = ""
    name = rest
    if parts:
        maybe_unit = parts[0].lower().strip(",.")
        if maybe_unit in UNIT_TO_GRAMS:
            unit = maybe_unit
            name = " ".join(parts[1:]).strip()
    if not name:
        name = rest
    name_norm = normalize_name(name)
    key = best_match_ingredient_key(name_norm)
    g_per_unit = UNIT_TO_GRAMS.get(unit, 100.0)
    grams = max(0.0, qty * g_per_unit)
    return IngredientParsed(raw=line, qty=qty, unit=unit, name=name_norm, grams=grams, matched_key=key)


def parse_servings(yields_text: str | None) -> float:
    if not yields_text:
        return 1.0
    m = re.search(r"(\d+(?:\.\d+)?)", yields_text)
    if not m:
        return 1.0
    return max(1.0, float(m.group(1)))


def add_macro(total: dict[str, float], per100: dict[str, float], grams: float) -> None:
    factor = grams / 100.0
    for k in ("kcal", "protein", "carbs", "fat", "fiber", "sugar"):
        total[k] += per100.get(k, 0.0) * factor


def classify_diet_flags(ingredient_text: str) -> dict[str, bool]:
    txt = ingredient_text.lower()

    def has_any(words: list[str]) -> bool:
        return any(w in txt for w in words)

    has_nuts = has_any(ALLERGEN_KEYWORDS["nuts"])
    has_dairy = has_any(ALLERGEN_KEYWORDS["dairy"])
    has_eggs = has_any(ALLERGEN_KEYWORDS["eggs"])
    has_gluten = has_any(ALLERGEN_KEYWORDS["gluten"])
    has_animal = has_any(ALLERGEN_KEYWORDS["animal"])

    return {
        "vegan": not has_animal,
        "vegetarian": not has_any(["chicken", "beef", "pork", "lamb", "fish", "shrimp", "bacon", "meat", "stock", "broth"]),
        "gluten_free": not has_gluten,
        "nut_free": not has_nuts,
        "dairy_free": not has_dairy,
        "egg_free": not has_eggs,
    }


def health_score(total: dict[str, float]) -> float:
    kcal = max(1.0, total["kcal"])
    protein_density = (total["protein"] * 4.0) / kcal  # ratio of kcal from protein
    fiber_per_1000 = (total["fiber"] / kcal) * 1000.0
    sugar_per_1000 = (total["sugar"] / kcal) * 1000.0

    score = 50.0
    score += min(20.0, protein_density * 40.0)
    score += min(20.0, fiber_per_1000 * 1.5)
    score -= min(20.0, sugar_per_1000 * 0.8)
    return round(max(0.0, min(100.0, score)), 1)


def analyze_recipe(recipe: dict[str, Any]) -> dict[str, Any]:
    ingredients = recipe.get("ingredients") or []
    yields_text = recipe.get("yields")
    servings = parse_servings(yields_text)

    totals = {"kcal": 0.0, "protein": 0.0, "carbs": 0.0, "fat": 0.0, "fiber": 0.0, "sugar": 0.0}
    unknown: list[str] = []
    parsed_rows: list[dict[str, Any]] = []

    ingredient_blob = " ".join([str(x) for x in ingredients])
    flags = classify_diet_flags(ingredient_blob)

    for line in ingredients:
        p = parse_ingredient_line(str(line))
        if p.matched_key:
            add_macro(totals, NUTRITION_DB[p.matched_key], p.grams)
        else:
            unknown.append(p.raw)
        parsed_rows.append(
            {
                "raw": p.raw,
                "qty": p.qty,
                "unit": p.unit,
                "normalized_name": p.name,
                "estimated_grams": round(p.grams, 2),
                "matched_db_key": p.matched_key,
            }
        )

    per_serving = {k: round(v / servings, 2) for k, v in totals.items()}
    totals = {k: round(v, 2) for k, v in totals.items()}

    score = health_score(totals)

    return {
        "servings_estimated": servings,
        "nutrition_estimated_total": totals,
        "nutrition_estimated_per_serving": per_serving,
        "diet_flags": flags,
        "health_score_0_100": score,
        "unknown_ingredients_count": len(unknown),
        "unknown_ingredients": unknown,
        "ingredient_parse_details": parsed_rows,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Recipe nutrition + diet-tag analyzer (heuristic).")
    ap.add_argument("--input", required=True, help="Input JSON file path (array of recipes)")
    ap.add_argument("--output", default=None, help="Output JSON path (default: <input>_analyzed.json)")
    ap.add_argument("--limit", type=int, default=0, help="Analyze only first N recipes (0 = all)")
    args = ap.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else input_path.with_name(input_path.stem + "_analyzed.json")

    with input_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("Input JSON must be an array of recipe objects.")

    recipes = data[: args.limit] if args.limit and args.limit > 0 else data
    out: list[dict[str, Any]] = []

    for r in recipes:
        analyzed = analyze_recipe(r)
        merged = dict(r)
        merged["nutrition_analysis"] = analyzed
        out.append(merged)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print(f"Analyzed {len(out)} recipes")
    print(f"Saved: {output_path}")


if __name__ == "__main__":
    main()

