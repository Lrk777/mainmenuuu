# AllRecipes Scraper – Recipes by Cuisine

Scrapes [AllRecipes](https://www.allrecipes.com) and saves **all recipes by cuisine**, each with full details: **ingredients**, **instructions**, and other fields (times, yields, nutrition, ratings, etc.).

## Why Playwright?

AllRecipes often returns 403 for plain HTTP requests. Using a real browser (Playwright) avoids that so we can fetch pages reliably.

## Setup

```bash
pip install -r requirements_allrecipes.txt
playwright install chromium
```

## Usage

```bash
# All cuisines, all recipes (saves progress; can resume)
python scrape_allrecipes.py

# Resume after stopping
python scrape_allrecipes.py --resume

# Test: 2 cuisines, 5 recipes per cuisine
python scrape_allrecipes.py --cuisines 2 --recipes 5 -o test_recipes.json
```

## Output

- **allrecipes_by_cuisine.json** (or `-o FILE`): one JSON object keyed by cuisine name. Each value is a list of recipe objects.

Each recipe object includes:

| Field | Description |
|-------|-------------|
| `title` | Recipe name |
| `description` | Summary (if present) |
| `ingredients` | List of ingredient strings (with amounts) |
| `instructions` | Full instructions text |
| `instructions_list` | Step-by-step list (if available) |
| `yields` | Servings / yield |
| `total_time`, `prep_time`, `cook_time` | Times |
| `image` | Main image URL |
| `ratings`, `reviews` | Rating/review info |
| `cuisine`, `category` | Classification |
| `author` | Recipe author |
| `nutrition` | Per-serving nutrition (if available) |

## Cuisines included

World Cuisine, Asian, European, African, Middle Eastern, Latin American, Mexican, Italian, Chinese, Indian, Japanese, Thai, Greek, French, German, Spanish, Southern United States, Cajun and Creole. You can extend the list in `scrape_allrecipes.py` (`CUISINE_START_URLS`).

## Progress

Progress is saved to **allrecipes_progress.json** after each cuisine so you can stop and run with `--resume` later.
