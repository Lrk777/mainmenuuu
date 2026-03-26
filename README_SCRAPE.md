# Ontario Restaurants Scraper (mainmenus.com)

## What it does

- **Ontario page** → list of all cities (262)
- **Each city** → all restaurant listing pages (pagination) → unique restaurant overview URLs
- **Each restaurant** → overview page (name, cuisines, status, address, hours, description, phone) + menu page (categories and items with name, price, description, tags e.g. "Popular")

Output: one JSON file with structure:

```json
{
  "Ajax": [
    {
      "url": "https://mainmenus.com/...",
      "name": "Restaurant Name - City, Ontario",
      "cuisines": ["Asian", "Indian"],
      "location": "Ontario, Ajax",
      "status": "Open Now",
      "address": "269 Kingston Road East, Ajax, Ontario L1Z 1G1",
      "hours_today": "05:00 PM - 02:00 AM (Today)",
      "hours_full": "Mon - Thu: 05:00 PM - 01:00 AM ...",
      "description": "...",
      "phone": "",
      "menu": [
        { "category": "Main Menu", "items": [{ "name": "...", "price": "$14.99", "description": "...", "tags": [] }] }
      ]
    }
  ],
  "Toronto": [ ... ]
}
```

## Commands

```bash
# Install
pip install -r requirements.txt

# Full run (saves progress after each city; safe to Ctrl+C and resume)
python scrape_ontario_restaurants.py

# Resume after interrupt
python scrape_ontario_restaurants.py --resume

# Test run: 2 cities, 5 restaurants per city
python scrape_ontario_restaurants.py --limit-cities 2 --limit-restaurants 5 -o test_output.json
```

## Files

- **ontario_restaurants.json** – final full output (or custom path with `-o`)
- **scrape_progress_ontario.json** – progress save (cities done + results so far); used by `--resume`
- **scrape_log.txt** – if you run with `nohup ... > scrape_log.txt 2>&1 &`

## Note

Full run can take many hours (1.5 s delay between requests; 2 requests per restaurant). Progress is saved after each city, so you can stop and resume anytime.
