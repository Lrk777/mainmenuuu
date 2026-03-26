#!/bin/bash
# Run this and leave it open. When the AllRecipes scraper completes, you'll see "COMPLETE" and a summary.
# Usage: ./wait_for_allrecipes_done.sh   or   bash wait_for_allrecipes_done.sh

cd "$(dirname "$0")"
echo "Waiting for scrape_allrecipes.py to finish (PID 56836 or any scrape_allrecipes process)..."
echo ""

while pgrep -f "scrape_allrecipes.py" > /dev/null 2>&1; do
  sleep 30
  if [ -f allrecipes_progress.json ]; then
    cuisines=$(python3 -c "
import json
try:
  with open('allrecipes_progress.json') as f: d = json.load(f)
  done = d.get('cuisines_done', [])
  r = d.get('results', {})
  print(len(done), sum(len(v) for v in r.values()))
except: print('0 0')
" 2>/dev/null)
    echo "$(date '+%H:%M:%S') - Still running... Cuisines done: $(echo $cuisines | cut -d' ' -f1) | Recipes: $(echo $cuisines | cut -d' ' -f2)"
  fi
done

echo ""
echo "=========================================="
echo "  ALLRECIPES SCRAPE COMPLETE"
echo "=========================================="
if [ -f allrecipes_progress.json ]; then
  python3 -c "
import json
with open('allrecipes_progress.json') as f:
  d = json.load(f)
done = d.get('cuisines_done', [])
r = d.get('results', {})
print('Cuisines:', len(done))
print('Total recipes:', sum(len(v) for v in r.values()))
print()
print('Output: allrecipes_by_cuisine/ (one JSON per cuisine)')
print('        allrecipes_by_cuisine.json (combined)')
"
fi
echo "Done at $(date)."
