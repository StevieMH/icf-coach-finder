# Coach Finder — ICF Credentialed Coach Directory Scraper

Scrapes the public [ICF Credentialed Coach Finder](https://apps.coachingfederation.org/eweb/CCFDynamicPage.aspx?webcode=ccfsearch)
directory in two phases:

1. **Search API** (`POST /api/search`) — paginates through every listed coach
   to collect their unique key, name, credential, and location.
2. **Profile pages** (`CCFDynamicPage.aspx?webcode=ccfcoachprofileview`) — for
   each key, fetches and parses the full profile (contact info + details
   table) out of the server-rendered HTML.

## Features

- **Resumable** — safe to stop (Ctrl+C) and restart at any time. Already-scraped
  coaches are skipped; only remaining/failed ones are retried.
- **Adaptive throttling** — concurrency ramps up gradually and backs off
  automatically (halves + cools down) if the server's failure rate spikes,
  instead of hammering an already-struggling backend.
- **Three output files**: a clean CSV of successes, a CSV of failures (for
  easy retry/inspection), and a full JSONL audit trail of every attempt.

## Setup

```bash
pip install -r requirements.txt
```

## Usage

Full run (collects all keys, then scrapes all profiles):

```bash
python scrape_all_coaches.py
```

Useful flags:

```bash
# Quick test run
python scrape_all_coaches.py --key-limit 50 --profile-limit 10

# Resume a previous run (skip re-collecting keys, just keep scraping profiles)
python scrape_all_coaches.py --skip-keys

# Raise the max concurrency ceiling the throttle is allowed to reach
python scrape_all_coaches.py --skip-keys --workers 12
```

## Output files

| File | Contents |
|---|---|
| `coaches_keys.csv` | One row per coach from the search API (key, name, credential, location, etc.) |
| `coaches_full.csv` | One row per **successfully** scraped profile — the main deliverable |
| `coaches_failed.csv` | Log of keys that failed (key, error, timestamp) — not treated as "done", auto-retried on next run |
| `coaches_full.jsonl` | Full audit trail, one JSON object per attempt (success or failure) |
| `scrape.log` | Timestamped progress log |

## Data & privacy note

The scraped output contains real people's names, emails, and phone numbers
as published in ICF's public directory. **These data files are excluded from
version control via `.gitignore`** — only the code is meant to be shared here.
If you intend to publish or share the data itself, do so deliberately and
make sure that's consistent with ICF's terms of use and applicable privacy
rules in your jurisdiction.
