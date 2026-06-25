#!/usr/bin/env python3
"""
Full ICF/CCF coach directory scraper.

Two phases, run automatically in sequence:
  1. Page through the search API (/api/search) to collect every coach's
     key + basic info (name, credential, location, etc.).
  2. Visit each coach's profile page (CCFDynamicPage.aspx) and parse the
     full contact + details info.

This file is self-contained (no dependency on parse_coach_profile.py) so it
can be run on its own.

Install requirements:
    pip install requests beautifulsoup4

Run (does everything, end to end):
    python scrape_all_coaches.py

Useful flags:
    python scrape_all_coaches.py --key-limit 50          # only collect 50 keys (test run)
    python scrape_all_coaches.py --profile-limit 20      # only scrape 20 profiles (test run)
    python scrape_all_coaches.py --workers 10            # more/fewer concurrent profile fetches
    python scrape_all_coaches.py --skip-keys             # reuse existing coaches_keys.csv

Outputs (written in the current folder):
    coaches_keys.csv     -- phase 1: one row per coach, basic search-result info
    coaches_full.csv      -- phase 2: one row per SUCCESSFULLY scraped coach (flat, fixed columns)
    coaches_failed.csv    -- phase 2: log of keys that failed this run (key, error, timestamp)
    coaches_full.jsonl    -- phase 2: full audit trail, one JSON object per attempt (success or fail)
    scrape.log            -- progress and error log

This script is RESUMABLE: if it's stopped and re-run, phase 2 skips any
coach key that's already present in coaches_full.csv. Keys that failed
are NOT marked done, so they are automatically retried on the next run.
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter

try:
    from urllib3.util.retry import Retry
except ImportError:  # very old urllib3 fallback path
    from requests.packages.urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Configuration -- tweak these if needed
# ---------------------------------------------------------------------------
SEARCH_URL = "https://icf-ccf.azurewebsites.net/api/search"
PROFILE_URL_TEMPLATE = (
    "https://apps.coachingfederation.org/eweb/CCFDynamicPage.aspx"
    "?webcode=ccfcoachprofileview&coachcstkey={key}"
)

KEYS_CSV = "coaches_keys.csv"
FULL_CSV = "coaches_full.csv"
FAILED_CSV = "coaches_failed.csv"
FULL_JSONL = "coaches_full.jsonl"
LOG_FILE = "scrape.log"

SEARCH_PAGE_SIZE = 100        # records per search API call -- the API accepted 10 in testing;
                              # bump this only after you've confirmed larger `take` values work
SEARCH_DELAY_SECONDS = 0.5    # pause between search API pages
PROFILE_WORKERS = 8           # MAXIMUM concurrent profile-page fetches -- the adaptive throttle
                              # starts well below this and only ramps up if the server stays healthy
PROFILE_DELAY_SECONDS = 0.0   # extra per-request pause if you want to be gentler on the server
CONNECT_TIMEOUT = 8           # seconds to wait for the TCP/TLS connection to establish
READ_TIMEOUT = 20             # seconds to wait for the server to send the response body --
                              # kept patient on purpose: if the server is overloaded rather than
                              # the network being flaky, retrying fast just adds more load
RETRY_TOTAL = 2               # retries per request inside this run (failures beyond this go to
                              # coaches_failed.csv and get retried on your NEXT run instead --
                              # cheaper than waiting on a slow connection in-process)
RETRY_BACKOFF = 1.0           # seconds, multiplied per retry attempt (1.0, 2.0, ...)

# --- Adaptive throttling (AIMD: additive increase, multiplicative decrease) ---
WAVE_SIZE = 20                # coaches evaluated per throttling decision
FAILURE_RATE_THRESHOLD = 0.15 # if a wave's failure rate exceeds this, back off hard
COOLDOWN_SECONDS_ON_OVERLOAD = 20  # pause this long after backing off, to let the server recover

COMMON_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def make_session(pool_size=PROFILE_WORKERS):
    session = requests.Session()
    try:
        retry = Retry(
            total=RETRY_TOTAL,
            backoff_factor=RETRY_BACKOFF,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
    except TypeError:
        # older urllib3 uses method_whitelist instead of allowed_methods
        retry = Retry(
            total=RETRY_TOTAL,
            backoff_factor=RETRY_BACKOFF,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["GET", "POST"],
        )
    # Size the connection pool to match worker count, otherwise once concurrent
    # requests exceed the default pool size (10), threads stop reusing
    # TCP/TLS connections and every request pays a fresh handshake cost.
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool_size, pool_maxsize=pool_size)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(COMMON_HEADERS)
    return session


# ---------------------------------------------------------------------------
# Phase 1: collect every coach key from the search API
# ---------------------------------------------------------------------------

def search_payload(skip, take):
    return {
        "requestId": "scrape-" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f"),
        "continuationToken": "",
        "skip": skip,
        "take": str(take),
        "sort": "lastName",
        "sortDirection": "asc",
        "keywords": "",
        "filters": {
            "keywords": "",
            "credentials": ["ACC", "PCC", "MCC"],
            "services": {
                "coachingThemes": [],
                "coachingMethods": {"methods": [], "relocate": False},
                "standardRate": {"proBono": False, "nonProfitDiscount": False, "feeRanges": []},
            },
            "experience": {
                "haveCoached": {"clientType": "", "organizationalClientTypes": []},
                "coachedOrganizations": {"global": False, "nonProfit": False, "industrySector": ""},
                "heldPositions": [],
            },
            "demographics": {
                "gender": "", "ageRange": "", "fluentLanguages": [],
                "locations": {"countries": [], "states": []},
            },
            "additional": {"canProvide": [], "designations": []},
        },
    }


SEARCH_FIELDS = ["key", "fullName", "credential", "location", "standardRate", "hasEnhancedProfile", "photoUrl"]


def fetch_all_keys(session, page_size=SEARCH_PAGE_SIZE, delay=SEARCH_DELAY_SECONDS, limit=None):
    """
    Page through /api/search and return a list of dicts, one per coach.
    Writes incrementally to KEYS_CSV so progress isn't lost on a crash.
    """
    results = []
    skip = 0
    total = None

    write_header = not os.path.exists(KEYS_CSV)
    with open(KEYS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SEARCH_FIELDS)
        if write_header:
            writer.writeheader()

        while True:
            payload = search_payload(skip, page_size)
            try:
                resp = session.post(SEARCH_URL, json=payload, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                log.error(f"Search request failed at skip={skip}: {e}")
                time.sleep(2)
                continue

            if total is None:
                total = data.get("resultCount", 0)
                log.info(f"Total coaches reported by API: {total}")

            page_results = data.get("results", [])
            if not page_results:
                log.info(f"No more results at skip={skip}; stopping key collection.")
                break

            for r in page_results:
                row = {k: r.get(k) for k in SEARCH_FIELDS}
                writer.writerow(row)
                results.append(row)

            f.flush()
            log.info(f"Fetched {len(results)}/{total} coach keys (skip={skip}, page_size={len(page_results)})")

            skip += page_size
            if limit and len(results) >= limit:
                log.info(f"Reached --key-limit={limit}, stopping key collection early.")
                break
            if total and skip >= total:
                break

            time.sleep(delay)

    return results


# ---------------------------------------------------------------------------
# Phase 2: fetch + parse each coach's profile page
# ---------------------------------------------------------------------------

DETAILS_FIELD_MAP = {
    "Coaching Themes": "coaching_themes",
    "Coaching Methods": "coaching_methods",
    "Willing to Relocate": "willing_to_relocate",
    "Special Rates": "special_rates",
    "Fee Range": "fee_range",
    "Type of Client": "type_of_client",
    "Organizational Client Types": "organizational_client_types",
    "Coached Organizations": "coached_organizations",
    "Industry Sectors Coached": "industry_sectors_coached",
    "Positions Held": "positions_held",
    "Has Prior Experience Delivering Coach Skills Training to Managers/Leaders": "prior_coach_training_experience",
    "Degrees": "degrees",
    "Gender": "gender",
    "Age": "age",
    "Fluent Languages": "fluent_languages",
    "Can Provide": "can_provide",
}

# Fixed column order for the CSV. Any unexpected/unmapped detail-table rows
# still get captured (see parse_profile_html's fallback_key) but only show
# up in the .jsonl output, not the CSV, so the CSV stays a stable shape.
FULL_CSV_FIELDS = (
    ["key", "search_full_name", "credential", "search_location", "standard_rate",
     "has_enhanced_profile", "search_photo_url"]
    + ["profile_url", "full_name", "credly_badge_id", "photo_url", "photo_record_id",
       "website", "email", "phone", "address", "fee", "location"]
    + list(DETAILS_FIELD_MAP.values())
    + ["scrape_error"]
)


def _text_or_none(tag):
    if tag is None:
        return None
    text = tag.get_text(strip=True)
    if not text or text.lower() == "unspecified":
        return None
    return text


def parse_profile_html(html, coach_key=None):
    """Parse one coach-profile page's HTML into a flat dict."""
    soup = BeautifulSoup(html, "html.parser")
    result = {
        "profile_url": PROFILE_URL_TEMPLATE.format(key=coach_key) if coach_key else None,
    }

    name_tag = soup.find(id="coachName")
    result["full_name"] = _text_or_none(name_tag)

    credential_div = soup.find(id="coachCredential")
    result["credly_badge_id"] = credential_div.get("data-share-badge-id") if credential_div else None

    photo_tag = soup.find(id="profilePhoto")
    photo_src = photo_tag.get("src") if photo_tag else None
    result["photo_url"] = photo_src
    if photo_src:
        m = re.search(r"file=(\d+)", photo_src)
        result["photo_record_id"] = m.group(1) if m else None
    else:
        result["photo_record_id"] = None

    result["website"] = _text_or_none(soup.find(id="webSiteLink"))

    email_tag = soup.find(id="emailLink")
    if email_tag and email_tag.get("href", "").startswith("mailto:"):
        result["email"] = email_tag["href"].replace("mailto:", "").strip()
    else:
        result["email"] = _text_or_none(email_tag)

    result["phone"] = _text_or_none(soup.find(id="phoneLbl"))
    result["address"] = _text_or_none(soup.find(id="addressLbl"))
    result["fee"] = _text_or_none(soup.find(id="coachFee"))
    result["location"] = _text_or_none(soup.find(id="coachLocation"))

    table = soup.find("table", class_=lambda c: c and "definition" in c)
    if table:
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) != 2:
                continue
            label = cells[0].get_text(strip=True)
            value = _text_or_none(cells[1])
            mapped_key = DETAILS_FIELD_MAP.get(label)
            if mapped_key:
                result[mapped_key] = value
            else:
                fallback_key = "field_" + re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
                result[fallback_key] = value

    for key in DETAILS_FIELD_MAP.values():
        result.setdefault(key, None)

    return result


def fetch_one_profile(session, search_row):
    """Fetch + parse a single coach's profile. Returns a combined flat dict."""
    key = search_row["key"]
    url = PROFILE_URL_TEMPLATE.format(key=key)
    combined = {
        "key": key,
        "search_full_name": search_row.get("fullName"),
        "credential": search_row.get("credential"),
        "search_location": search_row.get("location"),
        "standard_rate": search_row.get("standardRate"),
        "has_enhanced_profile": search_row.get("hasEnhancedProfile"),
        "search_photo_url": search_row.get("photoUrl"),
        "scrape_error": None,
    }
    try:
        resp = session.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        resp.raise_for_status()
        parsed = parse_profile_html(resp.text, coach_key=key)
        combined.update(parsed)
    except Exception as e:
        combined["scrape_error"] = str(e)
        log.warning(f"Failed to fetch/parse profile for key={key}: {e}")
    return combined


def load_keys(path=KEYS_CSV):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_already_scraped_keys(path=FULL_CSV):
    if not os.path.exists(path):
        return set()
    with open(path, newline="", encoding="utf-8") as f:
        return {row["key"] for row in csv.DictReader(f) if row.get("key")}


def scrape_all_profiles(session, search_rows, max_workers=PROFILE_WORKERS, limit=None,
                         wave_size=WAVE_SIZE, failure_threshold=FAILURE_RATE_THRESHOLD,
                         cooldown_seconds=COOLDOWN_SECONDS_ON_OVERLOAD):
    """
    Scrape profiles in small waves with adaptive concurrency (AIMD-style):
      - After each wave, if the failure rate is too high, HALVE concurrency
        and pause for a cooldown -- the server is struggling, back off hard and fast.
      - If the wave was healthy, INCREASE concurrency by 1 -- probe gently for
        more headroom.
    This avoids the failure spiral seen with a fixed high worker count: once a
    backend starts erroring under load, hammering it harder with retries only
    makes it worse. Backing off lets it recover; ramping slowly avoids
    re-triggering the same overload.
    """
    done_keys = load_already_scraped_keys()
    todo = [r for r in search_rows if r["key"] not in done_keys]
    if limit:
        todo = todo[:limit]

    log.info(f"{len(done_keys)} coaches already scraped previously, {len(todo)} remaining this run.")
    if not todo:
        log.info("Nothing to do.")
        return

    write_csv_header = not os.path.exists(FULL_CSV)
    write_failed_header = not os.path.exists(FAILED_CSV)

    concurrency = min(max_workers, max(1, max_workers // 3)) or 1  # start conservative
    completed = 0
    succeeded = 0
    failed = 0
    total = len(todo)
    start_time = time.time()

    with open(FULL_CSV, "a", newline="", encoding="utf-8") as csv_f, \
         open(FAILED_CSV, "a", newline="", encoding="utf-8") as failed_f, \
         open(FULL_JSONL, "a", encoding="utf-8") as jsonl_f:

        csv_writer = csv.DictWriter(csv_f, fieldnames=FULL_CSV_FIELDS, extrasaction="ignore")
        if write_csv_header:
            csv_writer.writeheader()

        failed_writer = csv.DictWriter(failed_f, fieldnames=["key", "scrape_error", "timestamp"])
        if write_failed_header:
            failed_writer.writeheader()

        idx = 0
        while idx < len(todo):
            wave = todo[idx: idx + wave_size]
            idx += wave_size

            wave_succeeded = 0
            wave_failed = 0

            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = [executor.submit(fetch_one_profile, session, row) for row in wave]
                for future in as_completed(futures):
                    row_result = future.result()

                    jsonl_f.write(json.dumps(row_result, ensure_ascii=False) + "\n")

                    if row_result.get("scrape_error"):
                        failed_writer.writerow({
                            "key": row_result["key"],
                            "scrape_error": row_result["scrape_error"],
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        })
                        wave_failed += 1
                    else:
                        csv_writer.writerow(row_result)
                        wave_succeeded += 1

            csv_f.flush()
            failed_f.flush()
            jsonl_f.flush()

            completed += len(wave)
            succeeded += wave_succeeded
            failed += wave_failed

            wave_total = wave_succeeded + wave_failed
            wave_failure_rate = (wave_failed / wave_total) if wave_total else 0.0

            elapsed = time.time() - start_time
            rate = succeeded / elapsed if elapsed > 0 else 0.0
            remaining = total - completed
            eta_hours = (remaining / rate / 3600) if rate > 0 else float("inf")

            log.info(
                f"Wave done: {wave_succeeded}/{wave_total} ok "
                f"(failure rate {wave_failure_rate:.0%}) at concurrency={concurrency}. "
                f"Progress: {completed}/{total}. "
                f"Overall: succeeded={succeeded}, failed={failed}. "
                f"Rate so far: {rate*3600:.0f}/hr. ETA: {eta_hours:.1f}h."
            )

            if wave_failure_rate > failure_threshold:
                old_concurrency = concurrency
                concurrency = max(1, concurrency // 2)
                log.warning(
                    f"Wave failure rate {wave_failure_rate:.0%} exceeded threshold "
                    f"{failure_threshold:.0%} -- backing off concurrency "
                    f"{old_concurrency} -> {concurrency} and cooling down for "
                    f"{cooldown_seconds}s to let the server recover."
                )
                time.sleep(cooldown_seconds)
            else:
                concurrency = min(max_workers, concurrency + 1)

    log.info(f"Profile scraping complete for this run. succeeded={succeeded}, failed={failed}, "
              f"final concurrency={concurrency} "
              f"(failed keys will be retried automatically on the next run -- see {FAILED_CSV}).")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def main():
    global READ_TIMEOUT

    parser = argparse.ArgumentParser(description="Scrape the full ICF/CCF coach directory.")
    parser.add_argument("--key-limit", type=int, default=None,
                         help="Stop after collecting this many keys (for a quick test run).")
    parser.add_argument("--profile-limit", type=int, default=None,
                         help="Only scrape this many profiles this run (for a quick test run).")
    parser.add_argument("--workers", type=int, default=PROFILE_WORKERS,
                         help="MAXIMUM concurrent profile-page fetches the adaptive throttle is "
                              "allowed to ramp up to (default: %(default)s). It starts well below "
                              "this and only climbs if the server stays healthy, backing off hard "
                              "if it doesn't -- you generally don't need to tune this by hand.")
    parser.add_argument("--read-timeout", type=int, default=READ_TIMEOUT,
                         help="Seconds to wait for a profile page response before giving up "
                              "and retrying/failing (default: %(default)s).")
    parser.add_argument("--skip-keys", action="store_true",
                         help="Skip phase 1 entirely and reuse the existing coaches_keys.csv.")
    args = parser.parse_args()
    READ_TIMEOUT = args.read_timeout

    session = make_session(pool_size=max(args.workers, 10))

    if args.skip_keys and os.path.exists(KEYS_CSV):
        log.info(f"--skip-keys set: reusing existing {KEYS_CSV}")
        search_rows = load_keys()
    else:
        log.info("Phase 1: collecting coach keys from the search API...")
        search_rows = fetch_all_keys(session, limit=args.key_limit)

    log.info(f"Phase 2: scraping {len(search_rows)} coach profiles "
              f"(workers={args.workers})...")
    scrape_all_profiles(session, search_rows, max_workers=args.workers, limit=args.profile_limit)

    log.info(f"Done. See {KEYS_CSV}, {FULL_CSV}, {FAILED_CSV}, and {FULL_JSONL}.")


if __name__ == "__main__":
    main()