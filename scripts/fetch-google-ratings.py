#!/usr/bin/env python3
"""
fetch-google-ratings.py
Holt Google Ratings und Review Counts fuer Hotels im Google Sheet
via Google Places API und schreibt die Ergebnisse zurueck.

Konfiguration via .env:
  GOOGLE_PLACES_API_KEY=dein_api_key
  SHEET_ID=1RD9N9OPMHMCa-AQNbWffqwbRIKTFZH-5H5I7l5hw6wU
  GOOGLE_CREDENTIALS_FILE=credentials.json  (optional, default: credentials.json)
"""

import os
import sys
import time
import logging
from pathlib import Path

import gspread
import requests
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

# ── Setup ──────────────────────────────────────────────────────────────
load_dotenv(Path(__file__).parent.parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fetch-ratings")

PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY", "")
SHEET_ID = os.getenv("SHEET_ID", "1RD9N9OPMHMCa-AQNbWffqwbRIKTFZH-5H5I7l5hw6wU")
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
SHEET_TAB = "hotels"

# Column names in the sheet
COL_NAME = "name"
COL_ORTSTEIL = "ortsteil"
COL_PLACE_ID = "google_place_id"
COL_RATING = "google_rating"
COL_REVIEWS = "google_reviews"

RATE_LIMIT_SECONDS = 1.0


def get_sheet():
    """Connect to Google Sheets via service account."""
    creds_path = Path(__file__).parent.parent / CREDENTIALS_FILE
    if not creds_path.exists():
        creds_path = Path(CREDENTIALS_FILE)

    if not creds_path.exists():
        log.error("Credentials file not found: %s", creds_path)
        sys.exit(1)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_file(str(creds_path), scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SHEET_ID).worksheet(SHEET_TAB)


def find_place_id(hotel_name: str, ortsteil: str = "") -> str | None:
    """Search for a hotel via Places API Text Search and return place_id."""
    query = f"{hotel_name} {ortsteil} Kleinwalsertal".strip()
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {
        "query": query,
        "key": PLACES_API_KEY,
        "language": "de",
        "region": "at",
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    results = data.get("results", [])
    if not results:
        log.warning("  No results for: %s", query)
        return None

    place = results[0]
    log.info("  Found: %s (place_id: %s)", place.get("name"), place.get("place_id"))
    return place.get("place_id")


def get_place_details(place_id: str) -> tuple[float | None, int | None]:
    """Get rating and review count for a place_id."""
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "rating,user_ratings_total",
        "key": PLACES_API_KEY,
        "language": "de",
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    result = resp.json().get("result", {})

    rating = result.get("rating")
    reviews = result.get("user_ratings_total")
    return rating, reviews


def col_index(headers: list[str], name: str) -> int:
    """Find 1-based column index for a header name (case-insensitive)."""
    name_lower = name.lower()
    for i, h in enumerate(headers):
        if h.strip().lower() == name_lower:
            return i + 1
    return -1


def main():
    if not PLACES_API_KEY:
        log.error("GOOGLE_PLACES_API_KEY not set in .env")
        sys.exit(1)

    log.info("Connecting to Google Sheet: %s", SHEET_ID)
    ws = get_sheet()

    all_values = ws.get_all_values()
    if not all_values:
        log.error("Sheet is empty")
        sys.exit(1)

    headers = all_values[0]
    rows = all_values[1:]

    idx_name = col_index(headers, COL_NAME)
    idx_ortsteil = col_index(headers, COL_ORTSTEIL)
    idx_place_id = col_index(headers, COL_PLACE_ID)
    idx_rating = col_index(headers, COL_RATING)
    idx_reviews = col_index(headers, COL_REVIEWS)

    if idx_name < 0:
        log.error("Column '%s' not found in sheet", COL_NAME)
        sys.exit(1)

    # Ensure target columns exist
    for col_name, idx in [(COL_PLACE_ID, idx_place_id), (COL_RATING, idx_rating), (COL_REVIEWS, idx_reviews)]:
        if idx < 0:
            log.warning("Column '%s' not found - adding it", col_name)
            new_col = len(headers) + 1
            ws.update_cell(1, new_col, col_name)
            headers.append(col_name)
            if col_name == COL_PLACE_ID:
                idx_place_id = new_col
            elif col_name == COL_RATING:
                idx_rating = new_col
            elif col_name == COL_REVIEWS:
                idx_reviews = new_col

    log.info("Processing %d hotels...", len(rows))
    updated = 0

    for i, row in enumerate(rows):
        row_num = i + 2  # 1-indexed + header
        name = row[idx_name - 1].strip() if idx_name - 1 < len(row) else ""
        if not name:
            continue

        ortsteil = row[idx_ortsteil - 1].strip() if idx_ortsteil > 0 and idx_ortsteil - 1 < len(row) else ""
        existing_place_id = row[idx_place_id - 1].strip() if idx_place_id > 0 and idx_place_id - 1 < len(row) else ""

        log.info("[%d/%d] %s (%s)", i + 1, len(rows), name, ortsteil)

        # Step 1: Get or find place_id
        place_id = existing_place_id
        if not place_id:
            place_id = find_place_id(name, ortsteil)
            if place_id:
                ws.update_cell(row_num, idx_place_id, place_id)
                log.info("  Saved place_id: %s", place_id)
            time.sleep(RATE_LIMIT_SECONDS)

        if not place_id:
            log.warning("  Skipping - no place_id found")
            continue

        # Step 2: Get rating details
        rating, reviews = get_place_details(place_id)
        time.sleep(RATE_LIMIT_SECONDS)

        if rating is not None:
            ws.update_cell(row_num, idx_rating, rating)
            ws.update_cell(row_num, idx_reviews, reviews or 0)
            log.info("  Rating: %.1f (%d reviews)", rating, reviews or 0)
            updated += 1
        else:
            log.warning("  No rating data available")

    log.info("Done! Updated %d of %d hotels.", updated, len(rows))


if __name__ == "__main__":
    main()
