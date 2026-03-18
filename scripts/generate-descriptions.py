#!/usr/bin/env python3
"""
generate-descriptions.py
Generiert KI-Beschreibungen fuer Hotels via Claude API
und schreibt sie zurueck ins Google Sheet.

Konfiguration via .env:
  ANTHROPIC_API_KEY=dein_api_key
  SHEET_ID=1RD9N9OPMHMCa-AQNbWffqwbRIKTFZH-5H5I7l5hw6wU
  GOOGLE_CREDENTIALS_FILE=credentials.json  (optional)
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path

import anthropic
import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

# ── Setup ──────────────────────────────────────────────────────────────
load_dotenv(Path(__file__).parent.parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("gen-desc")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
SHEET_ID = os.getenv("SHEET_ID", "1RD9N9OPMHMCa-AQNbWffqwbRIKTFZH-5H5I7l5hw6wU")
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
SHEET_TAB = "hotels"
MODEL = "claude-haiku-4-5-20251001"
MAX_DESCRIPTION_AGE_DAYS = 30

SYSTEM_PROMPT = """Du bist ein lokaler Reiseexperte fuer Alpenurlaub.
Schreibe eine authentische, ehrliche Beschreibung in 3-4 Saetzen auf Deutsch.
Kein Marketing-Blabla. Fokus auf was das Hotel wirklich besonders macht.
Sei konkret und nenne spezifische Details wie Entfernungen, Besonderheiten oder Geheimtipps."""

# Column names
COL_NAME = "name"
COL_ORTSTEIL = "ortsteil"
COL_TAGS = "tag"
COL_SHORT_PITCH = "short_pitch"
COL_INSIDER_TIP = "insider_tip"
COL_GOOGLE_RATING = "google_rating"
COL_LIFT_DISTANCE = "lift_distance_m"
COL_LOIPE_DISTANCE = "loipe_distance_m"
COL_AI_DESCRIPTION = "ai_description"
COL_AI_DESC_DATE = "ai_description_date"


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


def col_index(headers: list[str], name: str) -> int:
    """Find 1-based column index (case-insensitive)."""
    name_lower = name.lower()
    for i, h in enumerate(headers):
        if h.strip().lower() == name_lower:
            return i + 1
    return -1


def get_cell(row: list[str], idx: int) -> str:
    """Safely get cell value by 1-based column index."""
    if idx < 1 or idx - 1 >= len(row):
        return ""
    return row[idx - 1].strip()


def needs_update(desc: str, date_str: str) -> bool:
    """Check if description is missing or older than MAX_DESCRIPTION_AGE_DAYS."""
    if not desc:
        return True
    if not date_str:
        return True
    try:
        desc_date = datetime.strptime(date_str.strip(), "%Y-%m-%d")
        return datetime.now() - desc_date > timedelta(days=MAX_DESCRIPTION_AGE_DAYS)
    except ValueError:
        return True


def build_prompt(hotel_data: dict) -> str:
    """Build a prompt for Claude from hotel data."""
    parts = [f"Hotel: {hotel_data['name']}"]

    if hotel_data.get("ortsteil"):
        parts.append(f"Ort: {hotel_data['ortsteil']}, Kleinwalsertal")
    if hotel_data.get("tags"):
        parts.append(f"Tags: {hotel_data['tags']}")
    if hotel_data.get("short_pitch"):
        parts.append(f"Kurzinfo: {hotel_data['short_pitch']}")
    if hotel_data.get("insider_tip"):
        parts.append(f"Insider-Tipp: {hotel_data['insider_tip']}")
    if hotel_data.get("google_rating"):
        parts.append(f"Google Rating: {hotel_data['google_rating']}")
    if hotel_data.get("lift_distance_m"):
        parts.append(f"Entfernung zum Lift: {hotel_data['lift_distance_m']}m")
    if hotel_data.get("loipe_distance_m"):
        parts.append(f"Entfernung zur Loipe: {hotel_data['loipe_distance_m']}m")

    parts.append("\nSchreibe eine authentische Beschreibung in 3-4 Saetzen.")
    return "\n".join(parts)


def generate_description(client: anthropic.Anthropic, prompt: str) -> str:
    """Call Claude API to generate a description."""
    message = client.messages.create(
        model=MODEL,
        max_tokens=300,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text.strip()


def main():
    if not ANTHROPIC_API_KEY:
        log.error("ANTHROPIC_API_KEY not set in .env")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    log.info("Connecting to Google Sheet: %s", SHEET_ID)
    ws = get_sheet()

    all_values = ws.get_all_values()
    if not all_values:
        log.error("Sheet is empty")
        sys.exit(1)

    headers = all_values[0]
    rows = all_values[1:]

    # Find column indices
    idx = {
        "name": col_index(headers, COL_NAME),
        "ortsteil": col_index(headers, COL_ORTSTEIL),
        "tags": col_index(headers, COL_TAGS),
        "short_pitch": col_index(headers, COL_SHORT_PITCH),
        "insider_tip": col_index(headers, COL_INSIDER_TIP),
        "google_rating": col_index(headers, COL_GOOGLE_RATING),
        "lift_distance_m": col_index(headers, COL_LIFT_DISTANCE),
        "loipe_distance_m": col_index(headers, COL_LOIPE_DISTANCE),
        "ai_description": col_index(headers, COL_AI_DESCRIPTION),
        "ai_desc_date": col_index(headers, COL_AI_DESC_DATE),
    }

    if idx["name"] < 0:
        log.error("Column '%s' not found", COL_NAME)
        sys.exit(1)

    # Ensure ai_description column exists
    if idx["ai_description"] < 0:
        log.warning("Column '%s' not found - adding it", COL_AI_DESCRIPTION)
        new_col = len(headers) + 1
        ws.update_cell(1, new_col, COL_AI_DESCRIPTION)
        headers.append(COL_AI_DESCRIPTION)
        idx["ai_description"] = new_col

    # Ensure ai_description_date column exists
    if idx["ai_desc_date"] < 0:
        log.warning("Column '%s' not found - adding it", COL_AI_DESC_DATE)
        new_col = len(headers) + 1
        ws.update_cell(1, new_col, COL_AI_DESC_DATE)
        headers.append(COL_AI_DESC_DATE)
        idx["ai_desc_date"] = new_col

    log.info("Processing %d hotels...", len(rows))
    generated = 0
    skipped = 0

    for i, row in enumerate(rows):
        row_num = i + 2
        name = get_cell(row, idx["name"])
        if not name:
            continue

        existing_desc = get_cell(row, idx["ai_description"])
        existing_date = get_cell(row, idx["ai_desc_date"])

        if not needs_update(existing_desc, existing_date):
            log.info("[%d/%d] %s - Skipping (up to date)", i + 1, len(rows), name)
            skipped += 1
            continue

        log.info("[%d/%d] %s - Generating description...", i + 1, len(rows), name)

        hotel_data = {
            "name": name,
            "ortsteil": get_cell(row, idx["ortsteil"]),
            "tags": get_cell(row, idx["tags"]),
            "short_pitch": get_cell(row, idx["short_pitch"]),
            "insider_tip": get_cell(row, idx["insider_tip"]),
            "google_rating": get_cell(row, idx["google_rating"]),
            "lift_distance_m": get_cell(row, idx["lift_distance_m"]),
            "loipe_distance_m": get_cell(row, idx["loipe_distance_m"]),
        }

        prompt = build_prompt(hotel_data)

        try:
            description = generate_description(client, prompt)
            today = datetime.now().strftime("%Y-%m-%d")

            ws.update_cell(row_num, idx["ai_description"], description)
            ws.update_cell(row_num, idx["ai_desc_date"], today)

            log.info("  Generated: %s...", description[:80])
            generated += 1
        except anthropic.APIError as e:
            log.error("  API error: %s", e)
        except Exception as e:
            log.error("  Unexpected error: %s", e)

    log.info("Done! Generated: %d, Skipped: %d, Total: %d", generated, skipped, len(rows))


if __name__ == "__main__":
    main()
