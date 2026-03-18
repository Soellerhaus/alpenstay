#!/usr/bin/env python3
"""
generate-descriptions-local.py
Generates AI descriptions for hotels using Claude API.
Reads hotel data via Google Sheets public CSV export (no credentials needed).
Writes results to a JSON file that the website can load.

Usage: python scripts/generate-descriptions-local.py
"""

import csv
import io
import json
import os
import sys
import logging
from pathlib import Path

import anthropic
import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("gen-desc")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
SHEET_ID = os.getenv("SHEET_ID", "1RD9N9OPMHMCa-AQNbWffqwbRIKTFZH-5H5I7l5hw6wU")
SHEET_TAB = "hotels"
MODEL = "claude-haiku-4-5-20251001"
OUTPUT_FILE = Path(__file__).parent.parent / "data" / "descriptions.json"

SYSTEM_PROMPT = """Du bist ein lokaler Reiseexperte fuer das Kleinwalsertal in den Alpen.
Schreibe eine authentische, ehrliche Beschreibung in 3-4 Saetzen auf Deutsch.
Kein Marketing-Blabla. Fokus auf was das Hotel wirklich besonders macht.
Sei konkret und nenne spezifische Details wie Entfernungen, Besonderheiten oder Lage.
Schreibe so, als wuerdest du einem Freund das Hotel empfehlen."""


def fetch_sheet_data():
    """Fetch hotel data from Google Sheets public CSV export."""
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_TAB}"
    log.info("Fetching sheet data...")
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    reader = csv.DictReader(io.StringIO(resp.text))
    hotels = []
    for row in reader:
        name = (row.get("name") or "").strip()
        if not name:
            continue
        hotels.append({
            "name": name,
            "ortsteil": (row.get("ortsteil") or "").strip(),
            "typ": (row.get("typ") or "").strip(),
            "short_pitch": (row.get("short_pitch") or "").strip(),
            "insider_tip": (row.get("insider_tip") or "").strip(),
            "tag": (row.get("tag") or "").strip(),
            "ski_in_out": (row.get("ski_in_out") or "").strip(),
            "lift_distance_m": (row.get("lift_distance_m") or "").strip(),
            "loipe_distance_m": (row.get("loipe_distance_m") or "").strip(),
            "loipe_name": (row.get("loipe_name") or "").strip(),
            "nearest_lift_name": (row.get("nearest_lift_name") or "").strip(),
        })
    log.info("Loaded %d hotels from sheet", len(hotels))
    return hotels


def build_prompt(h):
    """Build a prompt for Claude from hotel data."""
    parts = [f"Hotel: {h['name']}"]
    if h.get("ortsteil"):
        parts.append(f"Ort: {h['ortsteil']}, Kleinwalsertal")
    if h.get("typ"):
        parts.append(f"Typ: {h['typ']}")
    if h.get("tag"):
        parts.append(f"Tags: {h['tag']}")
    if h.get("short_pitch"):
        parts.append(f"Kurzinfo: {h['short_pitch']}")
    if h.get("insider_tip"):
        parts.append(f"Insider-Tipp: {h['insider_tip']}")
    if h.get("ski_in_out"):
        parts.append(f"Ski-in/out: {h['ski_in_out']}")
    if h.get("lift_distance_m"):
        parts.append(f"Entfernung zum Lift: {h['lift_distance_m']}m ({h.get('nearest_lift_name', '')})")
    if h.get("loipe_distance_m"):
        parts.append(f"Entfernung zur Loipe: {h['loipe_distance_m']}m")
    parts.append("\nSchreibe eine authentische Beschreibung in 3-4 Saetzen.")
    return "\n".join(parts)


def generate_description(client, prompt):
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
    hotels = fetch_sheet_data()

    # Load existing descriptions if file exists
    existing = {}
    if OUTPUT_FILE.exists():
        try:
            existing = json.loads(OUTPUT_FILE.read_text(encoding="utf-8"))
            log.info("Loaded %d existing descriptions", len(existing))
        except Exception:
            pass

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    generated = 0
    skipped = 0

    for i, h in enumerate(hotels):
        name = h["name"]

        # Skip if already has a description
        if name in existing and existing[name]:
            log.info("[%d/%d] %s - already has description, skipping", i+1, len(hotels), name)
            skipped += 1
            continue

        log.info("[%d/%d] %s - generating...", i+1, len(hotels), name)
        prompt = build_prompt(h)

        try:
            desc = generate_description(client, prompt)
            existing[name] = desc
            log.info("  -> %s...", desc[:80])
            generated += 1

            # Save after each generation (in case of interruption)
            OUTPUT_FILE.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")

        except anthropic.APIError as e:
            log.error("  API error: %s", e)
        except Exception as e:
            log.error("  Error: %s", e)

    log.info("Done! Generated: %d, Skipped: %d, Total: %d", generated, skipped, len(hotels))
    log.info("Output: %s", OUTPUT_FILE)


if __name__ == "__main__":
    main()
