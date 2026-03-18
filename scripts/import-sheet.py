#!/usr/bin/env python3
"""
import-sheet.py — Import hotels from Google Sheet CSV into JSON format.
Merges with existing AI descriptions from data/descriptions.json.

Usage: python scripts/import-sheet.py
"""

import csv
import io
import json
from pathlib import Path

import requests

SHEET_ID = "1RD9N9OPMHMCa-AQNbWffqwbRIKTFZH-5H5I7l5hw6wU"
SHEET_TAB = "hotels"
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "kleinwalsertal"
DESC_FILE = Path(__file__).parent.parent / "data" / "descriptions.json"


def main():
    # Fetch sheet data as CSV
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_TAB}"
    print(f"Fetching sheet data from {SHEET_ID}...")
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    reader = csv.DictReader(io.StringIO(resp.text))

    # Load existing AI descriptions
    descs = {}
    if DESC_FILE.exists():
        descs = json.loads(DESC_FILE.read_text(encoding="utf-8"))
        print(f"Loaded {len(descs)} existing AI descriptions")

    hotels = []
    for row in reader:
        name = (row.get("name") or "").strip()
        if not name:
            continue

        tags_raw = (row.get("tag") or "").lower()
        h = {
            "name": name,
            "ortsteil": (row.get("ortsteil") or "").strip(),
            "typ": (row.get("typ") or "").strip(),
            "website": (row.get("website") or "").strip(),
            "booking_url": (row.get("booking_url") or "").strip(),
            "short_pitch": (row.get("short_pitch") or "").strip(),
            "insider_tip": (row.get("insider_tip") or "").strip(),
            "tags": (row.get("tag") or "").strip(),
            "ski_in_out": (row.get("ski_in_out") or "").strip(),
            "ski_in_out_note": (row.get("ski_in_out_note") or "").strip(),
            "nearest_lift_name": (row.get("nearest_lift_name") or "").strip(),
            "nearest_lift_type": (row.get("nearest_lift_type") or "").strip(),
            "lift_distance_m": (row.get("lift_distance_m") or "").strip() or None,
            "piste_distance_m": (row.get("piste_distance_m") or "").strip() or None,
            "loipe_distance_m": (row.get("loipe_distance_m") or "").strip() or None,
            "loipe_name": (row.get("loipe_name") or "").strip(),
            "wellness": "ja" if any(t in tags_raw for t in ["wellness", "spa", "sauna"]) else "nein",
            "ai_description": descs.get(name, ""),
        }
        hotels.append(h)

    result = {
        "destination": "Kleinwalsertal",
        "slug": "kleinwalsertal",
        "hotel_count": len(hotels),
        "hotels": hotels,
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_DIR / "hotels.json"
    output_file.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Exported {len(hotels)} hotels to {output_file}")


if __name__ == "__main__":
    main()
