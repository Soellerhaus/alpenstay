#!/usr/bin/env python3
"""
match-booking.py - Match alpenstay hotels to their Booking.com URLs.

This script reads the curated booking_urls.json mapping and provides
utilities to verify and update the hotel-to-Booking.com URL mappings.

The initial mapping was built via manual web search (2026-03-19).
Booking.com URL slugs often differ from hotel names (e.g., "Hotel Tradizio"
is at booking.com/hotel/at/neue-krone.html because it was formerly "Neue Krone").

Usage:
    python scripts/match-booking.py                  # Print summary
    python scripts/match-booking.py --check          # Verify URLs return 200
    python scripts/match-booking.py --missing        # List hotels without Booking.com URL
    python scripts/match-booking.py --csv            # Export as CSV
"""

import argparse
import json
import sys
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
BOOKING_URLS_FILE = BASE_DIR / "data" / "booking_urls.json"
DESCRIPTIONS_FILE = BASE_DIR / "data" / "descriptions.json"
HOTELS_FILE = BASE_DIR / "data" / "kleinwalsertal" / "hotels.json"


def load_booking_urls():
    """Load the booking URLs mapping."""
    with open(BOOKING_URLS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def load_descriptions():
    """Load the 54-hotel descriptions list."""
    with open(DESCRIPTIONS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def load_all_hotels():
    """Load all 102 scraped hotels."""
    with open(HOTELS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["hotels"]


def print_summary():
    """Print a summary of the booking URL mapping."""
    data = load_booking_urls()
    hotels = data["hotels"]

    found = [name for name, info in hotels.items() if info.get("booking_url")]
    not_found = [name for name, info in hotels.items() if not info.get("booking_url")]

    print(f"Booking.com URL Mapping Summary")
    print(f"{'=' * 50}")
    print(f"Total hotels (descriptions.json): {len(hotels)}")
    print(f"Found on Booking.com:             {len(found)}")
    print(f"NOT found on Booking.com:         {len(not_found)}")
    print(f"Coverage:                         {len(found)/len(hotels)*100:.1f}%")
    print()

    if not_found:
        print(f"Hotels NOT on Booking.com:")
        print(f"{'-' * 50}")
        for name in sorted(not_found):
            notes = hotels[name].get("notes", "")
            print(f"  - {name}")
            if notes:
                print(f"    Reason: {notes}")
        print()

    print(f"Hotels WITH Booking.com URLs:")
    print(f"{'-' * 50}")
    for name in sorted(found):
        url = hotels[name]["booking_url"]
        booking_name = hotels[name].get("booking_name", "")
        name_diff = f" (listed as: {booking_name})" if booking_name and booking_name != name else ""
        print(f"  {name}{name_diff}")
        print(f"    {url}")


def print_missing():
    """Print only the hotels without Booking.com URLs."""
    data = load_booking_urls()
    hotels = data["hotels"]

    not_found = [(name, info) for name, info in hotels.items() if not info.get("booking_url")]

    print(f"Hotels without Booking.com URL ({len(not_found)}):")
    print(f"{'=' * 50}")
    for name, info in sorted(not_found):
        notes = info.get("notes", "No notes")
        print(f"  {name}")
        print(f"    {notes}")


def check_urls():
    """Verify that Booking.com URLs return HTTP 200."""
    try:
        import requests
    except ImportError:
        print("ERROR: 'requests' package required. Install with: pip install requests")
        sys.exit(1)

    data = load_booking_urls()
    hotels = data["hotels"]

    found = {name: info for name, info in hotels.items() if info.get("booking_url")}

    print(f"Checking {len(found)} Booking.com URLs...")
    print(f"{'=' * 50}")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    ok_count = 0
    fail_count = 0

    for name, info in sorted(found.items()):
        url = info["booking_url"]
        try:
            resp = requests.head(url, headers=headers, timeout=10, allow_redirects=True)
            status = resp.status_code
            if status == 200:
                print(f"  OK   {name}")
                ok_count += 1
            else:
                print(f"  FAIL {name} -> HTTP {status}")
                fail_count += 1
        except requests.RequestException as e:
            print(f"  ERR  {name} -> {e}")
            fail_count += 1

    print()
    print(f"Results: {ok_count} OK, {fail_count} failed")


def export_csv():
    """Export the mapping as CSV."""
    data = load_booking_urls()
    hotels = data["hotels"]

    print("hotel_name,booking_url,booking_name,location,on_booking")
    for name in sorted(hotels.keys()):
        info = hotels[name]
        url = info.get("booking_url") or ""
        bname = info.get("booking_name") or ""
        loc = info.get("location") or ""
        on_booking = "yes" if url else "no"
        # Escape commas in names
        name_escaped = f'"{name}"' if "," in name else name
        bname_escaped = f'"{bname}"' if "," in bname else bname
        print(f"{name_escaped},{url},{bname_escaped},{loc},{on_booking}")


def main():
    parser = argparse.ArgumentParser(
        description="Match alpenstay hotels to Booking.com URLs"
    )
    parser.add_argument("--check", action="store_true", help="Verify URLs return HTTP 200")
    parser.add_argument("--missing", action="store_true", help="List hotels without Booking.com URL")
    parser.add_argument("--csv", action="store_true", help="Export as CSV")

    args = parser.parse_args()

    if args.check:
        check_urls()
    elif args.missing:
        print_missing()
    elif args.csv:
        export_csv()
    else:
        print_summary()


if __name__ == "__main__":
    main()
