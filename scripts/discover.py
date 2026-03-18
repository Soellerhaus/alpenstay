#!/usr/bin/env python3
"""
discover.py — Discover hotels for Alpine destinations from real web sources.

Pipeline:
1. Scrape oberallgaeu.info for complete hotel list (names, locations, features)
2. Use Claude to enrich data (tags, descriptions) based on scraped info
3. Output data/{slug}/hotels.json for the website

Usage:
  python scripts/discover.py "Kleinwalsertal"

Sources: oberallgaeu.info (public tourism portal for Oberallgaeu/Kleinwalsertal)
"""

import json
import os
import re
import sys
import logging
import time
from pathlib import Path

import anthropic
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("discover")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MODEL = "claude-haiku-4-5-20251001"

# Mapping of destination names to oberallgaeu.info URL slugs
SOURCES = {
    "kleinwalsertal": {
        "name": "Kleinwalsertal",
        "base_url": "https://oberallgaeu.info/hotels-und-ferienwohnungen-im-oberallg%C3%A4u/kleinwalsertal",
        "ortsteile": ["Riezlern", "Hirschegg", "Mittelberg", "Baad"],
    },
    "oberstdorf": {
        "name": "Oberstdorf",
        "base_url": "https://oberallgaeu.info/hotels-und-ferienwohnungen-im-oberallg%C3%A4u/oberstdorf",
        "ortsteile": ["Oberstdorf", "Tiefenbach", "Kornau", "Schollang"],
    },
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "de-DE,de;q=0.9",
}


def slugify(name):
    s = name.lower().strip()
    s = re.sub(r"[aeoeueAeOeUe]", lambda m: {"ae":"ae","oe":"oe","ue":"ue","Ae":"ae","Oe":"oe","Ue":"ue"}.get(m.group(), m.group()), s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def scrape_page(url):
    """Scrape a single page from oberallgaeu.info and extract hotel data."""
    log.info("  Fetching %s", url)
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    hotels = []

    # Find hotel cards - the site uses article or div elements with hotel info
    # Try multiple selectors for robustness
    cards = soup.select("article, .accommodation-card, .teaser, .card, .listing-item, .result-item")

    if not cards:
        # Fallback: look for any elements with hotel names (h2, h3 links)
        cards = soup.select("h2 a, h3 a, .title a")

    for card in cards:
        name = ""
        ortsteil = ""
        typ = ""
        features = []
        rating = None
        website = ""

        # Try to extract name
        title_el = card.select_one("h2, h3, .title, .name") if card.name not in ("a",) else None
        if title_el:
            name = title_el.get_text(strip=True)
        elif card.name == "a":
            name = card.get_text(strip=True)
            website = card.get("href", "")

        if not name or len(name) < 3:
            continue

        # Try to extract location
        loc_el = card.select_one(".location, .address, .subtitle, .ort")
        if loc_el:
            ortsteil = loc_el.get_text(strip=True)

        # Try to extract type
        type_el = card.select_one(".type, .category, .badge")
        if type_el:
            typ = type_el.get_text(strip=True)

        # Try to extract description/features
        desc_el = card.select_one(".description, .text, p, .features")
        if desc_el:
            features.append(desc_el.get_text(strip=True))

        # Try rating
        rating_el = card.select_one(".rating, .score, [class*=rating]")
        if rating_el:
            try:
                rating = float(re.search(r"[\d.]+", rating_el.get_text()).group())
            except (AttributeError, ValueError):
                pass

        # Try link
        link_el = card.select_one("a[href]") if card.name != "a" else card
        if link_el and link_el.get("href"):
            href = link_el["href"]
            if href.startswith("/"):
                href = "https://oberallgaeu.info" + href
            website = href

        hotels.append({
            "name": name,
            "ortsteil": ortsteil,
            "typ": typ,
            "website": website,
            "features_raw": " ".join(features),
            "rating": rating,
        })

    return hotels, soup


def scrape_all_pages(base_url):
    """Scrape all pages of hotel listings."""
    all_hotels = []
    seen_names = set()
    page = 1

    while True:
        url = base_url if page == 1 else f"{base_url}?page={page}"
        try:
            hotels, soup = scrape_page(url)
        except Exception as e:
            log.warning("  Failed to fetch page %d: %s", page, e)
            break

        if not hotels:
            log.info("  No more hotels on page %d, stopping", page)
            break

        new_count = 0
        for h in hotels:
            if h["name"] not in seen_names:
                seen_names.add(h["name"])
                all_hotels.append(h)
                new_count += 1

        log.info("  Page %d: %d hotels (%d new)", page, len(hotels), new_count)

        if new_count == 0:
            break

        page += 1
        time.sleep(1)  # Be polite

        if page > 10:  # Safety limit
            break

    return all_hotels


def enrich_with_claude(client, hotels, destination, ortsteile):
    """Use Claude to add tags, descriptions, and structured data."""
    log.info("Enriching %d hotels with Claude...", len(hotels))

    # Build a summary of all hotels for Claude to process in batch
    hotel_list = []
    for h in hotels:
        line = f"- {h['name']}"
        if h.get("ortsteil"):
            line += f" ({h['ortsteil']})"
        if h.get("typ"):
            line += f" [{h['typ']}]"
        if h.get("features_raw"):
            line += f": {h['features_raw'][:150]}"
        hotel_list.append(line)

    prompt = f"""Hier ist eine Liste von {len(hotels)} Unterkuenften in {destination} (Ortsteile: {', '.join(ortsteile)}).
Fuer JEDE Unterkunft gib ein JSON-Objekt mit folgenden Feldern:
- name: exakt wie in der Liste
- ortsteil: einer von {', '.join(ortsteile)} (falls du es weisst, sonst leer)
- typ: "Hotel", "Pension", "Gasthof", "Resort", "Aparthotel", "Ferienwohnung", "Garni"
- tags: komma-separiert aus: familie, wellness, spa, ski, adults_only, langlauf, wandern, ruhig, zentral, budget, luxus, bio, tradition
- ski_in_out: "ja", "nein" oder "eingeschraenkt"
- lift_distance_m: geschaetzte Entfernung zum naechsten Lift (Zahl oder null)
- nearest_lift_name: Name des naechsten Lifts wenn bekannt
- loipe_distance_m: geschaetzte Entfernung zur naechsten Loipe (Zahl oder null)
- wellness: "ja" oder "nein"
- short_pitch: ein Satz was die Unterkunft ausmacht (max 60 Zeichen)
- insider_tip: konkreter Tipp (max 80 Zeichen, oder leer)
- ai_description: authentische Beschreibung in 2-3 Saetzen, max 250 Zeichen. Kein Marketing-Blabla.

WICHTIG:
- Bei Unsicherheit: Feld leer lassen
- Distanzen nur wenn sicher
- Antworte NUR mit JSON-Array

Liste:
{chr(10).join(hotel_list)}

JSON:"""

    message = client.messages.create(
        model=MODEL,
        max_tokens=16000,
        messages=[{"role": "user", "content": prompt}],
    )

    text = message.content[0].text.strip()
    match = re.search(r"\[.*\]", text, re.DOTALL)
    if not match:
        log.error("Could not parse Claude response as JSON")
        return hotels

    enriched = json.loads(match.group())

    # Merge enriched data back into scraped hotels
    enriched_by_name = {h["name"]: h for h in enriched if h.get("name")}
    for h in hotels:
        if h["name"] in enriched_by_name:
            e = enriched_by_name[h["name"]]
            h.update({k: v for k, v in e.items() if v and k != "name"})

    return hotels


def enrich_remaining_descriptions(client, hotels, destination):
    """Generate descriptions for hotels that don't have one yet."""
    missing = [h for h in hotels if not h.get("ai_description")]
    if not missing:
        return

    log.info("Generating descriptions for %d remaining hotels...", len(missing))
    for i, h in enumerate(missing):
        prompt = f"Hotel: {h['name']}"
        if h.get("ortsteil"):
            prompt += f"\nOrt: {h['ortsteil']}, {destination}"
        if h.get("typ"):
            prompt += f"\nTyp: {h['typ']}"
        if h.get("tags"):
            prompt += f"\nTags: {h['tags']}"
        if h.get("short_pitch"):
            prompt += f"\nInfo: {h['short_pitch']}"
        prompt += "\n\nSchreibe 2-3 authentische Saetze. Max 250 Zeichen. Kein Marketing."

        try:
            msg = client.messages.create(
                model=MODEL, max_tokens=200,
                system=f"Du bist Reiseexperte fuer {destination}. Ehrlich, konkret, kein Blabla.",
                messages=[{"role": "user", "content": prompt}],
            )
            desc = msg.content[0].text.strip()
            desc = re.sub(r"^#.*?\n+", "", desc).strip()
            if len(desc) > 280:
                last_period = desc[:280].rfind(".")
                if last_period > 80:
                    desc = desc[:last_period + 1]
            h["ai_description"] = desc
            log.info("  [%d/%d] %s", i + 1, len(missing), h["name"])
        except Exception as e:
            log.error("  Error for %s: %s", h["name"], e)


def clean_hotel(h):
    """Clean up hotel data for output."""
    # Remove scraper-internal fields
    h.pop("features_raw", None)
    h.pop("rating", None)
    # Clean ai_description
    if h.get("ai_description"):
        h["ai_description"] = re.sub(r"^#.*?\n+", "", h["ai_description"]).strip()
    return h


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/discover.py \"Destination\"")
        print(f"Available: {', '.join(SOURCES.keys())}")
        sys.exit(1)

    destination = sys.argv[1]
    slug = slugify(destination)

    if slug not in SOURCES:
        log.error("Unknown destination '%s'. Available: %s", destination, ", ".join(SOURCES.keys()))
        sys.exit(1)

    if not ANTHROPIC_API_KEY:
        log.error("ANTHROPIC_API_KEY not set in .env")
        sys.exit(1)

    source = SOURCES[slug]
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # Step 1: Scrape hotel listings
    log.info("=== Step 1: Scraping %s from oberallgaeu.info ===", source["name"])
    hotels = scrape_all_pages(source["base_url"])
    log.info("Scraped %d unique hotels", len(hotels))

    if not hotels:
        log.error("No hotels found! Check if the website structure changed.")
        sys.exit(1)

    # Step 2: Enrich with Claude (batch)
    log.info("=== Step 2: Enriching with Claude ===")
    # Process in batches of 30 to stay within token limits
    batch_size = 30
    for i in range(0, len(hotels), batch_size):
        batch = hotels[i:i + batch_size]
        log.info("Batch %d-%d...", i + 1, min(i + batch_size, len(hotels)))
        try:
            enrich_with_claude(client, batch, source["name"], source["ortsteile"])
        except Exception as e:
            log.error("Batch enrichment failed: %s", e)

    # Step 3: Generate missing descriptions
    log.info("=== Step 3: Generating remaining descriptions ===")
    enrich_remaining_descriptions(client, hotels, source["name"])

    # Step 4: Clean and save
    hotels = [clean_hotel(h) for h in hotels]

    output_dir = Path(__file__).parent.parent / "data" / slug
    output_dir.mkdir(parents=True, exist_ok=True)

    result = {
        "destination": source["name"],
        "slug": slug,
        "hotel_count": len(hotels),
        "source": "oberallgaeu.info",
        "hotels": hotels,
    }

    output_file = output_dir / "hotels.json"
    output_file.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("=== Done! Saved %d hotels to %s ===", len(hotels), output_file)


if __name__ == "__main__":
    main()
