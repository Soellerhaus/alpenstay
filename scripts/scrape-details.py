#!/usr/bin/env python3
"""
scrape-details.py — Scrape detailed info for each hotel from oberallgaeu.info
and enrich with Claude AI.

Reads hotel URLs from data/kleinwalsertal/hotels.json,
fetches each detail page, extracts structured data,
then uses Claude to fill gaps and generate descriptions.

Usage: python scripts/scrape-details.py
"""

import json
import os
import re
import sys
import time
import logging
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
log = logging.getLogger("details")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MODEL = "claude-haiku-4-5-20251001"
DATA_DIR = Path(__file__).parent.parent / "data" / "kleinwalsertal"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def scrape_detail_page(url):
    """Scrape a single hotel detail page."""
    resp = requests.get(url, headers=HEADERS, timeout=15)
    if resp.status_code != 200:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(separator="\n", strip=True)
    lines = text.split("\n")

    data = {}

    # Address
    for i, line in enumerate(lines):
        if re.match(r"^\d{4}$", line.strip()):
            data["plz"] = line.strip()
            if i > 0:
                data["strasse"] = lines[i - 1].strip()
            if i + 1 < len(lines):
                data["ort_raw"] = lines[i + 1].strip()
            break

    # Website
    for line in lines:
        if line.startswith("www.") and "oberallgaeu" not in line:
            data["hotel_website"] = "https://" + line.strip().rstrip("/")
            break

    # Extract structured sections
    section_triggers = {
        "Unterkunftsart:": "unterkunftsart",
        "Verpflegung:": "verpflegung",
        "Freizeit:": "freizeit",
        "Ausstattung:": "ausstattung",
    }

    for i, line in enumerate(lines):
        for trigger, key in section_triggers.items():
            if trigger in line:
                items = []
                for j in range(i + 1, min(i + 30, len(lines))):
                    item = lines[j].strip()
                    if any(t in item for t in section_triggers) or "Beschreibung" in item:
                        break
                    if (
                        item
                        and item != "alle..."
                        and len(item) > 2
                        and len(item) < 80
                        and not item.startswith("Tipps")
                        and not item.startswith("Das besondere")
                    ):
                        items.append(item)
                data[key] = list(dict.fromkeys(items))

    # Description
    for i, line in enumerate(lines):
        if "Beschreibung Unterkunft" in line:
            desc_lines = []
            for j in range(i + 1, min(i + 15, len(lines))):
                l = lines[j].strip()
                if l.startswith("Kontakt") or l.startswith("Verfügbarkeit") or l.startswith("**"):
                    break
                if l and len(l) > 20:
                    desc_lines.append(l)
            if desc_lines:
                data["beschreibung"] = " ".join(desc_lines[:2])[:500]
            break

    return data


def enrich_batch(client, hotels_batch):
    """Use Claude to structure and fill gaps for a batch of hotels."""
    summaries = []
    for h in hotels_batch:
        s = f"Name: {h['name']}"
        if h.get("_detail"):
            d = h["_detail"]
            if d.get("unterkunftsart"):
                s += f"\nTyp: {', '.join(d['unterkunftsart'][:5])}"
            if d.get("verpflegung"):
                s += f"\nVerpflegung: {', '.join(d['verpflegung'][:5])}"
            if d.get("freizeit"):
                s += f"\nFreizeit: {', '.join(d['freizeit'][:8])}"
            if d.get("ausstattung"):
                s += f"\nAusstattung: {', '.join(d['ausstattung'][:10])}"
            if d.get("beschreibung"):
                s += f"\nBeschreibung: {d['beschreibung'][:200]}"
            if d.get("hotel_website"):
                s += f"\nWebsite: {d['hotel_website']}"
            if d.get("ort_raw"):
                s += f"\nOrt: {d['ort_raw']}"
        summaries.append(s)

    prompt = f"""Hier sind {len(hotels_batch)} Unterkuenfte im Kleinwalsertal mit gescrapten Detaildaten.
Fuer JEDE gib ein JSON-Objekt mit:
- name: bereinigt (ohne "in Riezlern im Kleinwalsertal")
- ortsteil: Riezlern/Hirschegg/Mittelberg/Baad
- typ: Hotel/Pension/Gasthof/Resort/Aparthotel/Garni/Ferienwohnung
- sterne: Zahl oder null
- website: Hotel-Website URL
- tags: komma-separiert (familie,wellness,spa,ski,adults_only,langlauf,wandern,ruhig,zentral,budget,luxus,bio,tradition,hundefreundlich)
- verpflegung: komma-separiert (fruehstueck,halbpension,vollpension,restaurant)
- wellness_details: komma-separiert (hallenbad,sauna,dampfbad,spa,massage,infinity_pool,fitness)
- ski_in_out: ja/nein/eingeschraenkt
- nearest_lift_name: Name des naechsten Lifts
- familie_details: komma-separiert (kinderbetreuung,spielplatz,familienzimmer,kinderpool,kindergerichte)
- lage: ruhig/zentral/am_lift
- preis_kategorie: budget/mittel/gehoben/luxus
- besonderes: komma-separiert
- short_pitch: 1 Satz max 60 Zeichen
- insider_tip: konkreter Tipp max 80 Zeichen
- ai_description: 2-3 ehrliche Saetze, max 250 Zeichen

Bei Unsicherheit: leer lassen. NUR JSON-Array antworten.

{chr(10).join(summaries)}

JSON:"""

    try:
        msg = client.messages.create(
            model=MODEL, max_tokens=16000,
            messages=[{"role": "user", "content": prompt}],
        )
        text = msg.content[0].text.strip()
        match = re.search(r"\[.*\]", text, re.DOTALL)
        if match:
            return json.loads(match.group())
    except Exception as e:
        log.error("Claude enrichment failed: %s", e)
    return []


def main():
    if not ANTHROPIC_API_KEY:
        log.error("ANTHROPIC_API_KEY not set")
        sys.exit(1)

    hotels_file = DATA_DIR / "hotels.json"
    if not hotels_file.exists():
        log.error("hotels.json not found. Run discover.py first.")
        sys.exit(1)

    data = json.loads(hotels_file.read_text(encoding="utf-8"))
    hotels = data["hotels"]
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # Step 1: Scrape detail pages
    log.info("=== Scraping detail pages for %d hotels ===", len(hotels))
    for i, h in enumerate(hotels):
        url = h.get("website", "")
        if not url or "oberallgaeu.info" not in url:
            log.info("[%d/%d] %s - no detail URL, skipping", i + 1, len(hotels), h["name"])
            continue

        log.info("[%d/%d] %s", i + 1, len(hotels), h["name"])
        try:
            detail = scrape_detail_page(url)
            if detail:
                h["_detail"] = detail
                # Immediately use hotel_website if found
                if detail.get("hotel_website"):
                    h["website"] = detail["hotel_website"]
        except Exception as e:
            log.warning("  Failed: %s", e)

        time.sleep(0.5)  # Be polite

    # Step 2: Enrich with Claude in batches
    log.info("=== Enriching with Claude ===")
    batch_size = 15
    enriched_map = {}

    for i in range(0, len(hotels), batch_size):
        batch = hotels[i : i + batch_size]
        log.info("Batch %d-%d...", i + 1, min(i + batch_size, len(hotels)))
        results = enrich_batch(client, batch)
        for r in results:
            if r.get("name"):
                enriched_map[r["name"]] = r

    # Step 3: Merge enriched data
    log.info("=== Merging data ===")
    for h in hotels:
        h.pop("_detail", None)  # Remove raw scrape data

        # Find matching enriched data
        enriched = enriched_map.get(h["name"])
        if not enriched:
            # Try fuzzy match
            for ename, edata in enriched_map.items():
                if ename in h["name"] or h["name"] in ename:
                    enriched = edata
                    break

        if enriched:
            # Merge non-empty fields
            for k, v in enriched.items():
                if v and k != "name":
                    h[k] = v

    # Step 4: Save
    data["hotels"] = hotels
    data["hotel_count"] = len(hotels)
    hotels_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("=== Done! Saved %d enriched hotels ===", len(hotels))


if __name__ == "__main__":
    main()
