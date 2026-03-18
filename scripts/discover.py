#!/usr/bin/env python3
"""
discover.py — Discover hotels for an Alpine destination using Claude AI.

Usage:
  python scripts/discover.py "Kleinwalsertal"
  python scripts/discover.py "Oberstdorf"
  python scripts/discover.py "Lech am Arlberg"

Outputs: data/{destination-slug}/hotels.json
The website loads this JSON directly — no Google Sheet needed.
"""

import json
import os
import re
import sys
import logging
from pathlib import Path

import anthropic
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("discover")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MODEL_DISCOVER = "claude-haiku-4-5-20251001"
MODEL_DESCRIBE = "claude-haiku-4-5-20251001"


def slugify(name):
    """Convert destination name to URL-safe slug."""
    s = name.lower().strip()
    s = re.sub(r"[äÄ]", "ae", s)
    s = re.sub(r"[öÖ]", "oe", s)
    s = re.sub(r"[üÜ]", "ue", s)
    s = re.sub(r"[ß]", "ss", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def discover_hotels(client, destination):
    """Ask Claude to list all known hotels in the destination."""
    log.info("Discovering hotels in %s...", destination)

    prompt = f"""Liste ALLE Hotels, Pensionen, Gasthoeffe, Resorts und Unterkuenfte die du in {destination} (Alpen) kennst.
Sei gruendlich und liste mindestens 20-50 Unterkuenfte auf. Gehe jeden Ortsteil systematisch durch.

Fuer jedes Hotel gib folgende Informationen als JSON-Objekt:
- name: Offizieller Name
- ortsteil: Ortsteil/Stadtteil falls bekannt
- typ: "Hotel", "Pension", "Gasthof", "Resort", "Aparthotel", "Ferienwohnung" etc.
- website: URL falls bekannt (sonst leer)
- tags: Komma-separiert, z.B. "familie, wellness, ski, adults_only, langlauf, wandern, ruhig, zentral, budget, luxus"
- ski_in_out: "ja", "nein" oder "eingeschraenkt"
- lift_distance_m: Geschaetzte Entfernung zum naechsten Skilift in Metern (Zahl oder null)
- nearest_lift_name: Name des naechsten Lifts falls bekannt
- loipe_distance_m: Geschaetzte Entfernung zur naechsten Loipe in Metern (Zahl oder null)
- wellness: "ja" oder "nein"
- short_pitch: Ein kurzer Satz was das Hotel ausmacht (max 60 Zeichen)
- insider_tip: Ein konkreter Tipp fuer Gaeste (max 80 Zeichen)

WICHTIG:
- Nur Hotels die du mit hoher Sicherheit kennst (keine erfundenen)
- Lieber weniger Hotels als falsche Informationen
- Bei Unsicherheit: Feld leer lassen statt raten
- Distanzen nur angeben wenn du dir sicher bist
- Antworte NUR mit einem JSON-Array, kein anderer Text

Antwort:"""

    message = client.messages.create(
        model=MODEL_DISCOVER,
        max_tokens=16000,
        messages=[{"role": "user", "content": prompt}],
    )

    text = message.content[0].text.strip()

    # Extract JSON array from response
    match = re.search(r"\[.*\]", text, re.DOTALL)
    if not match:
        log.error("Could not parse JSON from response")
        log.error("Response: %s", text[:500])
        sys.exit(1)

    hotels = json.loads(match.group())
    log.info("Found %d hotels/accommodations", len(hotels))
    return hotels


def generate_description(client, hotel, destination):
    """Generate a short authentic description for one hotel."""
    parts = [f"Hotel: {hotel['name']}"]
    if hotel.get("ortsteil"):
        parts.append(f"Ort: {hotel['ortsteil']}, {destination}")
    if hotel.get("typ"):
        parts.append(f"Typ: {hotel['typ']}")
    if hotel.get("tags"):
        parts.append(f"Tags: {hotel['tags']}")
    if hotel.get("short_pitch"):
        parts.append(f"Kurzinfo: {hotel['short_pitch']}")
    if hotel.get("insider_tip"):
        parts.append(f"Insider-Tipp: {hotel['insider_tip']}")
    if hotel.get("ski_in_out"):
        parts.append(f"Ski-in/out: {hotel['ski_in_out']}")
    if hotel.get("lift_distance_m"):
        parts.append(f"Lift-Distanz: {hotel['lift_distance_m']}m")
    if hotel.get("wellness"):
        parts.append(f"Wellness: {hotel['wellness']}")

    prompt = "\n".join(parts) + "\n\nSchreibe eine authentische Beschreibung in 2-3 Saetzen auf Deutsch. Max 250 Zeichen."

    system = f"""Du bist ein lokaler Reiseexperte fuer {destination} in den Alpen.
Schreibe authentisch und ehrlich. Kein Marketing-Blabla.
Fokus auf was das Hotel wirklich besonders macht.
Nenne keine konkreten Meter-Angaben ausser du bist dir absolut sicher.
Max 2-3 Saetze, max 250 Zeichen."""

    message = client.messages.create(
        model=MODEL_DESCRIBE,
        max_tokens=200,
        system=system,
        messages=[{"role": "user", "content": prompt}],
    )

    text = message.content[0].text.strip()
    # Remove markdown headings
    text = re.sub(r"^#.*?\n+", "", text).strip()
    # Trim to 280 chars at sentence boundary
    if len(text) > 280:
        cut = text[:280]
        last_period = cut.rfind(".")
        if last_period > 80:
            text = cut[: last_period + 1]
    return text


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/discover.py \"Destination Name\"")
        print("Example: python scripts/discover.py \"Kleinwalsertal\"")
        sys.exit(1)

    if not ANTHROPIC_API_KEY:
        log.error("ANTHROPIC_API_KEY not set in .env")
        sys.exit(1)

    destination = sys.argv[1]
    slug = slugify(destination)
    output_dir = Path(__file__).parent.parent / "data" / slug
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "hotels.json"

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # Step 1: Discover hotels
    hotels = discover_hotels(client, destination)

    # Step 2: Generate descriptions
    log.info("Generating descriptions for %d hotels...", len(hotels))
    for i, h in enumerate(hotels):
        name = h.get("name", "")
        if not name:
            continue
        log.info("[%d/%d] %s", i + 1, len(hotels), name)
        try:
            h["ai_description"] = generate_description(client, h, destination)
            log.info("  -> %s", h["ai_description"][:80])
        except Exception as e:
            log.error("  Error: %s", e)
            h["ai_description"] = ""

    # Step 3: Add metadata
    result = {
        "destination": destination,
        "slug": slug,
        "hotel_count": len(hotels),
        "hotels": hotels,
    }

    # Step 4: Save
    output_file.write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info("Saved %d hotels to %s", len(hotels), output_file)

    # Also create a descriptions.json for backward compatibility
    desc_file = output_dir / "descriptions.json"
    descs = {h["name"]: h.get("ai_description", "") for h in hotels if h.get("name")}
    desc_file.write_text(
        json.dumps(descs, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info("Saved descriptions to %s", desc_file)


if __name__ == "__main__":
    main()
