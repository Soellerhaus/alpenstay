#!/usr/bin/env python3
"""
AlpenStay — Automated Hotel Data Pipeline v2
=============================================
100% automated, ZERO manual entries.

Data sources:
1. Google Places API → Hotels
2. OSM Overpass API  → Pistes, Loipes, Lifts (aerialways)
3. Google Routes API → Walking time to nearest lift (1 call per hotel)
4. Claude API        → AI descriptions from real facts

Output: data/kleinwalsertal_real.json
"""

import json
import math
import os
import time
import urllib.parse
import requests
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env", override=True)

API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")
if not API_KEY:
    raise ValueError("GOOGLE_PLACES_API_KEY nicht in .env gefunden!")

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

# Aerialway type mapping: OSM tag → German name
AERIALWAY_KEEP = {
    "gondola": "Gondelbahn",
    "cable_car": "Seilbahn",
    "chair_lift": "Sessellift",
    "mixed_lift": "Kombibahn",
    "drag_lift": "Schlepplift",
    "t-bar": "Bügellift",
    "platter": "Tellerlift",
    "j-bar": "Ankerlift",
    "magic_carpet": "Förderband",
}
AERIALWAY_FILTER_OUT = {"goods", "station", "zip_line", "rope_tow"}


# =====================================================================
# HAVERSINE & GEOMETRY
# =====================================================================

def haversine(lat1, lon1, lat2, lon2):
    """Haversine distance in meters between two GPS points."""
    R = 6371000
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def point_to_segment(px, py, ax, ay, bx, by):
    """Shortest distance from point (px,py) to line segment (ax,ay)-(bx,by)."""
    dx, dy = bx - ax, by - ay
    if dx == 0 and dy == 0:
        return haversine(px, py, ax, ay)
    t = max(0, min(1, ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)))
    proj_lat = ax + t * dx
    proj_lon = ay + t * dy
    return haversine(px, py, proj_lat, proj_lon)


def find_nearest_track(hotel_lat, hotel_lon, tracks):
    """Find nearest piste/loipe from OSM track data using point-to-segment distance."""
    best_dist = float('inf')
    best_name = ''
    for track in tracks:
        pts = track['points']
        for i in range(len(pts) - 1):
            d = point_to_segment(
                hotel_lat, hotel_lon,
                pts[i][0], pts[i][1],
                pts[i + 1][0], pts[i + 1][1]
            )
            if d < best_dist:
                best_dist = d
                best_name = track['name']
    if best_dist == float('inf'):
        return None
    return {"name": best_name, "distanz_m": round(best_dist)}


def find_nearest_lift_haversine(hotel_lat, hotel_lng, lifts):
    """Find nearest lift using haversine (Luftlinie). Returns (lift_info, distance_m)."""
    best_dist = float('inf')
    best_lift = None
    for lift in lifts:
        d = haversine(hotel_lat, hotel_lng, lift['lat'], lift['lng'])
        if d < best_dist:
            best_dist = d
            best_lift = lift
    if best_lift is None:
        return None, float('inf')
    return best_lift, round(best_dist)


# =====================================================================
# GOOGLE PLACES API
# =====================================================================

def search_places(query, max_results=20):
    """Search for hotels/accommodations via Google Places Text Search."""
    url = "https://places.googleapis.com/v1/places:searchText"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": ",".join([
            "places.id",
            "places.displayName",
            "places.rating",
            "places.userRatingCount",
            "places.formattedAddress",
            "places.location",
            "places.websiteUri",
            "places.types",
            "places.businessStatus",
        ])
    }
    data = {
        "textQuery": query,
        "languageCode": "de",
        "maxResultCount": max_results,
    }
    resp = requests.post(url, headers=headers, json=data)
    if resp.status_code != 200:
        print(f"  FEHLER Places API: {resp.status_code} - {resp.text[:200]}")
        return []
    return resp.json().get("places", [])


def find_all_hotels():
    """Find all hotels/accommodations in Kleinwalsertal."""
    queries = [
        "Hotels Kleinwalsertal Vorarlberg",
        "Hotel Riezlern Kleinwalsertal",
        "Hotel Hirschegg Kleinwalsertal",
        "Hotel Mittelberg Kleinwalsertal",
        "Pension Kleinwalsertal",
        "Gasthof Kleinwalsertal",
        "Ferienwohnung Kleinwalsertal",
        "Garni Hotel Kleinwalsertal",
        "Aparthotel Kleinwalsertal",
        "Hotel Baad Kleinwalsertal",
    ]
    seen_ids = set()
    all_places = []
    for query in queries:
        print(f"Suche: '{query}'...")
        places = search_places(query, max_results=20)
        for p in places:
            place_id = p.get("id", "")
            if place_id not in seen_ids:
                seen_ids.add(place_id)
                all_places.append(p)
                name = p.get("displayName", {}).get("text", "?")
                rating = p.get("rating", "?")
                reviews = p.get("userRatingCount", 0)
                print(f"  + {name} ({rating}*, {reviews} Bew.)")
        time.sleep(0.5)
    print(f"\nGesamt: {len(all_places)} einzigartige Unterkuenfte gefunden")
    return all_places


# =====================================================================
# OSM OVERPASS API — Pistes, Loipes, AND Lifts in ONE call
# =====================================================================

def fetch_osm_data():
    """Fetch all ski-related data from OSM: pistes, loipes, AND lifts."""
    query = """[out:json][timeout:60];
(
  way["piste:type"="downhill"](47.30,10.10,47.37,10.25);
  way["piste:type"="nordic"](47.30,10.10,47.37,10.25);
  way["aerialway"](47.30,10.10,47.37,10.25);
);
out body;
>;
out skel qt;"""

    encoded = urllib.parse.quote(query)
    url = f"https://overpass-api.de/api/interpreter?data={encoded}"

    print("  Lade OSM Daten (Pisten + Loipen + Lifte) via Overpass API...")
    for attempt in range(5):
        try:
            resp = requests.get(url, timeout=90)
            if resp.status_code == 429 or resp.status_code >= 500:
                wait = 10 * (attempt + 1)
                print(f"  Overpass API Status {resp.status_code}, Retry {attempt + 1}/5 (warte {wait}s)...")
                time.sleep(wait)
                continue
            if resp.status_code == 200:
                data = resp.json()
                break
            print(f"  Overpass API Status {resp.status_code}, Retry {attempt + 1}/5...")
            time.sleep(10 * (attempt + 1))
        except Exception as e:
            print(f"  Overpass API Fehler: {e}, Retry {attempt + 1}/5...")
            time.sleep(10 * (attempt + 1))
    else:
        print("  WARNUNG: Overpass API nicht erreichbar!")
        return [], [], []

    elements = data.get('elements', [])
    ways = [e for e in elements if e['type'] == 'way']
    nodes = {e['id']: (e['lat'], e['lon']) for e in elements if e['type'] == 'node'}

    pisten = []
    loipen = []
    lifts = []

    for w in ways:
        tags = w.get('tags', {})
        node_ids = w.get('nodes', [])

        # Pistes
        ptype = tags.get('piste:type', '')
        if ptype == 'downhill':
            name = tags.get('name', 'Unbenannt')
            pts = [(nodes[nid][0], nodes[nid][1]) for nid in node_ids if nid in nodes]
            if pts:
                pisten.append({'name': name, 'points': pts})
            continue
        if ptype == 'nordic':
            name = tags.get('name', 'Unbenannt')
            pts = [(nodes[nid][0], nodes[nid][1]) for nid in node_ids if nid in nodes]
            if pts:
                loipen.append({'name': name, 'points': pts})
            continue

        # Aerialways / Lifts
        aerialway_type = tags.get('aerialway', '')
        if aerialway_type and aerialway_type not in AERIALWAY_FILTER_OUT:
            german_type = AERIALWAY_KEEP.get(aerialway_type, aerialway_type)
            if aerialway_type not in AERIALWAY_KEEP:
                # Unknown type, skip
                continue
            name = tags.get('name', 'Unbenannt')
            # Use FIRST node as bottom station GPS
            first_node_id = node_ids[0] if node_ids else None
            if first_node_id and first_node_id in nodes:
                lat, lon = nodes[first_node_id]
                lifts.append({
                    'name': name,
                    'typ': german_type,
                    'osm_type': aerialway_type,
                    'lat': lat,
                    'lng': lon,
                })

    print(f"  -> {len(pisten)} Abfahrtspisten")
    print(f"  -> {len(loipen)} Loipen")
    print(f"  -> {len(lifts)} Lifte/Bergbahnen")

    # Print lift summary by type
    type_counts = {}
    for l in lifts:
        type_counts[l['typ']] = type_counts.get(l['typ'], 0) + 1
    for typ, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"     {typ}: {count}")

    return pisten, loipen, lifts


# =====================================================================
# GOOGLE ROUTES API — Walking time to nearest lift
# =====================================================================

def calc_walking_route(origin_lat, origin_lng, dest_lat, dest_lng):
    """Calculate real walking distance and time via Google Routes API."""
    url = "https://routes.googleapis.com/directions/v2:computeRoutes"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": "routes.distanceMeters,routes.duration"
    }
    data = {
        "origin": {"location": {"latLng": {"latitude": origin_lat, "longitude": origin_lng}}},
        "destination": {"location": {"latLng": {"latitude": dest_lat, "longitude": dest_lng}}},
        "travelMode": "WALK"
    }
    resp = requests.post(url, headers=headers, json=data)
    if resp.status_code == 200:
        result = resp.json()
        if "routes" in result and result["routes"]:
            route = result["routes"][0]
            dist_m = route.get("distanceMeters", None)
            dur_s = route.get("duration", "0s")
            dur_min = int(dur_s.replace("s", "")) // 60 if isinstance(dur_s, str) else 0
            return dist_m, dur_min
    return None, None


# =====================================================================
# HELPERS
# =====================================================================

def extract_ortsteil(address):
    """Determine Ortsteil from Google address."""
    addr = address.lower()
    if "riezlern" in addr:
        return "Riezlern"
    elif "hirschegg" in addr:
        return "Hirschegg"
    elif "mittelberg" in addr:
        return "Mittelberg"
    elif "baad" in addr:
        return "Baad"
    return ""


def extract_typ(types, name):
    """Determine accommodation type from Google types and name."""
    name_lower = name.lower()
    if "apartment" in (types or []) or "ferienwohnung" in name_lower or "apartment" in name_lower:
        return "Ferienwohnung"
    elif "garni" in name_lower:
        return "Garni"
    elif any(w in name_lower for w in ["pension", "gaestehaus", "gastehaus", "gästehaus"]):
        return "Pension"
    elif "gasthof" in name_lower:
        return "Gasthof"
    elif "resort" in name_lower:
        return "Resort"
    elif "lodge" in name_lower:
        return "Lodge"
    elif "hotel" in name_lower:
        return "Hotel"
    return "Unterkunft"


# =====================================================================
# AI DESCRIPTIONS (Claude API)
# =====================================================================

def generate_ai_descriptions(hotels, existing_descriptions):
    """Generate AI descriptions for hotels using Claude API."""
    if not ANTHROPIC_API_KEY:
        print("  WARNUNG: ANTHROPIC_API_KEY nicht gesetzt, ueberspringe AI-Beschreibungen")
        return hotels

    try:
        import anthropic
    except ImportError:
        print("  WARNUNG: anthropic Paket nicht installiert, ueberspringe AI-Beschreibungen")
        return hotels

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    system_prompt = (
        "Du bist ein lokaler Reiseexperte fuer Alpenurlaub. "
        "Schreibe eine authentische, ehrliche Beschreibung in 3-4 Saetzen auf Deutsch. "
        "Kein Marketing-Blabla. Fokus auf was das Hotel wirklich besonders macht. "
        "Verwende die echten Distanzen und Fakten."
    )

    for i, hotel in enumerate(hotels):
        name = hotel['name']

        # Check if we already have a description from descriptions.json
        existing = existing_descriptions.get(name)
        if not existing:
            # Fuzzy match
            name_lower = name.lower()
            for key, val in existing_descriptions.items():
                if (key.lower() == name_lower or
                        name_lower in key.lower() or
                        key.lower() in name_lower):
                    existing = val
                    break

        if existing:
            hotel['ai_description'] = existing
            print(f"  [{i + 1}/{len(hotels)}] {name} — bestehende Beschreibung uebernommen")
            continue

        # Build prompt with real data
        bergbahn = hotel.get('naechste_bergbahn')
        piste = hotel.get('naechste_piste')
        loipe = hotel.get('naechste_loipe')

        facts = []
        facts.append(f"Hotel: {name}")
        facts.append(f"Ortsteil: {hotel.get('ortsteil', 'unbekannt')}")
        facts.append(f"Typ: {hotel.get('typ', 'Unterkunft')}")
        if hotel.get('google_rating'):
            facts.append(f"Google Bewertung: {hotel['google_rating']} ({hotel.get('google_reviews', 0)} Bewertungen)")
        if bergbahn:
            facts.append(f"Naechste Bergbahn: {bergbahn['name']} ({bergbahn['typ']}), {bergbahn['distanz_m']}m entfernt, {bergbahn['gehzeit_min']} Min Gehzeit")
        if piste:
            facts.append(f"Naechste Piste: {piste['name']}, {piste['distanz_m']}m Luftlinie")
        if loipe:
            facts.append(f"Naechste Loipe: {loipe['name']}, {loipe['distanz_m']}m Luftlinie")
        facts.append(f"Ski-in/Ski-out: {hotel.get('ski_in_out', 'nein')}")

        prompt = "Schreibe eine kurze, authentische Beschreibung fuer diese Unterkunft basierend auf diesen Fakten:\n\n" + "\n".join(facts)

        try:
            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=300,
                system=system_prompt,
                messages=[{"role": "user", "content": prompt}]
            )
            desc = response.content[0].text.strip()
            hotel['ai_description'] = desc
            print(f"  [{i + 1}/{len(hotels)}] {name} — AI-Beschreibung generiert")
        except Exception as e:
            print(f"  [{i + 1}/{len(hotels)}] {name} — FEHLER: {e}")
            hotel['ai_description'] = None

        time.sleep(0.5)  # Rate limit Claude API

    return hotels


# =====================================================================
# BUILD HOTEL OBJECT
# =====================================================================

def build_hotel(place, nearest_lift_info, walk_dist_m, walk_min, osm_piste, osm_loipe):
    """Build a clean hotel object."""
    name = place.get("displayName", {}).get("text", "")
    address = place.get("formattedAddress", "")
    lat = place.get("location", {}).get("latitude", 0)
    lng = place.get("location", {}).get("longitude", 0)
    rating = place.get("rating", None)
    reviews = place.get("userRatingCount", 0)
    website = place.get("websiteUri", "")
    types = place.get("types", [])

    ortsteil = extract_ortsteil(address)
    typ = extract_typ(types, name)

    # Ski-in/Ski-out based on piste distance
    ski_in_out = "nein"
    if osm_piste and osm_piste["distanz_m"] <= 50:
        ski_in_out = "ja"
    elif osm_piste and osm_piste["distanz_m"] <= 150:
        ski_in_out = "fast"

    # Bergbahn info
    bergbahn = None
    if nearest_lift_info and walk_dist_m is not None:
        bergbahn = {
            "name": nearest_lift_info['name'],
            "typ": nearest_lift_info['typ'],
            "distanz_m": walk_dist_m,
            "gehzeit_min": walk_min,
        }

    hotel = {
        "name": name,
        "ortsteil": ortsteil,
        "typ": typ,
        "adresse": address,
        "gps": {"lat": lat, "lng": lng},
        "website": website,
        "google_rating": rating,
        "google_reviews": reviews,
        "naechste_bergbahn": bergbahn,
        "naechste_piste": osm_piste,
        "naechste_loipe": osm_loipe,
        "ski_in_out": ski_in_out,
        "ai_description": None,
        "booking_url": f"https://www.booking.com/searchresults.de.html?ss={urllib.parse.quote(name + ' Kleinwalsertal')}",
    }
    return hotel


# =====================================================================
# MAIN
# =====================================================================

def main():
    print("=" * 60)
    print("AlpenStay — Automated Data Pipeline v2")
    print("=" * 60)
    print()

    # Step 1: Find all hotels
    print("SCHRITT 1: Hotels suchen via Google Places API...")
    print("-" * 40)
    all_places = find_all_hotels()

    # Filter: only active businesses
    active_places = [
        p for p in all_places
        if p.get("businessStatus", "") != "CLOSED_PERMANENTLY"
    ]
    print(f"\nAktive Betriebe: {len(active_places)}")

    # Step 2: Load ALL OSM data in ONE call
    print()
    print("SCHRITT 2: OSM Daten laden (Pisten + Loipen + Lifte)...")
    print("-" * 40)
    osm_pisten, osm_loipen, osm_lifts = fetch_osm_data()

    if not osm_lifts:
        print("FEHLER: Keine Lifte von OSM geladen, Abbruch!")
        return

    # Step 3: For each hotel, calculate distances
    print()
    print("SCHRITT 3: Distanzen berechnen...")
    print("-" * 40)

    hotels = []
    skipped = []

    for i, place in enumerate(active_places):
        name = place.get("displayName", {}).get("text", "?")
        lat = place.get("location", {}).get("latitude", 0)
        lng = place.get("location", {}).get("longitude", 0)

        print(f"\n[{i + 1}/{len(active_places)}] {name}")

        # Find nearest lift (haversine first)
        nearest_lift, luftlinie_m = find_nearest_lift_haversine(lat, lng, osm_lifts)

        # Filter: >10km from any lift = not in Kleinwalsertal
        if luftlinie_m > 10000:
            print(f"  UEBERSPRUNGEN: Naechster Lift {luftlinie_m}m Luftlinie — nicht im Kleinwalsertal")
            skipped.append(name)
            continue

        if nearest_lift:
            print(f"  Naechster Lift (Luftlinie): {nearest_lift['name']} ({nearest_lift['typ']}) — {luftlinie_m}m")

        # Google Routes: walking time to nearest lift ONLY (1 call per hotel)
        walk_dist_m, walk_min = None, None
        if nearest_lift:
            walk_dist_m, walk_min = calc_walking_route(lat, lng, nearest_lift['lat'], nearest_lift['lng'])
            if walk_dist_m is not None:
                print(f"  Gehzeit: {walk_dist_m}m, {walk_min} Min")
            else:
                # Fallback: use Luftlinie
                walk_dist_m = luftlinie_m
                walk_min = max(1, round(luftlinie_m / 80))  # ~80m/min walking
                print(f"  Routes API Fallback: ~{walk_dist_m}m, ~{walk_min} Min (geschaetzt)")
            time.sleep(0.2)  # Rate limit

        # OSM: nearest piste
        osm_piste = find_nearest_track(lat, lng, osm_pisten) if osm_pisten else None
        if osm_piste:
            print(f"  Piste: {osm_piste['name']} ({osm_piste['distanz_m']}m)")

        # OSM: nearest loipe
        osm_loipe = find_nearest_track(lat, lng, osm_loipen) if osm_loipen else None
        if osm_loipe:
            print(f"  Loipe: {osm_loipe['name']} ({osm_loipe['distanz_m']}m)")

        # Build hotel object
        hotel = build_hotel(place, nearest_lift, walk_dist_m, walk_min, osm_piste, osm_loipe)
        hotels.append(hotel)

    if skipped:
        print(f"\n{len(skipped)} Hotels uebersprungen (nicht im Kleinwalsertal):")
        for s in skipped:
            print(f"  - {s}")

    # Step 4: Load existing descriptions
    print()
    print("SCHRITT 4: Beschreibungen laden...")
    print("-" * 40)
    desc_path = PROJECT_ROOT / "data" / "descriptions.json"
    existing_descriptions = {}
    if desc_path.exists():
        try:
            existing_descriptions = json.loads(desc_path.read_text(encoding="utf-8"))
            print(f"  {len(existing_descriptions)} bestehende Beschreibungen geladen")
        except Exception as e:
            print(f"  Fehler beim Laden: {e}")

    # Step 5: Generate AI descriptions
    print()
    print("SCHRITT 5: AI-Beschreibungen generieren (Claude API)...")
    print("-" * 40)
    hotels = generate_ai_descriptions(hotels, existing_descriptions)

    # Step 6: Save results
    print()
    print("SCHRITT 6: Ergebnis speichern...")
    print("-" * 40)

    result = {
        "destination": "Kleinwalsertal",
        "slug": "kleinwalsertal",
        "region": "Vorarlberg, Oesterreich",
        "hotel_count": len(hotels),
        "datenquelle": "Google Places API + OSM Overpass API + Google Routes API + Claude AI",
        "stand": time.strftime("%Y-%m-%d"),
        "hotels": hotels,
    }

    out_path = PROJECT_ROOT / "data" / "kleinwalsertal_real.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Gespeichert: {out_path} ({len(hotels)} Hotels)")

    # Summary
    print()
    print("=" * 60)
    print("ZUSAMMENFASSUNG")
    print("=" * 60)
    print(f"Hotels gefunden:    {len(hotels)}")
    print(f"Uebersprungen:      {len(skipped)} (zu weit weg)")
    print(f"Mit Rating:         {sum(1 for h in hotels if h['google_rating'])}")
    print(f"Mit Website:        {sum(1 for h in hotels if h['website'])}")
    print(f"Ski-in/Ski-out:     {sum(1 for h in hotels if h['ski_in_out'] == 'ja')}")
    print(f"Fast Ski-in/out:    {sum(1 for h in hotels if h['ski_in_out'] == 'fast')}")
    print(f"Mit Piste:          {sum(1 for h in hotels if h.get('naechste_piste'))}")
    print(f"Mit Loipe:          {sum(1 for h in hotels if h.get('naechste_loipe'))}")
    print(f"Mit AI-Beschr.:     {sum(1 for h in hotels if h.get('ai_description'))}")

    ortsteile = {}
    for h in hotels:
        ot = h["ortsteil"] or "Unbekannt"
        ortsteile[ot] = ortsteile.get(ot, 0) + 1
    print(f"\nNach Ortsteil:")
    for ot, count in sorted(ortsteile.items(), key=lambda x: -x[1]):
        print(f"  {ot}: {count}")


if __name__ == "__main__":
    main()
