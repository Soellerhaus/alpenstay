#!/usr/bin/env python3
"""
AlpenStay — Echte Hotel-Daten Pipeline
=======================================
Holt ALLE Daten von Google Places + Routes API.
Keine Schaetzungen, nur echte Daten.

Ergebnis: data/kleinwalsertal.json
"""

import json
import math
import os
import time
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")
if not API_KEY:
    raise ValueError("GOOGLE_PLACES_API_KEY nicht in .env gefunden!")

# =====================================================================
# BEKANNTE ORTE IM KLEINWALSERTAL (echte GPS-Koordinaten)
# =====================================================================

# GPS-Koordinaten von Google Places API verifiziert
BERGBAHNEN = {
    "Kanzelwandbahn": {"lat": 47.3558113, "lng": 10.1846243, "typ": "Gondelbahn"},          # Walserstrasse 77, Riezlern
    "Walmendingerhorn-Bahn": {"lat": 47.3229909, "lng": 10.1523906, "typ": "Gondelbahn"},   # Mittelberg
    "Ifenbahn": {"lat": 47.3426246, "lng": 10.1375605, "typ": "Gondelbahn"},                 # Auenalpe 4, Hirschegg
    "Heuberg-Arena": {"lat": 47.3428198, "lng": 10.1661431, "typ": "Sessellift/Schlepplift"},# Walserstrasse 262a, Hirschegg
    "Soellereckbahn": {"lat": 47.400288, "lng": 10.2422934, "typ": "Sessellift"},            # Kornau, Oberstdorf
    "Parsenn-Schlepplift": {"lat": 47.3549422, "lng": 10.1773648, "typ": "Schlepplift"},     # Schwarzwassertalstrasse, Riezlern
    "Zaferna-Sessellift": {"lat": 47.3275864, "lng": 10.1534248, "typ": "Sessellift"},       # Mittelberg
}

# Loipen-Einstiege — Schwarzwassertal am Sportplatz Hirschegg
LOIPEN_EINSTIEGE = {
    "Loipe Riezlern (Steinbogen)": {"lat": 47.3555, "lng": 10.1830},                         # Naehe Parsenn/Steinbogen
    "Loipe Hirschegg (Sportplatz)": {"lat": 47.3440, "lng": 10.1700},                        # Sportplatz Hirschegg an der Breitach
    "Loipe Baad (Baergunttal)": {"lat": 47.3100, "lng": 10.1400},                            # Einstieg Baad Richtung Baergunttal
    "Loipe Mittelberg (Waeldele)": {"lat": 47.3280, "lng": 10.1520},                         # Waeldele Mittelberg
}

# =====================================================================
# GOOGLE PLACES API — Hotels suchen
# =====================================================================

def search_places(query, max_results=20):
    """Sucht nach Hotels/Unterkuenften via Google Places Text Search."""
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
            "places.priceLevel",
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

    result = resp.json()
    return result.get("places", [])


def find_all_hotels():
    """Findet alle Hotels/Unterkuenfte im Kleinwalsertal."""
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

        time.sleep(0.5)  # Rate limiting

    print(f"\nGesamt: {len(all_places)} einzigartige Unterkuenfte gefunden")
    return all_places


# =====================================================================
# ORTSTEIL AUS ADRESSE EXTRAHIEREN
# =====================================================================

def extract_ortsteil(address):
    """Bestimmt Ortsteil aus der Google-Adresse."""
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
    """Bestimmt Unterkunftstyp aus Google-Types und Namen."""
    name_lower = name.lower()
    if "apartment" in (types or []) or "ferienwohnung" in name_lower or "apartment" in name_lower:
        return "Ferienwohnung"
    elif "garni" in name_lower:
        return "Garni"
    elif "pension" in name_lower or "gaestehaus" in name_lower or "gastehaus" in name_lower or "gästehaus" in name_lower:
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
# GOOGLE ROUTES API — Echte Distanzen berechnen
# =====================================================================

def calc_walking_distance(origin_lat, origin_lng, dest_lat, dest_lng):
    """Berechnet echte Gehdistanz und -zeit via Google Routes API."""
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


def find_nearest_bergbahn(hotel_lat, hotel_lng):
    """Findet die naechste Bergbahn und berechnet echte Gehdistanz."""
    nearest = None
    nearest_dist = float("inf")
    nearest_dur = None
    nearest_name = ""
    nearest_typ = ""

    for name, info in BERGBAHNEN.items():
        dist_m, dur_min = calc_walking_distance(
            hotel_lat, hotel_lng, info["lat"], info["lng"]
        )
        if dist_m is not None and dist_m < nearest_dist:
            nearest_dist = dist_m
            nearest_dur = dur_min
            nearest_name = name
            nearest_typ = info["typ"]
        time.sleep(0.15)  # Rate limiting

    if nearest_dist == float("inf"):
        return None

    return {
        "name": nearest_name,
        "typ": nearest_typ,
        "distanz_m": nearest_dist,
        "gehzeit_min": nearest_dur,
    }


def find_nearest_loipe(hotel_lat, hotel_lng):
    """Findet den naechsten Loipen-Einstieg und berechnet echte Gehdistanz."""
    nearest = None
    nearest_dist = float("inf")
    nearest_dur = None
    nearest_name = ""

    for name, info in LOIPEN_EINSTIEGE.items():
        dist_m, dur_min = calc_walking_distance(
            hotel_lat, hotel_lng, info["lat"], info["lng"]
        )
        if dist_m is not None and dist_m < nearest_dist:
            nearest_dist = dist_m
            nearest_dur = dur_min
            nearest_name = name
        time.sleep(0.15)  # Rate limiting

    if nearest_dist == float("inf"):
        return None

    return {
        "name": nearest_name,
        "distanz_m": nearest_dist,
        "gehzeit_min": nearest_dur,
    }


# =====================================================================
# OSM OVERPASS API — Pisten und Loipen im Kleinwalsertal
# =====================================================================

def fetch_osm_pisten():
    """Holt alle Skipisten und Loipen aus OpenStreetMap via Overpass API."""
    query = '''
[out:json][timeout:30];
(
  way["piste:type"="downhill"](47.30,10.10,47.37,10.25);
  way["piste:type"="nordic"](47.30,10.10,47.37,10.25);
);
out body;
>;
out skel qt;
'''
    print("  Lade OSM Pisten/Loipen via Overpass API...")
    for attempt in range(3):
        try:
            resp = requests.post('https://overpass-api.de/api/interpreter', data={'data': query}, timeout=60)
            if resp.status_code == 429 or resp.status_code >= 500:
                print(f"  Overpass API Status {resp.status_code}, Retry {attempt+1}/3...")
                time.sleep(10 * (attempt + 1))
                continue
            data = resp.json()
            break
        except Exception as e:
            print(f"  Overpass API Fehler: {e}, Retry {attempt+1}/3...")
            time.sleep(10 * (attempt + 1))
    else:
        print("  WARNUNG: Overpass API nicht erreichbar, OSM-Daten uebersprungen")
        return [], []
    elements = data.get('elements', [])
    ways = [e for e in elements if e['type'] == 'way']
    nodes = {e['id']: (e['lat'], e['lon']) for e in elements if e['type'] == 'node'}

    pisten = []
    loipen = []
    for w in ways:
        tags = w.get('tags', {})
        ptype = tags.get('piste:type', '')
        name = tags.get('name', 'Unbenannt')
        pts = [(nodes[nid][0], nodes[nid][1]) for nid in w.get('nodes', []) if nid in nodes]
        if ptype == 'downhill':
            pisten.append({'name': name, 'points': pts})
        elif ptype == 'nordic':
            loipen.append({'name': name, 'points': pts})

    print(f"  -> {len(pisten)} Abfahrtspisten, {len(loipen)} Loipen geladen")
    return pisten, loipen


def haversine(lat1, lon1, lat2, lon2):
    """Berechnet Luftliniendistanz zwischen zwei GPS-Punkten in Metern."""
    R = 6371000
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def point_to_segment(px, py, ax, ay, bx, by):
    """Berechnet kuerzeste Distanz von Punkt zu Liniensegment."""
    dx, dy = bx - ax, by - ay
    if dx == 0 and dy == 0:
        return haversine(px, py, ax, ay)
    t = max(0, min(1, ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)))
    proj_lat = ax + t * dx
    proj_lon = ay + t * dy
    return haversine(px, py, proj_lat, proj_lon)


def find_nearest_osm_track(hotel_lat, hotel_lon, tracks):
    """Findet die naechste Piste/Loipe aus OSM-Daten."""
    best_dist = float('inf')
    best_name = ''
    for track in tracks:
        pts = track['points']
        for i in range(len(pts) - 1):
            d = point_to_segment(hotel_lat, hotel_lon, pts[i][0], pts[i][1], pts[i+1][0], pts[i+1][1])
            if d < best_dist:
                best_dist = d
                best_name = track['name']
    if best_dist == float('inf'):
        return None
    return {"name": best_name, "distanz_m": round(best_dist)}


# =====================================================================
# HOTEL-OBJEKT BAUEN
# =====================================================================

def build_hotel(place, bergbahn, loipe, osm_piste=None, osm_loipe=None):
    """Baut ein Hotel-Objekt mit allen echten Daten."""
    name = place.get("displayName", {}).get("text", "")
    address = place.get("formattedAddress", "")
    lat = place.get("location", {}).get("latitude", 0)
    lng = place.get("location", {}).get("longitude", 0)
    rating = place.get("rating", None)
    reviews = place.get("userRatingCount", 0)
    website = place.get("websiteUri", "")
    types = place.get("types", [])
    price_level = place.get("priceLevel", None)
    business_status = place.get("businessStatus", "")

    ortsteil = extract_ortsteil(address)
    typ = extract_typ(types, name)

    # Ski-in/Ski-out: basierend auf OSM Pisten-Distanz (genauer als Bergbahn-Distanz)
    ski_in_out = "nein"
    if osm_piste and osm_piste["distanz_m"] <= 50:
        ski_in_out = "ja"
    elif osm_piste and osm_piste["distanz_m"] <= 150:
        ski_in_out = "fast (unter 150m)"

    hotel = {
        # Basis (alles von Google)
        "name": name,
        "google_place_id": place.get("id", ""),
        "ortsteil": ortsteil,
        "typ": typ,
        "adresse": address,
        "gps": {"lat": lat, "lng": lng},
        "website": website,
        "business_status": business_status,

        # Google Rating (echt)
        "google_rating": rating,
        "google_reviews": reviews,
        "price_level": price_level,

        # Naechste Bergbahn (echte Gehdistanz via Google Routes)
        "naechste_bergbahn": bergbahn,

        # Naechste Loipe (echte Gehdistanz via Google Routes)
        "naechste_loipe": loipe,

        # Naechste Piste (OSM Luftlinie)
        "naechste_piste": osm_piste,

        # Naechste Loipe OSM (Luftlinie)
        "naechste_loipe_osm": osm_loipe,

        # Ski-in/Ski-out (abgeleitet aus OSM Pisten-Distanz)
        "ski_in_out": ski_in_out,

        # Diese Felder muessen manuell/spaeter ergaenzt werden:
        "wellness": None,  # Unbekannt — muss manuell gepflegt werden
        "familie": None,   # Unbekannt
        "adults_only": None,  # Unbekannt
        "verpflegung": None,  # Unbekannt (HP, VP, etc.)

        # Wird spaeter von Claude generiert
        "ai_description": None,

        # Booking.com Link (Suchlink als Fallback)
        "booking_url": f"https://www.booking.com/searchresults.de.html?ss={requests.utils.quote(name + ' Kleinwalsertal')}",

        # Datenquelle fuer Transparenz
        "datenquelle": {
            "hotel_daten": "Google Places API",
            "distanzen": "Google Routes API (echte Gehstrecke) + OSM Overpass (Pisten/Loipen)",
            "rating": "Google",
            "stand": time.strftime("%Y-%m-%d"),
        }
    }

    return hotel


# =====================================================================
# HAUPTPROGRAMM
# =====================================================================

def main():
    print("=" * 60)
    print("AlpenStay — Echte Hotel-Daten Pipeline")
    print("=" * 60)
    print()

    # Schritt 1: Alle Hotels finden
    print("SCHRITT 1: Hotels suchen via Google Places API...")
    print("-" * 40)
    all_places = find_all_hotels()

    # Filter: nur aktive Betriebe
    active_places = [
        p for p in all_places
        if p.get("businessStatus", "") != "CLOSED_PERMANENTLY"
    ]
    print(f"\nAktive Betriebe: {len(active_places)}")

    # Schritt 2: OSM Pisten/Loipen laden
    print()
    print("SCHRITT 2: OSM Pisten/Loipen laden via Overpass API...")
    print("-" * 40)
    osm_pisten, osm_loipen = fetch_osm_pisten()

    # Schritt 3: Fuer jedes Hotel Distanzen berechnen
    print()
    print("SCHRITT 3: Distanzen berechnen via Google Routes API + OSM...")
    print("-" * 40)

    hotels = []
    skipped = []
    for i, place in enumerate(active_places):
        name = place.get("displayName", {}).get("text", "?")
        lat = place.get("location", {}).get("latitude", 0)
        lng = place.get("location", {}).get("longitude", 0)

        print(f"\n[{i+1}/{len(active_places)}] {name}")

        # Naechste Bergbahn
        print(f"  Berechne naechste Bergbahn...")
        bergbahn = find_nearest_bergbahn(lat, lng)
        if bergbahn:
            print(f"  -> {bergbahn['name']}: {bergbahn['distanz_m']}m ({bergbahn['gehzeit_min']} Min)")

        # Filter: Hotels mit Bergbahn-Distanz > 10000m sind nicht im Kleinwalsertal
        if bergbahn and bergbahn["distanz_m"] > 10000:
            print(f"  UEBERSPRUNGEN: Bergbahn zu weit ({bergbahn['distanz_m']}m) — nicht im Kleinwalsertal")
            skipped.append(name)
            continue
        if bergbahn is None:
            print(f"  UEBERSPRUNGEN: Keine Bergbahn erreichbar")
            skipped.append(name)
            continue

        # Naechste Loipe (Google Routes)
        print(f"  Berechne naechste Loipe...")
        loipe = find_nearest_loipe(lat, lng)
        if loipe:
            print(f"  -> {loipe['name']}: {loipe['distanz_m']}m ({loipe['gehzeit_min']} Min)")

        # OSM: Naechste Piste
        osm_piste = find_nearest_osm_track(lat, lng, osm_pisten) if osm_pisten else None
        if osm_piste:
            print(f"  -> OSM Piste: {osm_piste['name']} ({osm_piste['distanz_m']}m)")

        # OSM: Naechste Loipe
        osm_loipe = find_nearest_osm_track(lat, lng, osm_loipen) if osm_loipen else None
        if osm_loipe:
            print(f"  -> OSM Loipe: {osm_loipe['name']} ({osm_loipe['distanz_m']}m)")

        # Hotel-Objekt bauen
        hotel = build_hotel(place, bergbahn, loipe, osm_piste, osm_loipe)
        hotels.append(hotel)

    if skipped:
        print(f"\n{len(skipped)} Hotels uebersprungen (nicht im Kleinwalsertal):")
        for s in skipped:
            print(f"  - {s}")

    # Schritt 4: Ergebnis speichern
    print()
    print("SCHRITT 4: Ergebnis speichern...")
    print("-" * 40)

    result = {
        "destination": "Kleinwalsertal",
        "slug": "kleinwalsertal",
        "region": "Vorarlberg, Oesterreich",
        "hotel_count": len(hotels),
        "datenquelle": "Google Places API + Google Routes API + OSM Overpass API",
        "stand": time.strftime("%Y-%m-%d"),
        "bergbahnen": {name: info for name, info in BERGBAHNEN.items()},
        "loipen": {name: info for name, info in LOIPEN_EINSTIEGE.items()},
        "hotels": hotels,
    }

    out_path = Path("data/kleinwalsertal_real.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Gespeichert: {out_path} ({len(hotels)} Hotels)")

    # Zusammenfassung
    print()
    print("=" * 60)
    print("ZUSAMMENFASSUNG")
    print("=" * 60)
    print(f"Hotels gefunden:    {len(hotels)}")
    print(f"Uebersprungen:      {len(skipped)} (zu weit weg)")
    print(f"Mit Rating:         {sum(1 for h in hotels if h['google_rating'])}")
    print(f"Mit Website:        {sum(1 for h in hotels if h['website'])}")
    print(f"Ski-in/Ski-out:     {sum(1 for h in hotels if h['ski_in_out'] == 'ja')}")
    print(f"Fast Ski-in/out:    {sum(1 for h in hotels if 'fast' in str(h['ski_in_out']))}")
    print(f"Mit OSM Piste:      {sum(1 for h in hotels if h.get('naechste_piste'))}")
    print(f"Mit OSM Loipe:      {sum(1 for h in hotels if h.get('naechste_loipe_osm'))}")

    # Ortsteile
    ortsteile = {}
    for h in hotels:
        ot = h["ortsteil"] or "Unbekannt"
        ortsteile[ot] = ortsteile.get(ot, 0) + 1
    print(f"\nNach Ortsteil:")
    for ot, count in sorted(ortsteile.items(), key=lambda x: -x[1]):
        print(f"  {ot}: {count}")

    print(f"\nAPI-Kosten (geschaetzt):")
    print(f"  Places: ~{len(all_places) // 20 + 1} Text Search Anfragen")
    print(f"  Routes: ~{len(active_places) * (len(BERGBAHNEN) + len(LOIPEN_EINSTIEGE))} Anfragen")
    print(f"  Alles im $200 Freivolumen")


if __name__ == "__main__":
    main()
