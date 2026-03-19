#!/usr/bin/env python3
"""
AlpenStay — Automated Hotel Data Pipeline v3
=============================================
100% automated, ZERO manual entries.

6-step pipeline:
1. FIND      — Google Places Text Search for all accommodations
2. VALIDATE  — Check Google types to filter non-accommodations
3. SKI DATA  — OSM Overpass for pistes, loipes, lifts + Google Routes walking time
4. ENRICH    — Google Places Details (reviews, editorial, goodForChildren)
               + website scraping + Claude analysis
5. DESCRIBE  — Claude generates AI description from ALL collected data
6. SAVE      — Output to data/kleinwalsertal_real.json

Data sources:
- Google Places API (Text Search + Details)
- OSM Overpass API (pistes, loipes, lifts)
- Google Routes API (walking time to nearest lift)
- Website scraping (simple HTML text extraction)
- Claude API (structured analysis + AI descriptions)

Output: data/kleinwalsertal_real.json
"""

import json
import math
import os
import time
import urllib.parse
import urllib.request
import ssl
from html.parser import HTMLParser
from pathlib import Path

# =====================================================================
# CONFIG & API KEYS — read directly from .env file
# =====================================================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent

def load_env_file(env_path):
    """Read .env file and return dict of key=value pairs."""
    env_vars = {}
    if not env_path.exists():
        return env_vars
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                env_vars[key] = value
    return env_vars

ENV = load_env_file(PROJECT_ROOT / ".env")

API_KEY = ENV.get("GOOGLE_PLACES_API_KEY", "")
if not API_KEY:
    raise ValueError("GOOGLE_PLACES_API_KEY nicht in .env gefunden!")

ANTHROPIC_API_KEY = ENV.get("ANTHROPIC_API_KEY", "")
if not ANTHROPIC_API_KEY:
    print("WARNUNG: ANTHROPIC_API_KEY nicht in .env gefunden!")

CLAUDE_MODEL = "claude-sonnet-4-20250514"

# Rate limits
GOOGLE_DELAY = 0.2   # seconds between Google API calls
CLAUDE_DELAY = 0.5   # seconds between Claude API calls

# Aerialway type mapping: OSM tag -> German name
AERIALWAY_KEEP = {
    "gondola": "Gondelbahn",
    "cable_car": "Seilbahn",
    "chair_lift": "Sessellift",
    "mixed_lift": "Kombibahn",
    "drag_lift": "Schlepplift",
    "t-bar": "Buegellift",
    "platter": "Tellerlift",
    "j-bar": "Ankerlift",
    "magic_carpet": "Foerderband",
}
AERIALWAY_FILTER_OUT = {"goods", "station", "zip_line", "rope_tow"}

# Accommodation types to ACCEPT
ACCEPT_TYPES = {
    "lodging", "hotel", "guest_house", "resort_hotel",
    "bed_and_breakfast", "motel", "hostel", "campground", "cottage",
}

# Types to REJECT (if no lodging type present)
REJECT_TYPES = {
    "travel_agency", "museum", "park", "church",
    "local_government_office", "tourist_information_center",
}


# =====================================================================
# HTTP HELPERS (using requests)
# =====================================================================

import requests


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
    """Find nearest lift using haversine. Returns (lift_info, distance_m)."""
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
# WEBSITE SCRAPING — Simple HTML text extraction
# =====================================================================

class TextExtractor(HTMLParser):
    """Simple HTML parser that extracts visible text content."""
    SKIP_TAGS = {"script", "style", "noscript", "svg", "path", "meta", "link", "head"}

    def __init__(self):
        super().__init__()
        self.text_parts = []
        self._skip_depth = 0

    def handle_starttag(self, tag, attrs):
        if tag.lower() in self.SKIP_TAGS:
            self._skip_depth += 1

    def handle_endtag(self, tag):
        if tag.lower() in self.SKIP_TAGS:
            self._skip_depth = max(0, self._skip_depth - 1)

    def handle_data(self, data):
        if self._skip_depth == 0:
            text = data.strip()
            if text:
                self.text_parts.append(text)

    def get_text(self):
        return " ".join(self.text_parts)


def scrape_website(url, timeout=8):
    """Scrape text content from a website. Returns text or empty string on failure."""
    if not url:
        return ""
    try:
        if not url.startswith("http"):
            url = "https://" + url
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; AlpenStay/1.0)",
            "Accept": "text/html",
            "Accept-Language": "de,en;q=0.5",
        }
        resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        if resp.status_code != 200:
            return ""
        content_type = resp.headers.get("content-type", "")
        if "html" not in content_type.lower() and "text" not in content_type.lower():
            return ""
        html = resp.text
        parser = TextExtractor()
        parser.feed(html)
        text = parser.get_text()
        # If less than 100 chars, likely a JS-heavy site
        if len(text) < 100:
            return ""
        # Limit to first 3000 chars for Claude
        return text[:3000]
    except Exception as e:
        return ""


# =====================================================================
# STEP 1: FIND — Google Places Text Search
# =====================================================================

def search_places(query, max_results=20):
    """Search for accommodations via Google Places Text Search."""
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


def step1_find():
    """STEP 1: Find all accommodations in Kleinwalsertal via Google Places."""
    print("=" * 60)
    print("SCHRITT 1: FIND — Hotels suchen via Google Places API")
    print("=" * 60)

    queries = [
        # Hotels
        "Hotels Kleinwalsertal Vorarlberg",
        "Hotel Riezlern Kleinwalsertal",
        "Hotel Hirschegg Kleinwalsertal",
        "Hotel Mittelberg Kleinwalsertal",
        "Hotel Baad Kleinwalsertal",
        "Garni Hotel Kleinwalsertal",
        "Aparthotel Kleinwalsertal",
        # Pensionen & Gaestehaeuser
        "Pension Kleinwalsertal",
        "Pension Riezlern",
        "Pension Hirschegg Vorarlberg",
        "Pension Mittelberg Vorarlberg",
        "Gästehaus Kleinwalsertal",
        "Gästehaus Riezlern",
        "Gästehaus Hirschegg Kleinwalsertal",
        "Gästehaus Mittelberg Kleinwalsertal",
        # Ferienwohnungen & Chalets
        "Ferienwohnung Kleinwalsertal",
        "Ferienwohnung Riezlern",
        "Ferienwohnung Hirschegg",
        "Ferienwohnung Mittelberg",
        "Chalet Kleinwalsertal",
        "Appartement Kleinwalsertal",
        # Gasthoeffe & sonstige
        "Gasthof Kleinwalsertal",
        "Unterkunft Kleinwalsertal",
        "Zimmer Kleinwalsertal",
        "Haus Baad Kleinwalsertal",
    ]

    seen_ids = set()
    all_places = []

    for i, query in enumerate(queries):
        print(f"  [{i+1}/{len(queries)}] Suche: '{query}'...")
        places = search_places(query, max_results=20)
        new_count = 0
        for p in places:
            place_id = p.get("id", "")
            if place_id not in seen_ids:
                seen_ids.add(place_id)
                all_places.append(p)
                new_count += 1
        print(f"    -> {len(places)} Ergebnisse, {new_count} neu")
        time.sleep(GOOGLE_DELAY)

    print(f"\nGesamt: {len(all_places)} einzigartige Unterkuenfte gefunden")
    return all_places


# =====================================================================
# STEP 2: VALIDATE — Filter non-accommodations
# =====================================================================

def step2_validate(all_places, lifts):
    """STEP 2: Validate places — filter out non-accommodations."""
    print()
    print("=" * 60)
    print("SCHRITT 2: VALIDATE — Nicht-Unterkuenfte filtern")
    print("=" * 60)

    validated = []
    rejected = []

    for p in all_places:
        name = p.get("displayName", {}).get("text", "?")
        types = set(p.get("types", []))
        status = p.get("businessStatus", "")

        # Reject permanently closed
        if status == "CLOSED_PERMANENTLY":
            rejected.append((name, "permanent geschlossen"))
            continue

        # Check types: ACCEPT if any accommodation type
        has_lodging = bool(types & ACCEPT_TYPES)

        # REJECT if has reject types WITHOUT lodging
        has_reject = bool(types & REJECT_TYPES)
        # Special case: tourist_attraction without lodging
        is_tourist_attraction = "tourist_attraction" in types and not has_lodging

        if not has_lodging and (has_reject or is_tourist_attraction):
            rejected.append((name, f"Typ: {', '.join(types & (REJECT_TYPES | {'tourist_attraction'}))}"))
            continue

        # Check distance from lifts (>10km = not in Kleinwalsertal)
        lat = p.get("location", {}).get("latitude", 0)
        lng = p.get("location", {}).get("longitude", 0)
        _, min_dist = find_nearest_lift_haversine(lat, lng, lifts)
        if min_dist > 10000:
            rejected.append((name, f"zu weit weg ({min_dist}m vom naechsten Lift)"))
            continue

        validated.append(p)

    print(f"\nValidiert: {len(validated)} Unterkuenfte")
    print(f"Abgelehnt: {len(rejected)}")
    if rejected:
        for name, reason in rejected:
            print(f"  - {name}: {reason}")

    return validated


# =====================================================================
# STEP 3: SKI DATA — OSM Overpass + Google Routes
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
    data = None
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

    if data is None:
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
            if aerialway_type not in AERIALWAY_KEEP:
                continue
            german_type = AERIALWAY_KEEP[aerialway_type]
            name = tags.get('name', 'Unbenannt')
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


def step3_ski_data(validated_places, pisten, loipen, lifts):
    """STEP 3: Calculate ski distances for each validated hotel."""
    print()
    print("=" * 60)
    print("SCHRITT 3: SKI DATA — Distanzen berechnen")
    print("=" * 60)

    results = []

    for i, place in enumerate(validated_places):
        name = place.get("displayName", {}).get("text", "?")
        lat = place.get("location", {}).get("latitude", 0)
        lng = place.get("location", {}).get("longitude", 0)

        print(f"  [{i+1}/{len(validated_places)}] {name}")

        # Find nearest lift (haversine)
        nearest_lift, luftlinie_m = find_nearest_lift_haversine(lat, lng, lifts)

        if nearest_lift:
            print(f"    Lift: {nearest_lift['name']} ({nearest_lift['typ']}) — {luftlinie_m}m Luftlinie")

        # Google Routes: walking time to nearest lift
        walk_dist_m, walk_min = None, None
        if nearest_lift:
            walk_dist_m, walk_min = calc_walking_route(lat, lng, nearest_lift['lat'], nearest_lift['lng'])
            if walk_dist_m is not None:
                print(f"    Gehzeit: {walk_dist_m}m, {walk_min} Min")
            else:
                walk_dist_m = luftlinie_m
                walk_min = max(1, round(luftlinie_m / 80))
                print(f"    Routes Fallback: ~{walk_dist_m}m, ~{walk_min} Min")
            time.sleep(GOOGLE_DELAY)

        # OSM: nearest piste
        osm_piste = find_nearest_track(lat, lng, pisten) if pisten else None
        if osm_piste:
            print(f"    Piste: {osm_piste['name']} ({osm_piste['distanz_m']}m)")

        # OSM: nearest loipe
        osm_loipe = find_nearest_track(lat, lng, loipen) if loipen else None
        if osm_loipe:
            print(f"    Loipe: {osm_loipe['name']} ({osm_loipe['distanz_m']}m)")

        # Ski-in/Ski-out based on piste distance
        ski_in_out = "nein"
        if osm_piste and osm_piste["distanz_m"] <= 50:
            ski_in_out = "ja"
        elif osm_piste and osm_piste["distanz_m"] <= 150:
            ski_in_out = "fast"

        # Bergbahn info
        bergbahn = None
        if nearest_lift and walk_dist_m is not None:
            bergbahn = {
                "name": nearest_lift['name'],
                "typ": nearest_lift['typ'],
                "distanz_m": walk_dist_m,
                "gehzeit_min": walk_min,
            }

        results.append({
            "place": place,
            "bergbahn": bergbahn,
            "piste": osm_piste,
            "loipe": osm_loipe,
            "ski_in_out": ski_in_out,
        })

    return results


# =====================================================================
# STEP 4: ENRICH — Google Details + Website + Claude Analysis
# =====================================================================

def fetch_place_details(place_id):
    """Fetch Google Places Details: reviews, editorialSummary, goodForChildren."""
    url = f"https://places.googleapis.com/v1/places/{place_id}"
    headers = {
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": "reviews,editorialSummary,goodForChildren",
    }
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json()
    return {}


def call_claude(system_prompt, user_prompt, max_tokens=800):
    """Call Claude API directly via HTTP."""
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
    }
    data = {
        "model": CLAUDE_MODEL,
        "max_tokens": max_tokens,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_prompt}],
    }
    resp = requests.post(url, headers=headers, json=data, timeout=60)
    if resp.status_code == 200:
        result = resp.json()
        if "content" in result and result["content"]:
            return result["content"][0].get("text", "")
    else:
        print(f"    Claude API Fehler: {resp.status_code} - {resp.text[:200]}")
    return ""


def claude_analyze_hotel(name, place_data, details, website_text):
    """Use Claude to analyze hotel data and extract structured information."""
    system_prompt = (
        "Du bist ein Datenanalyse-Assistent. Analysiere die gegebenen Hoteldaten "
        "und extrahiere strukturierte Informationen. Antworte NUR mit validem JSON, "
        "ohne Markdown-Formatierung, ohne Codeblocks, nur das JSON-Objekt."
    )

    # Build data for Claude
    info_parts = [f"Hotel: {name}"]

    # Google Places data
    types = place_data.get("types", [])
    if types:
        info_parts.append(f"Google Typen: {', '.join(types)}")

    rating = place_data.get("rating")
    if rating:
        info_parts.append(f"Google Rating: {rating}")

    reviews_count = place_data.get("userRatingCount", 0)
    info_parts.append(f"Anzahl Bewertungen: {reviews_count}")

    # Google Details
    editorial = details.get("editorialSummary", {}).get("text", "")
    if editorial:
        info_parts.append(f"Google Editorial: {editorial}")

    good_for_children = details.get("goodForChildren")
    if good_for_children is not None:
        info_parts.append(f"Gut fuer Kinder: {good_for_children}")

    # Reviews
    reviews = details.get("reviews", [])
    if reviews:
        review_texts = []
        for r in reviews[:5]:
            text = r.get("text", {}).get("text", "") if isinstance(r.get("text"), dict) else r.get("text", "")
            if text:
                review_texts.append(text[:300])
        if review_texts:
            info_parts.append("Google Bewertungen (Auszug):\n" + "\n---\n".join(review_texts))

    # Website text
    if website_text:
        info_parts.append(f"Website-Text (Auszug):\n{website_text[:2000]}")

    user_prompt = "\n\n".join(info_parts) + """

Analysiere diese Daten und antworte NUR mit diesem JSON-Format:
{
  "sterne": null,
  "wellness": false,
  "sauna": false,
  "pool": false,
  "familie": false,
  "adults_only": false,
  "verpflegung": "nur Uebernachtung",
  "haustiere": null,
  "parkplatz": null,
  "preis_kategorie": "mittel",
  "besonderheiten": "",
  "review_highlights": ""
}

Regeln:
- sterne: null wenn unbekannt, sonst 1-5
- verpflegung: "Fruehstueck" / "Halbpension" / "Vollpension" / "nur Uebernachtung"
- preis_kategorie: "budget" / "mittel" / "gehoben" / "luxus"
- besonderheiten: kurzer Text was das Hotel besonders macht (max 100 Zeichen)
- review_highlights: was Gaeste am meisten loben (max 100 Zeichen)
- Bei Unsicherheit lieber null/false setzen"""

    response_text = call_claude(system_prompt, user_prompt, max_tokens=500)
    if not response_text:
        return None

    # Parse JSON from response
    try:
        # Try to extract JSON from response (in case Claude wraps it)
        text = response_text.strip()
        # Remove markdown code blocks if present
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:])
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to find JSON in the response
        try:
            start = response_text.index("{")
            end = response_text.rindex("}") + 1
            return json.loads(response_text[start:end])
        except (ValueError, json.JSONDecodeError):
            print(f"    JSON Parse Fehler fuer {name}")
            return None


def step4_enrich(ski_results):
    """STEP 4: Enrich each hotel with Google Details, website scraping, and Claude analysis."""
    print()
    print("=" * 60)
    print("SCHRITT 4: ENRICH — Details, Website, Claude-Analyse")
    print("=" * 60)

    for i, result in enumerate(ski_results):
        place = result["place"]
        name = place.get("displayName", {}).get("text", "?")
        place_id = place.get("id", "")
        website = place.get("websiteUri", "")

        print(f"\n  [{i+1}/{len(ski_results)}] {name}")

        # a) Google Places Details
        print(f"    Google Details...")
        details = fetch_place_details(place_id) if place_id else {}
        time.sleep(GOOGLE_DELAY)

        editorial = details.get("editorialSummary", {}).get("text", "")
        if editorial:
            print(f"    Editorial: {editorial[:80]}...")

        reviews = details.get("reviews", [])
        print(f"    Reviews: {len(reviews)} gefunden")

        # b) Website scraping
        print(f"    Website scrapen: {website[:60]}..." if website else "    Keine Website")
        website_text = scrape_website(website) if website else ""
        if website_text:
            print(f"    Website: {len(website_text)} Zeichen extrahiert")
        elif website:
            print(f"    Website: kein Text extrahiert (JS-lastig oder Fehler)")

        # c) Claude analysis
        if ANTHROPIC_API_KEY:
            print(f"    Claude-Analyse...")
            analysis = claude_analyze_hotel(name, place, details, website_text)
            time.sleep(CLAUDE_DELAY)
        else:
            analysis = None

        result["details"] = details
        result["website_text"] = website_text
        result["analysis"] = analysis or {}

        # Save intermediate results every 20 hotels
        if (i + 1) % 20 == 0:
            save_intermediate(ski_results[:i+1], i+1)

    return ski_results


# =====================================================================
# STEP 5: DESCRIBE — Claude AI descriptions
# =====================================================================

def step5_describe(enriched_results):
    """STEP 5: Generate AI descriptions using Claude with ALL collected data."""
    print()
    print("=" * 60)
    print("SCHRITT 5: DESCRIBE — AI-Beschreibungen generieren")
    print("=" * 60)

    if not ANTHROPIC_API_KEY:
        print("  WARNUNG: ANTHROPIC_API_KEY nicht gesetzt, ueberspringe AI-Beschreibungen")
        return enriched_results

    system_prompt = (
        "Du bist ein lokaler Reiseexperte fuer Alpenurlaub im Kleinwalsertal. "
        "Schreibe eine authentische, ehrliche Beschreibung in 3-4 Saetzen auf Deutsch. "
        "Kein Marketing-Blabla. Fokus auf was das Hotel wirklich besonders macht. "
        "Verwende die echten Distanzen und Fakten. Erwaehne Ski-Naehe, Wellness, "
        "Bewertungshighlights wenn vorhanden."
    )

    for i, result in enumerate(enriched_results):
        place = result["place"]
        name = place.get("displayName", {}).get("text", "?")

        print(f"  [{i+1}/{len(enriched_results)}] {name}...")

        # Build comprehensive prompt
        facts = []
        facts.append(f"Hotel: {name}")

        address = place.get("formattedAddress", "")
        ortsteil = extract_ortsteil(address)
        if ortsteil:
            facts.append(f"Ortsteil: {ortsteil}")

        typ = extract_typ(place.get("types", []), name)
        facts.append(f"Typ: {typ}")

        rating = place.get("rating")
        reviews_count = place.get("userRatingCount", 0)
        if rating:
            facts.append(f"Google Bewertung: {rating} ({reviews_count} Bewertungen)")

        # Ski data
        bergbahn = result.get("bergbahn")
        piste = result.get("piste")
        loipe = result.get("loipe")
        ski_in_out = result.get("ski_in_out", "nein")

        if bergbahn:
            facts.append(f"Naechste Bergbahn: {bergbahn['name']} ({bergbahn['typ']}), {bergbahn['distanz_m']}m, {bergbahn['gehzeit_min']} Min Gehzeit")
        if piste:
            facts.append(f"Naechste Piste: {piste['name']}, {piste['distanz_m']}m")
        if loipe:
            facts.append(f"Naechste Loipe: {loipe['name']}, {loipe['distanz_m']}m")
        facts.append(f"Ski-in/Ski-out: {ski_in_out}")

        # Editorial summary
        editorial = result.get("details", {}).get("editorialSummary", {}).get("text", "")
        if editorial:
            facts.append(f"Google Editorial: {editorial}")

        # Analysis results
        analysis = result.get("analysis", {})
        if analysis:
            if analysis.get("wellness"):
                facts.append("Hat Wellness/Spa")
            if analysis.get("sauna"):
                facts.append("Hat Sauna")
            if analysis.get("pool"):
                facts.append("Hat Pool")
            if analysis.get("familie"):
                facts.append("Familienfreundlich")
            if analysis.get("adults_only"):
                facts.append("Adults Only")
            if analysis.get("verpflegung"):
                facts.append(f"Verpflegung: {analysis['verpflegung']}")
            if analysis.get("besonderheiten"):
                facts.append(f"Besonderheiten: {analysis['besonderheiten']}")
            if analysis.get("review_highlights"):
                facts.append(f"Gaeste loben: {analysis['review_highlights']}")

        # Review highlights from Google
        reviews = result.get("details", {}).get("reviews", [])
        if reviews:
            review_snippets = []
            for r in reviews[:3]:
                text = r.get("text", {}).get("text", "") if isinstance(r.get("text"), dict) else r.get("text", "")
                if text:
                    review_snippets.append(text[:150])
            if review_snippets:
                facts.append("Gaeste-Bewertungen (Auszug): " + " | ".join(review_snippets))

        prompt = "Schreibe eine kurze, authentische Beschreibung (3-4 Saetze) fuer diese Unterkunft:\n\n" + "\n".join(facts)

        description = call_claude(system_prompt, prompt, max_tokens=300)
        result["ai_description"] = description.strip() if description else None

        if description:
            print(f"    OK ({len(description)} Zeichen)")
        else:
            print(f"    FEHLER: Keine Beschreibung generiert")

        time.sleep(CLAUDE_DELAY)

    return enriched_results


# =====================================================================
# STEP 6: SAVE — Build final JSON and save
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


def build_hotel_object(result):
    """Build final hotel JSON object from enriched result."""
    place = result["place"]
    analysis = result.get("analysis", {})

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

    # Editorial summary
    editorial = result.get("details", {}).get("editorialSummary", {}).get("text", "")

    hotel = {
        "name": name,
        "ortsteil": ortsteil,
        "typ": typ,
        "sterne": analysis.get("sterne"),
        "adresse": address,
        "gps": {"lat": lat, "lng": lng},
        "website": website,
        "google_rating": rating,
        "google_reviews": reviews,
        "google_editorial": editorial,
        "naechste_bergbahn": result.get("bergbahn"),
        "naechste_piste": result.get("piste"),
        "naechste_loipe": result.get("loipe"),
        "ski_in_out": result.get("ski_in_out", "nein"),
        "wellness": analysis.get("wellness", False),
        "sauna": analysis.get("sauna", False),
        "pool": analysis.get("pool", False),
        "familie": analysis.get("familie", False),
        "adults_only": analysis.get("adults_only", False),
        "verpflegung": analysis.get("verpflegung", ""),
        "haustiere": analysis.get("haustiere"),
        "parkplatz": analysis.get("parkplatz"),
        "preis_kategorie": analysis.get("preis_kategorie", ""),
        "besonderheiten": analysis.get("besonderheiten", ""),
        "review_highlights": analysis.get("review_highlights", ""),
        "ai_description": result.get("ai_description"),
        "booking_url": f"https://www.booking.com/searchresults.de.html?ss={urllib.parse.quote(name + ' Kleinwalsertal')}",
        "validated": True,
    }

    return hotel


def save_intermediate(results, count):
    """Save intermediate results in case of crash."""
    out_path = PROJECT_ROOT / "data" / "kleinwalsertal_real_intermediate.json"
    hotels = [build_hotel_object(r) for r in results]
    data = {
        "destination": "Kleinwalsertal",
        "slug": "kleinwalsertal",
        "region": "Vorarlberg, Oesterreich",
        "hotel_count": len(hotels),
        "status": "intermediate",
        "hotels_processed": count,
        "stand": time.strftime("%Y-%m-%d"),
        "hotels": hotels,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n  --- Zwischenspeicherung: {count} Hotels gespeichert ---\n")


def step6_save(enriched_results):
    """STEP 6: Save final results to JSON."""
    print()
    print("=" * 60)
    print("SCHRITT 6: SAVE — Ergebnis speichern")
    print("=" * 60)

    hotels = [build_hotel_object(r) for r in enriched_results]

    result = {
        "destination": "Kleinwalsertal",
        "slug": "kleinwalsertal",
        "region": "Vorarlberg, Oesterreich",
        "hotel_count": len(hotels),
        "datenquelle": "Google Places API + OSM Overpass API + Google Routes API + Website Scraping + Claude AI",
        "stand": time.strftime("%Y-%m-%d"),
        "pipeline_version": "v3",
        "hotels": hotels,
    }

    out_path = PROJECT_ROOT / "data" / "kleinwalsertal_real.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\nGespeichert: {out_path} ({len(hotels)} Hotels)")

    # Print summary
    print()
    print("=" * 60)
    print("ZUSAMMENFASSUNG")
    print("=" * 60)
    print(f"Hotels gesamt:       {len(hotels)}")
    print(f"Mit Rating:          {sum(1 for h in hotels if h['google_rating'])}")
    print(f"Mit Website:         {sum(1 for h in hotels if h['website'])}")
    print(f"Mit Editorial:       {sum(1 for h in hotels if h['google_editorial'])}")
    print(f"Ski-in/Ski-out:      {sum(1 for h in hotels if h['ski_in_out'] == 'ja')}")
    print(f"Fast Ski-in/out:     {sum(1 for h in hotels if h['ski_in_out'] == 'fast')}")
    print(f"Mit Wellness:        {sum(1 for h in hotels if h.get('wellness'))}")
    print(f"Mit Sauna:           {sum(1 for h in hotels if h.get('sauna'))}")
    print(f"Mit Pool:            {sum(1 for h in hotels if h.get('pool'))}")
    print(f"Familienfreundlich:  {sum(1 for h in hotels if h.get('familie'))}")
    print(f"Adults Only:         {sum(1 for h in hotels if h.get('adults_only'))}")
    print(f"Mit AI-Beschreibung: {sum(1 for h in hotels if h.get('ai_description'))}")
    print(f"Mit Besonderheiten:  {sum(1 for h in hotels if h.get('besonderheiten'))}")

    # By Ortsteil
    ortsteile = {}
    for h in hotels:
        ot = h["ortsteil"] or "Unbekannt"
        ortsteile[ot] = ortsteile.get(ot, 0) + 1
    print(f"\nNach Ortsteil:")
    for ot, count in sorted(ortsteile.items(), key=lambda x: -x[1]):
        print(f"  {ot}: {count}")

    # By Preis
    preis = {}
    for h in hotels:
        pk = h.get("preis_kategorie") or "unbekannt"
        preis[pk] = preis.get(pk, 0) + 1
    print(f"\nNach Preiskategorie:")
    for pk, count in sorted(preis.items(), key=lambda x: -x[1]):
        print(f"  {pk}: {count}")

    # By Verpflegung
    verpfl = {}
    for h in hotels:
        v = h.get("verpflegung") or "unbekannt"
        verpfl[v] = verpfl.get(v, 0) + 1
    print(f"\nNach Verpflegung:")
    for v, count in sorted(verpfl.items(), key=lambda x: -x[1]):
        print(f"  {v}: {count}")

    return hotels


# =====================================================================
# MAIN
# =====================================================================

def main():
    start_time = time.time()

    print()
    print("*" * 60)
    print("  AlpenStay — Automated Data Pipeline v3")
    print("  6-Step Pipeline: FIND > VALIDATE > SKI > ENRICH > DESCRIBE > SAVE")
    print("*" * 60)
    print()

    # Step 3 (partial): Load OSM data first (needed for validation)
    print("Vorab: OSM Daten laden (fuer Validierung benoetigt)...")
    print("-" * 40)
    pisten, loipen, lifts = fetch_osm_data()
    if not lifts:
        print("FEHLER: Keine Lifte von OSM geladen, Abbruch!")
        return
    print()

    # Step 1: Find all hotels
    all_places = step1_find()

    # Step 2: Validate
    validated = step2_validate(all_places, lifts)

    # Step 3: Ski data (distances + walking routes)
    ski_results = step3_ski_data(validated, pisten, loipen, lifts)

    # Step 4: Enrich (Google Details + Website + Claude Analysis)
    enriched = step4_enrich(ski_results)

    # Step 5: AI Descriptions
    described = step5_describe(enriched)

    # Step 6: Save
    hotels = step6_save(described)

    elapsed = time.time() - start_time
    print(f"\nPipeline abgeschlossen in {elapsed/60:.1f} Minuten")

    # Clean up intermediate file
    intermediate = PROJECT_ROOT / "data" / "kleinwalsertal_real_intermediate.json"
    if intermediate.exists():
        intermediate.unlink()
        print("Zwischendatei geloescht.")


if __name__ == "__main__":
    main()
