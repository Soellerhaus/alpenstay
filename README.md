# AlpenStay - Alpen Hotel Finder

Unabhaengiger Unterkunfts-Guide fuer die Alpen mit KI-Beschreibungen und Affiliate-Integration.

## Projektstruktur

```
alpenstay/
├── index.html                        # Hauptseite (Hotel-Finder)
├── destinations/                     # Weitere Destinationen (spaeter)
├── scripts/
│   ├── fetch-google-ratings.py       # Google Places API → Ratings ins Sheet
│   ├── generate-descriptions.py      # Claude API → KI-Beschreibungen ins Sheet
│   └── requirements.txt              # Python Dependencies
├── vercel.json                       # Vercel Deployment Config
├── .env.example                      # Vorlage fuer Umgebungsvariablen
├── .gitignore
└── README.md
```

## Setup

### 1. Google Sheet

Das Sheet enthaelt alle Hotel-Daten und wird ueber die JSONP GViz API geladen (kein API Key noetig fuer Frontend).

**Sheet ID:** `1RD9N9OPMHMCa-AQNbWffqwbRIKTFZH-5H5I7l5hw6wU`
**Tab:** `hotels`

**Erforderliche Spalten:**

| Spalte | Beschreibung |
|--------|-------------|
| `name` | Hotelname |
| `ortsteil` | Ort (Riezlern, Hirschegg, Mittelberg, Baad) |
| `typ` | Hotel, Pension, Resort, etc. |
| `status` | aktiv/inaktiv |
| `website` | Hotel-Website URL |
| `booking_url` | Direkte Booking.com URL (optional) |
| `short_pitch` | Kurzbeschreibung |
| `insider_tip` | Geheimtipp |
| `tag` | Tags, kommasepariert (z.B. "familie, wellness, ski") |
| `ski_in_out` | ja/nein |
| `ski_in_out_note` | Hinweis zu Ski-Zugang |
| `nearest_lift_name` | Name des naechsten Lifts |
| `nearest_lift_type` | Lifttyp |
| `lift_distance_m` | Entfernung zum Lift in Metern |
| `piste_distance_m` | Entfernung zur Piste |
| `loipe_distance_m` | Entfernung zur Loipe |
| `loipe_name` | Name der Loipe |
| `google_place_id` | Google Place ID (wird per Script befuellt) |
| `google_rating` | Google Bewertung, z.B. 4.3 |
| `google_reviews` | Anzahl Google Bewertungen |
| `ai_description` | KI-generierte Beschreibung |
| `ai_description_date` | Datum der letzten KI-Beschreibung |

**Wichtig:** Das Sheet muss als "Fuer jeden mit Link" freigegeben sein (Betrachter), damit die JSONP-Abfrage im Frontend funktioniert.

### 2. Python Scripts einrichten

```bash
cd scripts
pip install -r requirements.txt
```

Erstelle eine `.env` Datei im Projekt-Root (siehe `.env.example`):

```bash
cp .env.example .env
# Dann API Keys eintragen
```

Fuer die Google Sheets API brauchst du einen Service Account:
1. Google Cloud Console > APIs & Services > Credentials
2. Service Account erstellen
3. JSON Key herunterladen als `credentials.json` im Projekt-Root
4. Sheet mit der Service Account E-Mail teilen (Editor)

### 3. Google Ratings abrufen

```bash
python scripts/fetch-google-ratings.py
```

- Sucht fuer jedes Hotel die Google Place ID
- Holt Rating + Review Count
- Schreibt Ergebnisse ins Sheet
- Rate Limiting: 1 Request/Sekunde

### 4. KI-Beschreibungen generieren

```bash
python scripts/generate-descriptions.py
```

- Generiert Beschreibungen fuer Hotels ohne `ai_description` oder aelter als 30 Tage
- Nutzt Claude Haiku fuer schnelle, guenstige Generierung
- Schreibt Beschreibungen + Datum ins Sheet

**Cron-Setup (VPS):**
```bash
# Woechentlich Sonntag 3:00
0 3 * * 0 cd /path/to/alpenstay && python scripts/generate-descriptions.py >> /var/log/alpenstay-ai.log 2>&1

# Woechentlich Sonntag 2:00 (Ratings zuerst)
0 2 * * 0 cd /path/to/alpenstay && python scripts/fetch-google-ratings.py >> /var/log/alpenstay-ratings.log 2>&1
```

### 5. Vercel Deploy

```bash
# Vercel CLI installieren
npm i -g vercel

# Deployen
vercel --prod
```

Oder via GitHub Integration:
1. Repo auf GitHub pushen
2. Vercel verbinden > Import Project
3. Auto-Deploy bei jedem Push auf `main`

## Affiliate Links

### Booking.com

1. Bei [Booking.com Affiliate Partner](https://www.booking.com/affiliate-program/v2/index.html) anmelden
2. Affiliate ID erhalten
3. In `index.html` die Konstante `CONFIG.AFFILIATE_ID` setzen
4. Optional: Direkte Hotel-URLs in der Spalte `booking_url` im Sheet hinterlegen

Die "Jetzt buchen" Buttons nutzen entweder:
- Die `booking_url` aus dem Sheet (wenn vorhanden) + Affiliate ID
- Oder eine Booking.com Suche nach Hotelname mit Affiliate ID

### Weitere Partner (spaeter)

- **GetYourGuide:** Aktivitaeten & Touren
- **Alpy:** Alpen-spezifische Angebote

## Neue Destination hinzufuegen

1. Neues Tab im Google Sheet erstellen (z.B. "oberstdorf")
2. Gleiche Spaltenstruktur wie "hotels" verwenden
3. In `index.html`:
   - Destination-Nav Link ergaenzen
   - `CONFIG.SHEET_TAB` dynamisch setzen (z.B. via URL Parameter)
4. Optional: Eigene Seite unter `/destinations/oberstdorf/index.html`

## Tech Stack

- **Frontend:** Vanilla HTML/CSS/JS (kein Framework, keine Build Tools)
- **Backend:** Google Sheets als Datenbank (JSONP GViz API)
- **Scripts:** Python 3.10+ (gspread, anthropic, requests)
- **Hosting:** Vercel (Static Deployment)
- **KI:** Claude Haiku fuer Beschreibungen
- **APIs:** Google Places API fuer Ratings

## Lizenz

Proprietaer - alpenstay.de
