#!/usr/bin/env python3
"""Generate historical weather data for destination info pages."""
import requests, json, time

KLEINWALSERTAL = {"lat": 47.34, "lng": 10.17, "name": "Kleinwalsertal"}
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

def get_winter_history(lat, lng, years=6):
    """Get snow/weather data for the last 6 winter seasons (Dec-Mar)."""
    seasons = []
    current_year = 2025  # Adjust as needed

    for year in range(current_year - years, current_year + 1):
        params = {
            "latitude": lat,
            "longitude": lng,
            "start_date": f"{year}-12-01",
            "end_date": f"{year+1}-03-31",
            "daily": "snowfall_sum,temperature_2m_min,temperature_2m_max,sunshine_duration,precipitation_sum",
            "timezone": "Europe/Berlin"
        }
        resp = requests.get(ARCHIVE_URL, params=params, timeout=30)
        if resp.status_code != 200:
            continue
        data = resp.json()
        daily = data.get("daily", {})
        dates = daily.get("time", [])
        snow = daily.get("snowfall_sum", [])
        tmin = daily.get("temperature_2m_min", [])
        tmax = daily.get("temperature_2m_max", [])
        sun = daily.get("sunshine_duration", [])
        precip = daily.get("precipitation_sum", [])

        # Per month stats
        months = {}
        for m_name, m_prefix in [("dez", f"{year}-12"), ("jan", f"{year+1}-01"), ("feb", f"{year+1}-02"), ("mar", f"{year+1}-03")]:
            m_snow = sum(s for i, s in enumerate(snow) if s and dates[i].startswith(m_prefix))
            m_days = sum(1 for i, s in enumerate(snow) if s and s > 0 and dates[i].startswith(m_prefix))
            m_tmin_vals = [t for i, t in enumerate(tmin) if t is not None and dates[i].startswith(m_prefix)]
            m_tmax_vals = [t for i, t in enumerate(tmax) if t is not None and dates[i].startswith(m_prefix)]
            m_sun_vals = [s/3600 for i, s in enumerate(sun) if s is not None and dates[i].startswith(m_prefix)]

            months[m_name] = {
                "schneefall_cm": round(m_snow),
                "schneetage": m_days,
                "temp_min_avg": round(sum(m_tmin_vals)/len(m_tmin_vals), 1) if m_tmin_vals else None,
                "temp_max_avg": round(sum(m_tmax_vals)/len(m_tmax_vals), 1) if m_tmax_vals else None,
                "sonnenstunden": round(sum(m_sun_vals)) if m_sun_vals else None,
            }

        seasons.append({
            "saison": f"{year}/{year+1}",
            "monate": months,
            "gesamt_schnee_cm": round(sum(s for s in snow if s)),
        })
        time.sleep(0.5)

    return seasons

def get_monthly_averages(lat, lng):
    """Get average monthly stats across all years for the date picker feature."""
    # Get daily data for last 6 years
    all_daily = {}  # key: "MM-DD" -> list of values

    for year in range(2019, 2026):
        params = {
            "latitude": lat,
            "longitude": lng,
            "start_date": f"{year}-01-01",
            "end_date": f"{year}-12-31",
            "daily": "snowfall_sum,temperature_2m_min,temperature_2m_max,sunshine_duration",
            "timezone": "Europe/Berlin"
        }
        resp = requests.get(ARCHIVE_URL, params=params, timeout=30)
        if resp.status_code != 200:
            continue
        data = resp.json()
        daily = data.get("daily", {})
        dates = daily.get("time", [])
        snow = daily.get("snowfall_sum", [])
        tmin = daily.get("temperature_2m_min", [])
        tmax = daily.get("temperature_2m_max", [])
        sun = daily.get("sunshine_duration", [])

        for i, d in enumerate(dates):
            mmdd = d[5:]  # "MM-DD"
            if mmdd not in all_daily:
                all_daily[mmdd] = {"snow": [], "tmin": [], "tmax": [], "sun": []}
            if snow[i] is not None: all_daily[mmdd]["snow"].append(snow[i])
            if tmin[i] is not None: all_daily[mmdd]["tmin"].append(tmin[i])
            if tmax[i] is not None: all_daily[mmdd]["tmax"].append(tmax[i])
            if sun[i] is not None: all_daily[mmdd]["sun"].append(sun[i] / 3600)
        time.sleep(0.3)

    # Build averages per day
    daily_avgs = {}
    for mmdd, vals in sorted(all_daily.items()):
        daily_avgs[mmdd] = {
            "snow_avg": round(sum(vals["snow"])/len(vals["snow"]), 1) if vals["snow"] else 0,
            "snow_chance": round(sum(1 for s in vals["snow"] if s > 0) / len(vals["snow"]) * 100) if vals["snow"] else 0,
            "tmin_avg": round(sum(vals["tmin"])/len(vals["tmin"]), 1) if vals["tmin"] else None,
            "tmax_avg": round(sum(vals["tmax"])/len(vals["tmax"]), 1) if vals["tmax"] else None,
            "sun_avg": round(sum(vals["sun"])/len(vals["sun"]), 1) if vals["sun"] else 0,
        }

    return daily_avgs

if __name__ == "__main__":
    print("Generating destination info data...")

    lat, lng = KLEINWALSERTAL["lat"], KLEINWALSERTAL["lng"]

    print("Loading winter history...")
    winter = get_winter_history(lat, lng)

    print("Loading daily averages...")
    daily_avgs = get_monthly_averages(lat, lng)

    result = {
        "destination": "Kleinwalsertal",
        "latitude": lat,
        "longitude": lng,
        "elevation_m": 1100,
        "winter_seasons": winter,
        "daily_averages": daily_avgs,
    }

    import pathlib
    out = pathlib.Path("data/kleinwalsertal_info.json")
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved to {out} ({len(winter)} seasons, {len(daily_avgs)} daily averages)")
