# Weather & Stats for Tahoma Tracker

## Overview

This document describes two features for Tahoma Tracker:

1. **Visibility Forecast** - Predict when Mt. Rainier will be visible using weather data
2. **Fun Stats** - Display interesting mountain statistics alongside images

---

## Part 1: Visibility Forecast

### Summary of Findings

After analyzing ~21,000 paired weather/visibility records spanning 2 years:

| Confidence Level | Score Range | Precision | Meaning |
|------------------|-------------|-----------|---------|
| 🟢 Very Likely | < 5 | 60% | Clear skies, good chance |
| 🟡 Likely | 5-10 | 45% | Some clouds, might see it |
| 🟠 Possible | 10-20 | 15% | Significant clouds, probably not |
| 🔴 Unlikely | > 20 | 10% | Heavy clouds, very unlikely |

**Key insight**: Weather alone cannot reliably predict visibility. Even with "perfect" weather conditions, the mountain is only visible 60% of the time.

**Best use case**: Weather prediction works well as a **negative filter** - when the score is > 20, don't bother checking the camera.

### Prediction Formula

```python
def predict_visibility(cloud_800hPa, path_max_cloud):
    """
    Args:
        cloud_800hPa: Cloud cover at 800hPa (~2km altitude) at mountain. 0-100%
        path_max_cloud: Max cloud cover along viewing path. 0-100%
    """
    score = 0.4 * cloud_800hPa + 0.1 * path_max_cloud
    
    if score < 5:
        return "🟢 Good chance"
    elif score < 10:
        return "🟡 Might see it"
    elif score < 20:
        return "🟠 Probably not"
    else:
        return "🔴 Very unlikely"
```

### Why These Variables?

| Variable | F1 Score | Notes |
|----------|----------|-------|
| `mtn_cloud_800hPa` | 58% | **Best predictor** - clouds at mid-mountain altitude |
| `path_max_cloud` | 61% | Clouds blocking the viewing path |
| `mtn_cloud_600hPa` | 50% | Summit clouds - less predictive than expected |

The 800hPa pressure level (~2km altitude) outperforms 600hPa (~4.2km summit) because visibility issues come from clouds on the mountain's flanks, not at the peak.

### Forecast Data Source

**Open-Meteo Forecast API** (free, no auth required)

```bash
# Mountain - 7 day forecast of 800hPa cloud cover
curl "https://api.open-meteo.com/v1/forecast?latitude=46.85&longitude=-121.75&hourly=cloud_cover_800hPa&timezone=America/Los_Angeles&forecast_days=7"

# Path points - surface cloud cover (batch all 4 locations)
curl "https://api.open-meteo.com/v1/forecast?latitude=47.62,47.43,47.24,47.05&longitude=-122.35,-122.20,-122.05,-121.90&hourly=cloud_cover&timezone=America/Los_Angeles&forecast_days=7"
```

### Coordinates

| Location | Lat | Lon | Purpose |
|----------|-----|-----|---------|
| Camera (Space Needle) | 47.6204 | -122.3491 | Path cloud |
| Path 25% | 47.43 | -122.20 | Path cloud |
| Path 50% | 47.24 | -122.05 | Path cloud |
| Path 75% | 47.05 | -121.90 | Path cloud |
| Mountain (Point Success) | 46.85 | -121.75 | 800hPa cloud |

---

## Part 2: Fun Stats

Stats displayed alongside each image for user interest.

### Selected Stats

| Stat | Source | Example Display |
|------|--------|-----------------|
| **Paradise Snow Depth** | SNOTEL sensor | "3.7 ft of snow at Paradise" |
| **Days Visible (30 day)** | Analysis data | "Visible 12 of last 30 days" |
| **Visibility Streak** | Analysis data | "5 day streak! 🔥" |

### Why These Stats?

- **Paradise Snow Depth** ⭐⭐⭐ - Real sensor data, interesting to hikers/skiers, changes seasonally
- **Days Visible** ⭐⭐⭐ - Unique to this site, gives context on rarity
- **Visibility Streak** ⭐⭐⭐ - Gamification element, fun to track

### Stats NOT Included

| Stat | Reason |
|------|--------|
| Summit temperature | Model estimate, not real sensor - less trustworthy |
| Camp Muir conditions | No sensor there |
| Freezing level | Less interesting to general audience |

### SNOTEL Data Source

**NRCS SNOTEL Station 679 (Paradise)**

Real sensor at 5,120 ft elevation. Data available since 1981.

```bash
# Current snow depth
curl "https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customSingleStationReport/daily/679:WA:SNTL|id=%22%22|name/0,0/SNWD::value"

# Historical (last 30 days)
curl "https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customSingleStationReport/daily/679:WA:SNTL|id=%22%22|name/-30,0/SNWD::value"

# Full backfill (since 2024-01-01)
curl "https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customSingleStationReport/daily/679:WA:SNTL|id=%22%22|name/2024-01-01,2026-01-16/SNWD::value"
```

Response format (CSV):
```
Date,Snow Depth (in) Start of Day Values
2026-01-16,45
2026-01-15,45
...
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│ SCRAPER LAMBDA (every 20 min)                                       │
│                                                                     │
│ 1. Capture image from webcam                                        │
│ 2. Run ML visibility classification                                 │
│ 3. Fetch SNOTEL snow depth (cache 1 hour)                          │
│ 4. Calculate visibility streak from recent files                    │
│ 5. Save to analysis/v2/YYYY/MM/DD/HHMM.json                        │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ FORECAST LAMBDA (hourly, separate)                                  │
│                                                                     │
│ 1. Fetch 7-day forecast from Open-Meteo (2 API calls)              │
│ 2. Calculate hourly visibility scores                               │
│ 3. Save to forecast/current.json (overwrite each hour)             │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ WEBSITE                                                             │
│                                                                     │
│ - Current image + ML classification                                 │
│ - Fun stats (snow depth, streak, days visible)                     │
│ - "Will I see Rainier?" forecast widget                            │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Data Schemas

### Analysis File (updated)

`analysis/v2/YYYY/MM/DD/HHMM.json`

```json
{
  "imageId": "2025-01-16T12-00-00",
  "frame_state_probabilities": { "good": 0.95, "bad": 0.03, "dark": 0.02 },
  "visibility_probabilities": { "out": 0.85, "partially_out": 0.10, "not_out": 0.05 },
  
  "stats": {
    "paradise_snow_depth_in": 45,
    "days_visible_30d": 12,
    "current_streak": 5
  }
}
```

### Forecast File

`forecast/current.json`

```json
{
  "generated_at": "2025-01-16T12:00:00-08:00",
  "hourly": [
    { "time": "2025-01-16T13:00", "score": 3.2, "prediction": "very_likely" },
    { "time": "2025-01-16T14:00", "score": 8.1, "prediction": "likely" },
    { "time": "2025-01-16T15:00", "score": 15.4, "prediction": "possible" }
  ],
  "daily_summary": [
    { "date": "2025-01-16", "best_score": 3.2, "best_hour": 13, "prediction": "very_likely" },
    { "date": "2025-01-17", "best_score": 12.0, "best_hour": 10, "prediction": "possible" }
  ]
}
```

---

## Implementation TODO

### Phase 1: Fun Stats (scraper update)
- [ ] Add SNOTEL fetch to scraper Lambda (with 1-hour cache)
- [ ] Calculate visibility streak from recent analysis files
- [ ] Calculate 30-day visibility count
- [ ] Update analysis file schema with `stats` field
- [ ] Backfill SNOTEL data for historical images

### Phase 2: Visibility Forecast (new Lambda)
- [ ] Create forecast collector Lambda
- [ ] Fetch Open-Meteo 7-day forecast
- [ ] Calculate hourly scores and predictions
- [ ] Save to `forecast/current.json`
- [ ] Set up hourly CloudWatch trigger

### Phase 3: Website Updates
- [ ] Display fun stats alongside images
- [ ] Add "Will I see Rainier?" forecast widget
- [ ] Show best viewing times for next 7 days

### Cleanup
- [ ] Delete `weather/v1/`, `weather/v2/`, `weather/v3/` from S3 (no longer needed)
- [ ] Archive correlation analysis scripts (analysis complete)

---

## Scripts Reference

| Script | Purpose | Status |
|--------|---------|--------|
| `scripts/backfill_weather.py` | Backfill v1 weather | Archive |
| `scripts/backfill_weather_v2.py` | Backfill v2 weather | Archive |
| `scripts/backfill_weather_v3.py` | Backfill v3 weather | Archive |
| `scripts/analyze_weather_correlation.py` | Correlation analysis | Archive |
| `scripts/analyze_weather_correlation_v2.py` | Correlation analysis | Archive |
| `scripts/analyze_weather_correlation_v3.py` | Correlation analysis | Archive |
| `scripts/analyze_confidence_tiers.py` | Confidence tiers | Archive |
| `scripts/backfill_snotel.py` | Backfill SNOTEL data | TODO |

---

## Limitations

1. **60% precision ceiling** - Even perfect weather only predicts visibility 60% of the time
2. **Haze not captured** - Weather models don't measure atmospheric haze
3. **Lighting effects** - Dawn/dusk can affect visibility classification
4. **Model resolution** - Weather models use ~10km grid, can't see local fog

---

## Conclusion

- **Forecast** is useful as a negative filter and for planning ("best time to check tomorrow")
- **Fun stats** add engagement and context for users
- **Camera remains ground truth** for current visibility
