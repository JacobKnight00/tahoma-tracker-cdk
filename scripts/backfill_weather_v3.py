#!/usr/bin/env python3
"""
Backfill weather data v3 - multi-level clouds and path sampling.

New features:
- Multi-level cloud cover (850, 800, 700, 600 hPa)
- Path sampling (4 points along viewing corridor)
- Derived features (dew point depression, pressure tendency)

Usage:
    python backfill_weather_v3.py --bucket BUCKET --start 2024-01-01 --end 2024-12-31 [--profile PROFILE]
"""

import argparse
import boto3
import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Dict, List, Optional
import urllib.request
import urllib.parse
import ssl
import certifi

# Viewing corridor coordinates
CAMERA_LAT, CAMERA_LON = 47.6204, -122.3491      # Space Needle
PATH_25_LAT, PATH_25_LON = 47.43, -122.20        # 25% along path
PATH_50_LAT, PATH_50_LON = 47.24, -122.05        # Midpoint
PATH_75_LAT, PATH_75_LON = 47.05, -121.90        # 75% along path
MOUNTAIN_LAT, MOUNTAIN_LON = 46.85, -121.75      # Point Success

ANALYSIS_PREFIX = "analysis"
WEATHER_PREFIX = "weather/v3"
FORECAST_API = "https://historical-forecast-api.open-meteo.com/v1/forecast"

SSL_CTX = ssl.create_default_context(cafile=certifi.where())


def parse_args():
    parser = argparse.ArgumentParser(description="Backfill weather data v3")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--version", type=int, default=2, help="Analysis version (default: 2)")
    parser.add_argument("--profile", help="AWS profile")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--workers", type=int, default=20, help="S3 write workers")
    return parser.parse_args()


def fetch_url(url: str, retries: int = 3) -> Optional[Dict]:
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=30, context=SSL_CTX) as response:
                return json.loads(response.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 2 ** attempt * 5
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
            else:
                return None
        except:
            return None
    return None


def fetch_weather_batch(start_date: str, end_date: str) -> Optional[Dict]:
    """Fetch weather for date range - 5 API calls with rate limiting."""
    results = {}
    
    # Path points: visibility and cloud cover (4 calls)
    path_points = [
        ("camera", CAMERA_LAT, CAMERA_LON),
        ("path_25", PATH_25_LAT, PATH_25_LON),
        ("path_50", PATH_50_LAT, PATH_50_LON),
        ("path_75", PATH_75_LAT, PATH_75_LON),
    ]
    for name, lat, lon in path_points:
        params = urllib.parse.urlencode({
            "latitude": lat, "longitude": lon,
            "start_date": start_date, "end_date": end_date,
            "hourly": "visibility,cloud_cover,cloud_cover_low,cloud_cover_mid",
            "timezone": "America/Los_Angeles",
        })
        data = fetch_url(f"{FORECAST_API}?{params}")
        if not data:
            return None
        results[name] = data
        time.sleep(0.3)
    
    # Mountain: multi-level pressure data (1 call)
    mtn_params = urllib.parse.urlencode({
        "latitude": MOUNTAIN_LAT, "longitude": MOUNTAIN_LON,
        "start_date": start_date, "end_date": end_date,
        "hourly": ",".join([
            "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high",
            "temperature_2m", "dew_point_2m", "pressure_msl", "wind_speed_10m",
            "cloud_cover_850hPa", "cloud_cover_800hPa", "cloud_cover_700hPa", "cloud_cover_600hPa",
            "relative_humidity_850hPa", "relative_humidity_700hPa", "relative_humidity_600hPa",
            "temperature_850hPa", "temperature_700hPa",
        ]),
        "timezone": "America/Los_Angeles",
    })
    data = fetch_url(f"{FORECAST_API}?{mtn_params}")
    if not data:
        return None
    results["mountain"] = data
    
    return results


def extract_hourly(weather_data: Dict, hour_index: int, prev_index: int) -> Optional[Dict]:
    """Extract hourly record with derived features."""
    def get(loc, key):
        arr = weather_data.get(loc, {}).get("hourly", {}).get(key, [])
        return arr[hour_index] if hour_index < len(arr) else None
    
    def get_prev(loc, key):
        if prev_index < 0:
            return None
        arr = weather_data.get(loc, {}).get("hourly", {}).get(key, [])
        return arr[prev_index] if prev_index < len(arr) else None
    
    # Path aggregates
    path_vis = [get(loc, "visibility") for loc in ["camera", "path_25", "path_50", "path_75"]]
    path_vis = [v for v in path_vis if v is not None]
    path_cloud = [get(loc, "cloud_cover") for loc in ["camera", "path_25", "path_50", "path_75"]]
    path_cloud = [c for c in path_cloud if c is not None]
    
    # Derived features
    temp = get("mountain", "temperature_2m")
    dew = get("mountain", "dew_point_2m")
    dew_depression = round(temp - dew, 1) if temp is not None and dew is not None else None
    
    pressure_now = get("mountain", "pressure_msl")
    pressure_prev = get_prev("mountain", "pressure_msl")
    pressure_trend = round(pressure_now - pressure_prev, 1) if pressure_now and pressure_prev else None
    
    vis_now = get("camera", "visibility")
    vis_prev = get_prev("camera", "visibility")
    vis_trend = round((vis_now - vis_prev) / 1000, 1) if vis_now and vis_prev else None
    
    return {
        "cam_visibility": get("camera", "visibility"),
        "cam_cloud": get("camera", "cloud_cover"),
        "path_min_visibility": min(path_vis) if path_vis else None,
        "path_max_cloud": max(path_cloud) if path_cloud else None,
        "mtn_cloud": get("mountain", "cloud_cover"),
        "mtn_cloud_low": get("mountain", "cloud_cover_low"),
        "mtn_cloud_mid": get("mountain", "cloud_cover_mid"),
        "mtn_cloud_high": get("mountain", "cloud_cover_high"),
        "mtn_cloud_850hPa": get("mountain", "cloud_cover_850hPa"),
        "mtn_cloud_800hPa": get("mountain", "cloud_cover_800hPa"),
        "mtn_cloud_700hPa": get("mountain", "cloud_cover_700hPa"),
        "mtn_cloud_600hPa": get("mountain", "cloud_cover_600hPa"),
        "mtn_humidity_700hPa": get("mountain", "relative_humidity_700hPa"),
        "mtn_humidity_600hPa": get("mountain", "relative_humidity_600hPa"),
        "dew_depression": dew_depression,
        "pressure_trend": pressure_trend,
        "vis_trend": vis_trend,
        "mtn_wind": get("mountain", "wind_speed_10m"),
    }


def list_all_analysis_times(s3_client, bucket: str, version: int, start: datetime, end: datetime) -> Dict[str, List[str]]:
    print("Scanning S3 for analysis files...")
    by_date = {}
    prefix = f"{ANALYSIS_PREFIX}/v{version}/"
    
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            parts = key.replace(prefix, "").replace(".json", "").split("/")
            if len(parts) == 4:
                year, month, day, time_str = parts
                date_str = f"{year}-{month}-{day}"
                dt = datetime.strptime(date_str, "%Y-%m-%d")
                if start <= dt <= end:
                    by_date.setdefault(date_str, []).append(time_str)
    
    print(f"  Found {sum(len(v) for v in by_date.values())} images across {len(by_date)} days")
    return by_date


def list_existing_weather(s3_client, bucket: str) -> set:
    print("Scanning existing weather v3 files...")
    existing = set()
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{WEATHER_PREFIX}/"):
        for obj in page.get("Contents", []):
            existing.add(obj["Key"])
    print(f"  Found {len(existing)} existing weather files")
    return existing


def write_to_s3(s3_client, bucket: str, key: str, record: Dict):
    s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps(record).encode("utf-8"), ContentType="application/json")


def main():
    args = parse_args()
    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d")
    
    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    s3_client = session.client("s3")
    
    print(f"Backfilling weather v3: {args.start} to {args.end}")
    if args.dry_run:
        print("DRY RUN mode")
    
    analysis_by_date = list_all_analysis_times(s3_client, args.bucket, args.version, start_date, end_date)
    
    existing_keys = set()
    if args.skip_existing:
        existing_keys = list_existing_weather(s3_client, args.bucket)
    
    all_dates = sorted(analysis_by_date.keys())
    batches = [all_dates[i:i+30] for i in range(0, len(all_dates), 30)]
    
    print(f"Processing {len(batches)} batches (5 API calls/batch, {args.workers} S3 workers)...")
    
    total_processed, total_skipped = 0, 0
    
    for i, batch in enumerate(batches):
        start_d, end_d = batch[0], batch[-1]
        print(f"Batch {i+1}/{len(batches)}: {start_d} to {end_d}...")
        
        weather_data = fetch_weather_batch(start_d, end_d)
        if not weather_data:
            skipped = sum(len(analysis_by_date.get(d, [])) for d in batch)
            total_skipped += skipped
            print(f"  Failed to fetch, skipped {skipped}")
            continue
        
        date_to_base = {d: idx * 24 for idx, d in enumerate(batch)}
        records_to_write = []
        
        for date_str in batch:
            times = analysis_by_date.get(date_str, [])
            base_idx = date_to_base[date_str]
            year, month, day = date_str.split("-")
            
            for time_str in times:
                key = f"{WEATHER_PREFIX}/{year}/{month}/{day}/{time_str}.json"
                
                if args.skip_existing and key in existing_keys:
                    total_skipped += 1
                    continue
                
                hour = int(time_str[:2])
                hour_idx = base_idx + hour
                hourly = extract_hourly(weather_data, hour_idx, hour_idx - 1)
                if not hourly:
                    total_skipped += 1
                    continue
                
                minute = int(time_str[2:])
                image_id = f"{date_str}T{hour:02d}-{minute:02d}"
                record = {"imageId": image_id, "timestamp": f"{date_str}T{hour:02d}:{minute:02d}:00-08:00", "hour": hour, **hourly}
                records_to_write.append((key, record))
        
        if args.dry_run:
            total_processed += len(records_to_write)
            print(f"  Would write {len(records_to_write)} files")
        else:
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = [executor.submit(write_to_s3, s3_client, args.bucket, k, r) for k, r in records_to_write]
                for f in futures:
                    f.result()
            total_processed += len(records_to_write)
            print(f"  Wrote {len(records_to_write)} files")
    
    print(f"\nDone! Processed: {total_processed}, Skipped: {total_skipped}")


if __name__ == "__main__":
    main()
