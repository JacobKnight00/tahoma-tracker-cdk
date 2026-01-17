#!/usr/bin/env python3
"""
Backfill weather data v2 - with pressure level data at summit altitude.

Usage:
    python backfill_weather_v2.py --bucket BUCKET --start 2024-01-01 --end 2024-12-31 [--profile PROFILE]
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

# Coordinates
CAMERA_LAT = 47.6204   # Space Needle
CAMERA_LON = -122.3491
MIDPOINT_LAT = 47.24   # Between camera and mountain
MIDPOINT_LON = -122.05
MOUNTAIN_LAT = 46.85   # Point Success (46°51'00.0"N 121°45'00.0"W)
MOUNTAIN_LON = -121.75

ANALYSIS_PREFIX = "analysis"
WEATHER_PREFIX = "weather/v2"

FORECAST_API = "https://historical-forecast-api.open-meteo.com/v1/forecast"

SSL_CTX = ssl.create_default_context(cafile=certifi.where())


def parse_args():
    parser = argparse.ArgumentParser(description="Backfill weather data v2")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--version", type=int, default=2, help="Analysis version (default: 2)")
    parser.add_argument("--profile", help="AWS profile to use")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be done")
    parser.add_argument("--skip-existing", action="store_true", help="Skip existing weather files")
    parser.add_argument("--workers", type=int, default=20, help="S3 write workers (default: 20)")
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
    """Fetch weather for a date range using pressure level data."""
    
    # Mountain: pressure level data at 600hPa (~4.2km, near summit)
    mtn_params = urllib.parse.urlencode({
        "latitude": MOUNTAIN_LAT, "longitude": MOUNTAIN_LON,
        "start_date": start_date, "end_date": end_date,
        "hourly": ",".join([
            "cloud_cover_600hPa",
            "relative_humidity_600hPa",
            "cloud_cover_low",
            "wind_speed_10m",
            "freezing_level_height",
        ]),
        "timezone": "America/Los_Angeles",
        "models": "gfs_hrrr",  # Use high-res HRRR model for US
    })
    
    # Camera: visibility
    cam_params = urllib.parse.urlencode({
        "latitude": CAMERA_LAT, "longitude": CAMERA_LON,
        "start_date": start_date, "end_date": end_date,
        "hourly": "visibility",
        "timezone": "America/Los_Angeles",
    })
    
    # Midpoint: visibility (path between camera and mountain)
    mid_params = urllib.parse.urlencode({
        "latitude": MIDPOINT_LAT, "longitude": MIDPOINT_LON,
        "start_date": start_date, "end_date": end_date,
        "hourly": "visibility",
        "timezone": "America/Los_Angeles",
    })
    
    mtn_data = fetch_url(f"{FORECAST_API}?{mtn_params}")
    time.sleep(0.3)
    cam_data = fetch_url(f"{FORECAST_API}?{cam_params}")
    time.sleep(0.3)
    mid_data = fetch_url(f"{FORECAST_API}?{mid_params}")
    
    if not all([mtn_data, cam_data, mid_data]):
        return None
    return {"mountain": mtn_data, "camera": cam_data, "midpoint": mid_data}


def extract_hourly(weather_data: Dict, hour_index: int) -> Optional[Dict]:
    mtn = weather_data["mountain"].get("hourly", {})
    cam = weather_data["camera"].get("hourly", {})
    mid = weather_data["midpoint"].get("hourly", {})
    
    def get(data, key):
        arr = data.get(key, [])
        return arr[hour_index] if hour_index < len(arr) else None
    
    return {
        "camera": {
            "visibility": get(cam, "visibility"),
        },
        "midpoint": {
            "visibility": get(mid, "visibility"),
        },
        "mountain": {
            "cloud_cover_summit": get(mtn, "cloud_cover_600hPa"),  # At ~4.2km altitude
            "humidity_summit": get(mtn, "relative_humidity_600hPa"),
            "cloud_cover_low": get(mtn, "cloud_cover_low"),
            "wind_speed": get(mtn, "wind_speed_10m"),
            "freezing_level": get(mtn, "freezing_level_height"),
        },
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
    print("Scanning existing weather v2 files...")
    existing = set()
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{WEATHER_PREFIX}/"):
        for obj in page.get("Contents", []):
            existing.add(obj["Key"])
    print(f"  Found {len(existing)} existing weather files")
    return existing


def write_to_s3(s3_client, bucket: str, key: str, record: Dict):
    s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps(record, indent=2).encode("utf-8"), ContentType="application/json")


def main():
    args = parse_args()
    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d")
    
    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    s3_client = session.client("s3")
    
    print(f"Backfilling weather v2: {args.start} to {args.end}")
    if args.dry_run:
        print("DRY RUN mode")
    
    analysis_by_date = list_all_analysis_times(s3_client, args.bucket, args.version, start_date, end_date)
    
    existing_keys = set()
    if args.skip_existing:
        existing_keys = list_existing_weather(s3_client, args.bucket)
    
    all_dates = sorted(analysis_by_date.keys())
    batches = [all_dates[i:i+30] for i in range(0, len(all_dates), 30)]
    
    print(f"Processing {len(batches)} batches (sequential API, parallel S3 with {args.workers} workers)...")
    
    total_processed, total_skipped = 0, 0
    
    for i, batch in enumerate(batches):
        start_d, end_d = batch[0], batch[-1]
        print(f"Batch {i+1}/{len(batches)}: {start_d} to {end_d} - fetching weather...")
        
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
                hourly = extract_hourly(weather_data, base_idx + hour)
                if not hourly:
                    total_skipped += 1
                    continue
                
                minute = int(time_str[2:])
                image_id = f"{date_str}T{hour:02d}-{minute:02d}"
                record = {"imageId": image_id, "timestamp": f"{date_str}T{hour:02d}:{minute:02d}:00-08:00", **hourly}
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
