#!/usr/bin/env python3
"""
Backfill script for generating daily and monthly manifests from existing analysis files.

This script reads existing analysis JSON files from S3 and generates the manifest files
that the frontend uses for efficient batch loading.

Usage:
    python backfill_manifests.py --bucket BUCKET --start 2024-01-01 --end 2024-12-31 [--profile PROFILE] [--workers 4] [--dry-run]
"""

import argparse
import boto3
import json
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any


# S3 prefixes (should match CDK/config values)
ANALYSIS_PREFIX = "analysis/v1"
MANIFESTS_PREFIX = "manifests"


def parse_args():
    parser = argparse.ArgumentParser(description="Backfill manifest files from existing analysis JSONs")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers (default: 4)")
    parser.add_argument("--profile", help="AWS profile to use")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be done without writing")
    return parser.parse_args()


def list_analysis_files_for_date(s3_client, bucket: str, year: str, month: str, day: str) -> List[Dict[str, Any]]:
    """List and read all analysis files for a specific date."""
    prefix = f"{ANALYSIS_PREFIX}/{year}/{month}/{day}/"
    analyses = []

    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".json"):
                    continue

                # Extract time from key like "analysis/v1/2024/12/25/1200.json"
                time_part = key.split("/")[-1].replace(".json", "")
                if len(time_part) != 4:
                    continue

                try:
                    response = s3_client.get_object(Bucket=bucket, Key=key)
                    content = json.loads(response["Body"].read().decode("utf-8"))

                    # Extract relevant fields
                    analysis = {
                        "time": time_part,
                        "frame_state": get_frame_state_from_analysis(content),
                        "frame_state_prob": get_frame_state_prob_from_analysis(content),
                        "visibility": get_visibility_from_analysis(content),
                        "visibility_prob": get_visibility_prob_from_analysis(content),
                    }
                    analyses.append(analysis)
                except Exception as e:
                    print(f"  Warning: Could not read {key}: {e}")
    except Exception as e:
        print(f"  Error listing {prefix}: {e}")

    return sorted(analyses, key=lambda x: x["time"])


def get_frame_state_from_analysis(content: Dict) -> Optional[str]:
    """Extract frame_state from analysis JSON."""
    probs = content.get("frame_state_probabilities", {})
    if not probs:
        return None
    # Return the class with highest probability
    return max(probs, key=probs.get) if probs else None


def get_visibility_from_analysis(content: Dict) -> Optional[str]:
    """Extract visibility from analysis JSON."""
    probs = content.get("visibility_probabilities", {})
    if not probs:
        return None
    # Return the class with highest probability
    return max(probs, key=probs.get) if probs else None


def get_visibility_prob_from_analysis(content: Dict) -> Optional[float]:
    """Extract visibility probability from analysis JSON."""
    probs = content.get("visibility_probabilities", {})
    visibility = get_visibility_from_analysis(content)
    if visibility and visibility in probs:
        return round(probs[visibility], 4)
    return None


def get_frame_state_prob_from_analysis(content: Dict) -> Optional[float]:
    """Extract frame state probability from analysis JSON."""
    probs = content.get("frame_state_probabilities", {})
    frame_state = get_frame_state_from_analysis(content)
    if frame_state and frame_state in probs:
        return round(probs[frame_state], 4)
    return None


def build_daily_manifest(date_str: str, analyses: List[Dict]) -> Dict:
    """Build a daily manifest from a list of analysis entries."""
    images = []
    out_count = 0
    partially_out_count = 0
    now = datetime.utcnow().isoformat() + "Z"

    for analysis in analyses:
        entry = {
            "time": analysis["time"],
            "frame_state": analysis["frame_state"],
            "frame_state_prob": analysis["frame_state_prob"],
            "visibility": analysis["visibility"],
            "visibility_prob": analysis["visibility_prob"],
        }
        images.append(entry)

        if analysis["visibility"] == "out":
            out_count += 1
        elif analysis["visibility"] == "partially_out":
            partially_out_count += 1

    return {
        "date": date_str,
        "images": images,
        "summary": {
            "total": len(images),
            "out_count": out_count,
            "partially_out_count": partially_out_count,
            "had_out": out_count > 0,
            "had_partially_out": partially_out_count > 0,
        },
        "generated_at": now,
        "last_checked_at": now,
    }


def build_monthly_manifest(month_str: str, daily_manifests: Dict[str, Dict]) -> Dict:
    """Build a monthly manifest from daily manifests."""
    days = {}
    days_with_out = 0
    days_with_partially_out = 0
    total_out_images = 0
    total_partially_out_images = 0
    total_images = 0
    now = datetime.utcnow().isoformat() + "Z"

    for day, daily in daily_manifests.items():
        summary = daily["summary"]
        days[day] = {
            "had_out": summary["had_out"],
            "had_partially_out": summary["had_partially_out"],
            "image_count": summary["total"],
            "out_count": summary["out_count"],
            "partially_out_count": summary["partially_out_count"],
        }

        if summary["had_out"]:
            days_with_out += 1
        if summary["had_partially_out"]:
            days_with_partially_out += 1
        total_out_images += summary["out_count"]
        total_partially_out_images += summary["partially_out_count"]
        total_images += summary["total"]

    return {
        "month": month_str,
        "days": days,
        "stats": {
            "days_with_out": days_with_out,
            "days_with_partially_out": days_with_partially_out,
            "total_out_images": total_out_images,
            "total_partially_out_images": total_partially_out_images,
            "total_images": total_images,
        },
        "generated_at": now,
        "last_checked_at": now,
    }


def write_manifest(s3_client, bucket: str, key: str, manifest: Dict, dry_run: bool):
    """Write a manifest to S3."""
    if dry_run:
        print(f"  [DRY RUN] Would write {key}")
        return

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(manifest, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"  Wrote {key}")


def process_day(s3_client, bucket: str, year: str, month: str, day: str, dry_run: bool) -> Optional[Dict]:
    """Process a single day and return the daily manifest."""
    date_str = f"{year}-{month}-{day}"

    analyses = list_analysis_files_for_date(s3_client, bucket, year, month, day)

    if not analyses:
        return None

    daily_manifest = build_daily_manifest(date_str, analyses)

    # Write daily manifest to archived location
    daily_key = f"{MANIFESTS_PREFIX}/daily/{year}/{month}/{day}.json"
    write_manifest(s3_client, bucket, daily_key, daily_manifest, dry_run)

    return daily_manifest


def main():
    args = parse_args()

    try:
        start_date = datetime.strptime(args.start, "%Y-%m-%d")
        end_date = datetime.strptime(args.end, "%Y-%m-%d")
    except ValueError as e:
        print(f"Error parsing dates: {e}")
        sys.exit(1)

    if start_date > end_date:
        print("Error: start date must be before or equal to end date")
        sys.exit(1)

    print(f"Backfilling manifests from {args.start} to {args.end}")
    print(f"Bucket: {args.bucket}")
    if args.profile:
        print(f"Profile: {args.profile}")
    print(f"Workers: {args.workers}")
    if args.dry_run:
        print("DRY RUN MODE - no files will be written")
    print()

    # Create S3 client with optional profile
    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    s3_client = session.client("s3")

    # Collect all dates to process
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)

    print(f"Processing {len(dates)} days...")

    # Process days in parallel
    daily_manifests_by_month = defaultdict(dict)  # month_str -> day -> manifest

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {}
        for date in dates:
            year = date.strftime("%Y")
            month = date.strftime("%m")
            day = date.strftime("%d")
            future = executor.submit(process_day, s3_client, args.bucket, year, month, day, args.dry_run)
            futures[future] = (year, month, day)

        for future in as_completed(futures):
            year, month, day = futures[future]
            try:
                daily_manifest = future.result()
                if daily_manifest:
                    month_str = f"{year}-{month}"
                    daily_manifests_by_month[month_str][day] = daily_manifest
                    print(f"Processed {year}/{month}/{day}: {daily_manifest['summary']['total']} images")
                else:
                    print(f"Processed {year}/{month}/{day}: no images found")
            except Exception as e:
                print(f"Error processing {year}/{month}/{day}: {e}")

    # Generate monthly manifests
    print()
    print("Generating monthly manifests...")

    for month_str, daily_manifests in sorted(daily_manifests_by_month.items()):
        year, month = month_str.split("-")
        monthly_manifest = build_monthly_manifest(month_str, daily_manifests)
        monthly_key = f"{MANIFESTS_PREFIX}/monthly/{year}/{month}.json"
        write_manifest(s3_client, args.bucket, monthly_key, monthly_manifest, args.dry_run)
        print(f"Generated monthly manifest for {month_str}: {monthly_manifest['stats']['total_images']} images, "
              f"{monthly_manifest['stats']['days_with_out']} days with 'out'")

    print()
    print("Done!")


if __name__ == "__main__":
    main()
