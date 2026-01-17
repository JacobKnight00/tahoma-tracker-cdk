#!/usr/bin/env python3
"""
Update existing manifest files to include frame_state_prob field.

This script reads existing manifest files from S3, fetches the corresponding analysis files
to extract frame state probabilities, and updates the manifests with the missing field.

Usage:
    python update_manifests_with_frame_state_prob.py --bucket BUCKET --start 2024-01-01 --end 2024-12-31 [--profile PROFILE] [--workers 4] [--dry-run]
"""

import argparse
import boto3
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Dict, List, Optional


# S3 prefixes
ANALYSIS_PREFIX = "analysis/v1"
MANIFESTS_PREFIX = "manifests"


def parse_args():
    parser = argparse.ArgumentParser(description="Update existing manifests with frame_state_prob")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers (default: 4)")
    parser.add_argument("--profile", help="AWS profile to use")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be done without writing")
    return parser.parse_args()


def get_frame_state_prob_from_analysis(content: Dict, frame_state: str) -> Optional[float]:
    """Extract frame state probability from analysis JSON."""
    probs = content.get("frame_state_probabilities", {})
    if frame_state and frame_state in probs:
        return round(probs[frame_state], 4)
    return None


def load_analysis_file(s3_client, bucket: str, year: str, month: str, day: str, time: str) -> Optional[Dict]:
    """Load a specific analysis file."""
    key = f"{ANALYSIS_PREFIX}/{year}/{month}/{day}/{time}.json"
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"  Warning: Could not read analysis {key}: {e}")
        return None


def update_daily_manifest(s3_client, bucket: str, year: str, month: str, day: str, dry_run: bool) -> bool:
    """Update a single daily manifest with frame_state_prob."""
    manifest_key = f"{MANIFESTS_PREFIX}/daily/{year}/{month}/{day}.json"
    
    try:
        # Load existing manifest
        response = s3_client.get_object(Bucket=bucket, Key=manifest_key)
        manifest = json.loads(response["Body"].read().decode("utf-8"))
        
        # Check if already has frame_state_prob
        if manifest.get("images") and len(manifest["images"]) > 0:
            first_image = manifest["images"][0]
            if "frame_state_prob" in first_image:
                print(f"  {year}/{month}/{day}: Already has frame_state_prob, skipping")
                return True
        
        updated = False
        
        # Update each image entry
        for image in manifest.get("images", []):
            if "frame_state_prob" not in image:
                frame_state = image.get("frame_state")
                if frame_state:
                    # Load corresponding analysis file
                    analysis = load_analysis_file(s3_client, bucket, year, month, day, image["time"])
                    if analysis:
                        frame_state_prob = get_frame_state_prob_from_analysis(analysis, frame_state)
                        image["frame_state_prob"] = frame_state_prob
                        updated = True
                    else:
                        image["frame_state_prob"] = None
                        updated = True
                else:
                    image["frame_state_prob"] = None
                    updated = True
        
        if updated:
            if dry_run:
                print(f"  [DRY RUN] Would update {manifest_key}")
            else:
                # Write updated manifest back to S3
                s3_client.put_object(
                    Bucket=bucket,
                    Key=manifest_key,
                    Body=json.dumps(manifest, indent=2).encode("utf-8"),
                    ContentType="application/json",
                )
                print(f"  Updated {manifest_key}")
        else:
            print(f"  {year}/{month}/{day}: No updates needed")
            
        return True
        
    except Exception as e:
        print(f"  Error updating {manifest_key}: {e}")
        return False


def process_day(s3_client, bucket: str, year: str, month: str, day: str, dry_run: bool) -> bool:
    """Process a single day's manifest."""
    return update_daily_manifest(s3_client, bucket, year, month, day, dry_run)


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

    print(f"Updating manifests with frame_state_prob from {args.start} to {args.end}")
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
    success_count = 0
    error_count = 0

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
                success = future.result()
                if success:
                    success_count += 1
                else:
                    error_count += 1
            except Exception as e:
                print(f"Error processing {year}/{month}/{day}: {e}")
                error_count += 1

    print()
    print(f"Done! Processed {success_count} days successfully, {error_count} errors")


if __name__ == "__main__":
    main()
