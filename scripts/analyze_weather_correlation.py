#!/usr/bin/env python3
"""
Analyze correlation between weather data and mountain visibility.

Usage:
    python analyze_weather_correlation.py --bucket BUCKET [--profile PROFILE] [--output results.json]
"""

import argparse
import boto3
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional

CACHE_DIR = ".cache/correlation"


def parse_args():
    parser = argparse.ArgumentParser(description="Analyze weather-visibility correlation")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--version", type=int, default=2, help="Analysis version (default: 2)")
    parser.add_argument("--profile", help="AWS profile to use")
    parser.add_argument("--output", help="Output file for results (optional)")
    parser.add_argument("--limit", type=int, help="Limit number of records to process")
    parser.add_argument("--workers", type=int, default=50, help="Parallel workers (default: 50)")
    parser.add_argument("--no-cache", action="store_true", help="Ignore cache and re-download")
    return parser.parse_args()


def load_json_from_s3(s3_client, bucket: str, key: str) -> Optional[Dict]:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except:
        return None


def load_all_weather(s3_client, bucket: str, workers: int, limit: Optional[int] = None) -> Dict[str, Dict]:
    """Load all weather files from S3 in parallel."""
    print("Listing weather files...")
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix="weather/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
                if limit and len(keys) >= limit:
                    break
        if limit and len(keys) >= limit:
            break
    
    print(f"  Found {len(keys)} weather files, loading with {workers} workers...")
    weather_data = {}
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(load_json_from_s3, s3_client, bucket, k): k for k in keys}
        for i, future in enumerate(as_completed(futures)):
            content = future.result()
            if content and content.get("imageId"):
                weather_data[content["imageId"]] = content
            if (i + 1) % 1000 == 0:
                print(f"    Loaded {i + 1}/{len(keys)}...")
    
    print(f"  Loaded {len(weather_data)} weather records")
    return weather_data


def load_analysis_for_weather(s3_client, bucket: str, version: int, weather_ids: set, workers: int) -> Dict[str, Dict]:
    """Load analysis files that match weather data in parallel."""
    print(f"Loading {len(weather_ids)} analysis files with {workers} workers...")
    
    def build_key(image_id):
        parts = image_id.split("T")
        date_parts = parts[0].split("-")
        time_parts = parts[1].split("-")
        year, month, day = date_parts
        hour, minute = time_parts
        return f"analysis/v{version}/{year}/{month}/{day}/{hour}{minute}.json"
    
    id_to_key = {img_id: build_key(img_id) for img_id in weather_ids}
    analysis_data = {}
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(load_json_from_s3, s3_client, bucket, k): img_id for img_id, k in id_to_key.items()}
        for i, future in enumerate(as_completed(futures)):
            img_id = futures[future]
            content = future.result()
            if content:
                analysis_data[img_id] = content
            if (i + 1) % 1000 == 0:
                print(f"    Loaded {i + 1}/{len(weather_ids)}...")
    
    print(f"  Loaded {len(analysis_data)} analysis records")
    return analysis_data


def calculate_correlations(weather_data: Dict, analysis_data: Dict) -> Dict:
    """Calculate correlations between weather variables and visibility."""
    
    def get_best(probs: Optional[Dict]) -> Optional[str]:
        if not probs:
            return None
        return max(probs, key=probs.get)
    
    records = []
    for image_id, weather in weather_data.items():
        if image_id not in analysis_data:
            continue
        
        analysis = analysis_data[image_id]
        frame_state = analysis.get("frame_state") or get_best(analysis.get("frame_state_probabilities"))
        visibility = analysis.get("visibility") or get_best(analysis.get("visibility_probabilities"))
        
        if frame_state != "good" or not visibility:
            continue
        
        is_visible = visibility == "out"
        camera = weather.get("camera", {})
        mountain = weather.get("mountain", {})
        
        records.append({
            "is_visible": is_visible,
            "visibility_class": visibility,
            "cam_visibility": camera.get("visibility"),
            "mtn_visibility": mountain.get("visibility"),
            "mtn_cloud_cover": mountain.get("cloud_cover"),
            "mtn_cloud_mid": mountain.get("cloud_cover_mid"),
            "mtn_precipitation": mountain.get("precipitation"),
            "mtn_humidity": mountain.get("relative_humidity"),
        })
    
    print(f"\nAnalyzing {len(records)} paired records (NORMAL frame state only)")
    
    if not records:
        return {"error": "No paired records found"}
    
    visible_count = sum(1 for r in records if r["is_visible"])
    print(f"  Visible (OUT/PARTIAL): {visible_count} ({100*visible_count/len(records):.1f}%)")
    print(f"  Not visible: {len(records) - visible_count} ({100*(len(records)-visible_count)/len(records):.1f}%)")
    
    weather_vars = ["cam_visibility", "mtn_visibility", "mtn_cloud_cover", "mtn_cloud_mid", "mtn_precipitation", "mtn_humidity"]
    
    correlations = {}
    print("\n--- Mean Values by Visibility ---")
    print(f"{'Variable':<20} {'Visible':>12} {'Not Visible':>12} {'Diff':>10}")
    print("-" * 56)
    
    for var in weather_vars:
        visible_vals = [r[var] for r in records if r["is_visible"] and r.get(var) is not None]
        not_visible_vals = [r[var] for r in records if not r["is_visible"] and r.get(var) is not None]
        
        if visible_vals and not_visible_vals:
            visible_mean = sum(visible_vals) / len(visible_vals)
            not_visible_mean = sum(not_visible_vals) / len(not_visible_vals)
            diff = visible_mean - not_visible_mean
            
            unit = "km" if "visibility" in var else ""
            v_disp = visible_mean / 1000 if "visibility" in var else visible_mean
            nv_disp = not_visible_mean / 1000 if "visibility" in var else not_visible_mean
            d_disp = diff / 1000 if "visibility" in var else diff
            
            correlations[var] = {
                "visible_mean": round(visible_mean, 2),
                "not_visible_mean": round(not_visible_mean, 2),
                "difference": round(diff, 2),
            }
            print(f"{var:<20} {v_disp:>11.1f}{unit} {nv_disp:>11.1f}{unit} {d_disp:>+9.1f}{unit}")
    
    print("\n--- Threshold Analysis (Precision/Recall) ---")
    print("Goal: When we predict 'visible', how often are we right? (precision)")
    print("      Of all visible days, how many do we catch? (recall)\n")
    
    for var in weather_vars:
        vals_with_visibility = [(r[var], r["is_visible"]) for r in records if r.get(var) is not None]
        if not vals_with_visibility:
            continue
        
        all_vals = sorted(v for v, _ in vals_with_visibility)
        if len(all_vals) < 2:
            continue
        
        # Test thresholds at percentiles
        best_f1, best_threshold, best_direction, best_stats = 0, None, None, None
        
        for pct in range(10, 91, 5):
            threshold = all_vals[len(all_vals) * pct // 100]
            
            # Try "greater than" = visible
            tp = sum(1 for v, vis in vals_with_visibility if v > threshold and vis)
            fp = sum(1 for v, vis in vals_with_visibility if v > threshold and not vis)
            fn = sum(1 for v, vis in vals_with_visibility if v <= threshold and vis)
            if tp + fp > 0 and tp + fn > 0:
                prec_gt = tp / (tp + fp)
                rec_gt = tp / (tp + fn)
                f1_gt = 2 * prec_gt * rec_gt / (prec_gt + rec_gt) if prec_gt + rec_gt > 0 else 0
                if f1_gt > best_f1:
                    best_f1, best_threshold, best_direction = f1_gt, threshold, ">"
                    best_stats = (prec_gt, rec_gt, tp, fp, fn)
            
            # Try "less than" = visible
            tp = sum(1 for v, vis in vals_with_visibility if v < threshold and vis)
            fp = sum(1 for v, vis in vals_with_visibility if v < threshold and not vis)
            fn = sum(1 for v, vis in vals_with_visibility if v >= threshold and vis)
            if tp + fp > 0 and tp + fn > 0:
                prec_lt = tp / (tp + fp)
                rec_lt = tp / (tp + fn)
                f1_lt = 2 * prec_lt * rec_lt / (prec_lt + rec_lt) if prec_lt + rec_lt > 0 else 0
                if f1_lt > best_f1:
                    best_f1, best_threshold, best_direction = f1_lt, threshold, "<"
                    best_stats = (prec_lt, rec_lt, tp, fp, fn)
        
        if best_threshold is not None and best_stats:
            prec, rec, tp, fp, fn = best_stats
            disp_thresh = f"{best_threshold/1000:.0f}km" if "visibility" in var else f"{best_threshold:.0f}%"
            print(f"{var}: if {best_direction} {disp_thresh}")
            print(f"  Precision: {prec*100:.0f}% | Recall: {rec*100:.0f}% | F1: {best_f1*100:.0f}%")
            correlations[var]["best_threshold"] = best_threshold
            correlations[var]["precision"] = round(prec, 3)
            correlations[var]["recall"] = round(rec, 3)
            correlations[var]["f1"] = round(best_f1, 3)
    
    # Try combined thresholds
    print("\n--- Combined Threshold Analysis ---")
    mtn_vis_vals = sorted(r["mtn_visibility"] for r in records if r.get("mtn_visibility"))
    mtn_cloud_vals = sorted(r["mtn_cloud_mid"] for r in records if r.get("mtn_cloud_mid") is not None)
    
    best_combined = {"f1": 0}
    for mtn_vis_pct in range(30, 80, 10):
        for mtn_cloud_pct in range(20, 70, 10):
            mtn_vis_thresh = mtn_vis_vals[len(mtn_vis_vals) * mtn_vis_pct // 100]
            mtn_cloud_thresh = mtn_cloud_vals[len(mtn_cloud_vals) * mtn_cloud_pct // 100]
            
            tp = sum(1 for r in records if r.get("mtn_visibility", 0) > mtn_vis_thresh and r.get("mtn_cloud_mid", 100) < mtn_cloud_thresh and r["is_visible"])
            fp = sum(1 for r in records if r.get("mtn_visibility", 0) > mtn_vis_thresh and r.get("mtn_cloud_mid", 100) < mtn_cloud_thresh and not r["is_visible"])
            fn = sum(1 for r in records if not (r.get("mtn_visibility", 0) > mtn_vis_thresh and r.get("mtn_cloud_mid", 100) < mtn_cloud_thresh) and r["is_visible"])
            
            if tp + fp > 0 and tp + fn > 0:
                prec = tp / (tp + fp)
                rec = tp / (tp + fn)
                f1 = 2 * prec * rec / (prec + rec) if prec + rec > 0 else 0
                if f1 > best_combined["f1"]:
                    best_combined = {"f1": f1, "prec": prec, "rec": rec, "mtn_vis": mtn_vis_thresh, "mtn_cloud": mtn_cloud_thresh}
    
    if best_combined["f1"] > 0:
        print(f"Best: mtn_visibility > {best_combined['mtn_vis']/1000:.0f}km AND mtn_cloud_mid < {best_combined['mtn_cloud']:.0f}%")
        print(f"  Precision: {best_combined['prec']*100:.0f}% | Recall: {best_combined['rec']*100:.0f}% | F1: {best_combined['f1']*100:.0f}%")
    
    return {"total_records": len(records), "visible_count": visible_count, "correlations": correlations}


def main():
    args = parse_args()
    
    cache_file = os.path.join(CACHE_DIR, f"{args.bucket}_v{args.version}.json")
    
    # Try loading from cache
    if not args.no_cache and os.path.exists(cache_file):
        print(f"Loading from cache: {cache_file}")
        with open(cache_file) as f:
            cached = json.load(f)
        weather_data = cached["weather"]
        analysis_data = cached["analysis"]
        print(f"  Loaded {len(weather_data)} weather, {len(analysis_data)} analysis records from cache")
    else:
        session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
        s3_client = session.client("s3")
        
        weather_data = load_all_weather(s3_client, args.bucket, args.workers, args.limit)
        if not weather_data:
            print("No weather data found!")
            sys.exit(1)
        
        analysis_data = load_analysis_for_weather(s3_client, args.bucket, args.version, set(weather_data.keys()), args.workers)
        if not analysis_data:
            print("No matching analysis data found!")
            sys.exit(1)
        
        # Save to cache
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(cache_file, "w") as f:
            json.dump({"weather": weather_data, "analysis": analysis_data}, f)
        print(f"  Cached to {cache_file}")
    
    results = calculate_correlations(weather_data, analysis_data)
    
    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\nResults written to {args.output}")
    
    print("\n✓ Analysis complete!")


if __name__ == "__main__":
    main()
