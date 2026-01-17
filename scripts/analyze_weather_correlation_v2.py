#!/usr/bin/env python3
"""
Analyze correlation between weather v2 data and mountain visibility.

Usage:
    python analyze_weather_correlation_v2.py --bucket BUCKET [--profile PROFILE] [--output results.json]
"""

import argparse
import boto3
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional

CACHE_DIR = ".cache/correlation"
WEATHER_PREFIX = "weather/v2"


def parse_args():
    parser = argparse.ArgumentParser(description="Analyze weather v2 correlation")
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


def load_weather_v2(s3_client, bucket: str, workers: int, limit: Optional[int] = None) -> Dict[str, Dict]:
    print("Listing weather v2 files...")
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{WEATHER_PREFIX}/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
                if limit and len(keys) >= limit:
                    break
        if limit and len(keys) >= limit:
            break
    
    print(f"  Found {len(keys)} weather v2 files, loading with {workers} workers...")
    weather_data = {}
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(load_json_from_s3, s3_client, bucket, k): k for k in keys}
        for i, future in enumerate(as_completed(futures)):
            content = future.result()
            if content and content.get("imageId"):
                weather_data[content["imageId"]] = content
            if (i + 1) % 1000 == 0:
                print(f"    Loaded {i + 1}/{len(keys)}...")
    
    print(f"  Loaded {len(weather_data)} weather v2 records")
    return weather_data


def load_analysis_from_cache(bucket: str, version: int) -> Optional[Dict[str, Dict]]:
    """Try to load analysis from existing v1 cache."""
    cache_file = os.path.join(CACHE_DIR, f"{bucket}_v{version}.json")
    if os.path.exists(cache_file):
        print(f"Loading analysis from existing cache: {cache_file}")
        with open(cache_file) as f:
            cached = json.load(f)
        return cached.get("analysis")
    return None


def load_analysis_for_weather(s3_client, bucket: str, version: int, weather_ids: set, workers: int) -> Dict[str, Dict]:
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


def get_best(probs: Optional[Dict]) -> Optional[str]:
    if not probs:
        return None
    return max(probs, key=probs.get)


def calculate_correlations(weather_data: Dict, analysis_data: Dict) -> Dict:
    records = []
    for image_id, weather in weather_data.items():
        if image_id not in analysis_data:
            continue
        
        analysis = analysis_data[image_id]
        frame_state = analysis.get("frame_state") or get_best(analysis.get("frame_state_probabilities"))
        visibility = analysis.get("visibility") or get_best(analysis.get("visibility_probabilities"))
        
        if frame_state != "good" or not visibility:
            continue
        
        is_visible = visibility in ["out", "partially_out"]
        camera = weather.get("camera", {})
        midpoint = weather.get("midpoint", {})
        mountain = weather.get("mountain", {})
        
        records.append({
            "is_visible": is_visible,
            "cam_visibility": camera.get("visibility"),
            "mid_visibility": midpoint.get("visibility"),
            "mtn_cloud_summit": mountain.get("cloud_cover_summit"),
            "mtn_humidity_summit": mountain.get("humidity_summit"),
            "mtn_cloud_low": mountain.get("cloud_cover_low"),
            "mtn_wind_speed": mountain.get("wind_speed"),
            "mtn_freezing_level": mountain.get("freezing_level"),
        })
    
    print(f"\nAnalyzing {len(records)} paired records (good frame state only)")
    
    if not records:
        return {"error": "No paired records found"}
    
    visible_count = sum(1 for r in records if r["is_visible"])
    print(f"  Visible (out): {visible_count} ({100*visible_count/len(records):.1f}%)")
    print(f"  Not visible: {len(records) - visible_count} ({100*(len(records)-visible_count)/len(records):.1f}%)")
    
    weather_vars = ["cam_visibility", "mid_visibility", "mtn_cloud_summit", "mtn_humidity_summit", "mtn_cloud_low", "mtn_wind_speed", "mtn_freezing_level"]
    
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
    
    # Threshold analysis with precision/recall
    print("\n--- Threshold Analysis (Precision/Recall) ---")
    for var in weather_vars:
        vals_with_visibility = [(r[var], r["is_visible"]) for r in records if r.get(var) is not None]
        if not vals_with_visibility:
            continue
        
        all_vals = sorted(v for v, _ in vals_with_visibility)
        if len(all_vals) < 2:
            continue
        
        best_f1, best_threshold, best_direction, best_stats = 0, None, None, None
        
        for pct in range(10, 91, 5):
            threshold = all_vals[len(all_vals) * pct // 100]
            
            for direction, cond in [('>', lambda v, t: v > t), ('<', lambda v, t: v < t)]:
                tp = sum(1 for v, vis in vals_with_visibility if cond(v, threshold) and vis)
                fp = sum(1 for v, vis in vals_with_visibility if cond(v, threshold) and not vis)
                fn = sum(1 for v, vis in vals_with_visibility if not cond(v, threshold) and vis)
                
                if tp + fp > 0 and tp + fn > 0:
                    prec = tp / (tp + fp)
                    rec = tp / (tp + fn)
                    f1 = 2 * prec * rec / (prec + rec) if prec + rec > 0 else 0
                    if f1 > best_f1:
                        best_f1, best_threshold, best_direction = f1, threshold, direction
                        best_stats = (prec, rec)
        
        if best_threshold is not None and best_stats:
            prec, rec = best_stats
            disp_thresh = f"{best_threshold/1000:.0f}km" if "visibility" in var else f"{best_threshold:.0f}"
            print(f"{var}: if {best_direction} {disp_thresh}")
            print(f"  Precision: {prec*100:.0f}% | Recall: {rec*100:.0f}% | F1: {best_f1*100:.0f}%")
            correlations[var]["best_threshold"] = best_threshold
            correlations[var]["precision"] = round(prec, 3)
            correlations[var]["recall"] = round(rec, 3)
            correlations[var]["f1"] = round(best_f1, 3)
    
    # Combined threshold analysis
    print("\n--- Combined Threshold Analysis ---")
    valid_records = [r for r in records if r.get("mtn_cloud_summit") is not None and r.get("cam_visibility") is not None]
    
    if valid_records:
        mtn_cloud_vals = sorted(r["mtn_cloud_summit"] for r in valid_records)
        cam_vis_vals = sorted(r["cam_visibility"] for r in valid_records)
        
        best_combined = {"f1": 0}
        for cloud_thresh in [1, 2, 5, 10, 20, 30, 50]:
            for vis_thresh in [15000, 20000, 25000, 30000, 35000, 40000]:
                tp = sum(1 for r in valid_records if r["mtn_cloud_summit"] < cloud_thresh and r["cam_visibility"] > vis_thresh and r["is_visible"])
                fp = sum(1 for r in valid_records if r["mtn_cloud_summit"] < cloud_thresh and r["cam_visibility"] > vis_thresh and not r["is_visible"])
                fn = sum(1 for r in valid_records if not (r["mtn_cloud_summit"] < cloud_thresh and r["cam_visibility"] > vis_thresh) and r["is_visible"])
                
                if tp + fp > 0 and tp + fn > 0:
                    prec = tp / (tp + fp)
                    rec = tp / (tp + fn)
                    f1 = 2 * prec * rec / (prec + rec) if prec + rec > 0 else 0
                    if f1 > best_combined["f1"]:
                        best_combined = {"f1": f1, "prec": prec, "rec": rec, "cloud": cloud_thresh, "vis": vis_thresh, "tp": tp, "fp": fp, "fn": fn}
        
        if best_combined["f1"] > 0:
            print(f"Best: mtn_cloud_summit < {best_combined['cloud']}% AND cam_visibility > {best_combined['vis']/1000:.0f}km")
            print(f"  Precision: {best_combined['prec']*100:.0f}% | Recall: {best_combined['rec']*100:.0f}% | F1: {best_combined['f1']*100:.0f}%")
            print(f"  (TP={best_combined['tp']}, FP={best_combined['fp']}, FN={best_combined['fn']})")
    
    # Try 3-variable combination
    print("\n--- 3-Variable Combined Threshold ---")
    valid_3 = [r for r in records if all(r.get(k) is not None for k in ["cam_visibility", "mtn_cloud_summit", "mtn_humidity_summit"])]
    
    if valid_3:
        best_3 = {"f1": 0}
        for vis_t in [20000, 25000, 30000]:
            for cloud_t in [2, 5, 10]:
                for hum_t in [40, 50, 60]:
                    tp = sum(1 for r in valid_3 if r["cam_visibility"] > vis_t and r["mtn_cloud_summit"] < cloud_t and r["mtn_humidity_summit"] < hum_t and r["is_visible"])
                    fp = sum(1 for r in valid_3 if r["cam_visibility"] > vis_t and r["mtn_cloud_summit"] < cloud_t and r["mtn_humidity_summit"] < hum_t and not r["is_visible"])
                    fn = sum(1 for r in valid_3 if not (r["cam_visibility"] > vis_t and r["mtn_cloud_summit"] < cloud_t and r["mtn_humidity_summit"] < hum_t) and r["is_visible"])
                    
                    if tp + fp > 0 and tp + fn > 0:
                        prec = tp / (tp + fp)
                        rec = tp / (tp + fn)
                        f1 = 2 * prec * rec / (prec + rec) if prec + rec > 0 else 0
                        if f1 > best_3["f1"]:
                            best_3 = {"f1": f1, "prec": prec, "rec": rec, "vis": vis_t, "cloud": cloud_t, "hum": hum_t}
        
        if best_3["f1"] > 0:
            print(f"Best: cam_visibility > {best_3['vis']/1000:.0f}km AND mtn_cloud_summit < {best_3['cloud']}% AND mtn_humidity_summit < {best_3['hum']}%")
            print(f"  Precision: {best_3['prec']*100:.0f}% | Recall: {best_3['rec']*100:.0f}% | F1: {best_3['f1']*100:.0f}%")
    
    # Simple weighted score approach
    print("\n--- Weighted Score Analysis ---")
    
    # Grid search for best weights
    best_overall = {"f1": 0}
    
    for w_vis in [0.3, 0.4, 0.5]:
        for w_cloud in [0.15, 0.25, 0.35]:
            for w_hum in [0.1, 0.2, 0.3]:
                for w_low in [0.05, 0.1, 0.15, 0.2]:
                    for w_mid in [0, 0.1, 0.15]:
                        for w_freeze in [0, 0.05, 0.1]:
                            # Skip if weights don't sum close to 1
                            total_w = w_vis + w_cloud + w_hum + w_low + w_mid + w_freeze
                            if total_w < 0.9 or total_w > 1.1:
                                continue
                            
                            scored = []
                            for r in records:
                                if all(r.get(k) is not None for k in ["cam_visibility", "mtn_cloud_summit", "mtn_humidity_summit", "mtn_cloud_low"]):
                                    vis_s = min(100, r["cam_visibility"] / 500)
                                    cloud_s = 100 - r["mtn_cloud_summit"]
                                    hum_s = 100 - r["mtn_humidity_summit"]
                                    low_s = 100 - r["mtn_cloud_low"]
                                    mid_s = min(100, r.get("mid_visibility", 0) / 500) if w_mid > 0 else 0
                                    freeze_s = min(100, r.get("mtn_freezing_level", 0) / 40) if w_freeze > 0 else 0  # 4000m = 100
                                    
                                    score = (vis_s * w_vis) + (cloud_s * w_cloud) + (hum_s * w_hum) + (low_s * w_low) + (mid_s * w_mid) + (freeze_s * w_freeze)
                                    scored.append({"score": score, "is_visible": r["is_visible"]})
                            
                            if not scored:
                                continue
                            
                            for thresh in range(50, 80, 5):
                                tp = sum(1 for r in scored if r["score"] > thresh and r["is_visible"])
                                fp = sum(1 for r in scored if r["score"] > thresh and not r["is_visible"])
                                fn = sum(1 for r in scored if r["score"] <= thresh and r["is_visible"])
                                
                                if tp + fp > 0 and tp + fn > 0:
                                    prec = tp / (tp + fp)
                                    rec = tp / (tp + fn)
                                    f1 = 2 * prec * rec / (prec + rec) if prec + rec > 0 else 0
                                    if f1 > best_overall["f1"]:
                                        best_overall = {
                                            "f1": f1, "prec": prec, "rec": rec, "thresh": thresh,
                                            "w_vis": w_vis, "w_cloud": w_cloud, "w_hum": w_hum, 
                                            "w_low": w_low, "w_mid": w_mid, "w_freeze": w_freeze
                                        }
    
    if best_overall["f1"] > 0:
        print(f"Best weights found:")
        print(f"  cam_visibility:    {best_overall['w_vis']}")
        print(f"  mtn_cloud_summit:  {best_overall['w_cloud']}")
        print(f"  mtn_humidity:      {best_overall['w_hum']}")
        print(f"  mtn_cloud_low:     {best_overall['w_low']}")
        print(f"  mid_visibility:    {best_overall['w_mid']}")
        print(f"  freezing_level:    {best_overall['w_freeze']}")
        print(f"  Threshold: > {best_overall['thresh']}")
        print(f"  Precision: {best_overall['prec']*100:.0f}% | Recall: {best_overall['rec']*100:.0f}% | F1: {best_overall['f1']*100:.0f}%")
    
    return {"total_records": len(records), "visible_count": visible_count, "correlations": correlations}


def main():
    args = parse_args()
    
    weather_cache = os.path.join(CACHE_DIR, f"{args.bucket}_v2_weather.json")
    analysis_cache = os.path.join(CACHE_DIR, f"{args.bucket}_v{args.version}.json")
    
    # Load weather v2
    if not args.no_cache and os.path.exists(weather_cache):
        print(f"Loading weather v2 from cache: {weather_cache}")
        with open(weather_cache) as f:
            weather_data = json.load(f)
        print(f"  Loaded {len(weather_data)} weather v2 records from cache")
    else:
        session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
        s3_client = session.client("s3")
        weather_data = load_weather_v2(s3_client, args.bucket, args.workers, args.limit)
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(weather_cache, "w") as f:
            json.dump(weather_data, f)
        print(f"  Cached weather v2 to {weather_cache}")
    
    if not weather_data:
        print("No weather v2 data found!")
        sys.exit(1)
    
    # Load analysis (try existing cache first)
    analysis_data = None
    if not args.no_cache and os.path.exists(analysis_cache):
        print(f"Loading analysis from existing cache: {analysis_cache}")
        with open(analysis_cache) as f:
            cached = json.load(f)
        analysis_data = cached.get("analysis", {})
        # Filter to only IDs we have weather for
        analysis_data = {k: v for k, v in analysis_data.items() if k in weather_data}
        print(f"  Loaded {len(analysis_data)} matching analysis records from cache")
    
    if not analysis_data:
        session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
        s3_client = session.client("s3")
        analysis_data = load_analysis_for_weather(s3_client, args.bucket, args.version, set(weather_data.keys()), args.workers)
    
    if not analysis_data:
        print("No matching analysis data found!")
        sys.exit(1)
    
    results = calculate_correlations(weather_data, analysis_data)
    
    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\nResults written to {args.output}")
    
    print("\n✓ Analysis complete!")


if __name__ == "__main__":
    main()
