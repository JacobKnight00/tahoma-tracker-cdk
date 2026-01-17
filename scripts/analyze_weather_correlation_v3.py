#!/usr/bin/env python3
"""
Analyze weather v3 correlation - tests multi-level clouds and path sampling.

Reuses cached analysis data from v1/v2 scripts when available.
"""

import argparse
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

CACHE_DIR = ".cache/correlation"


def parse_args():
    parser = argparse.ArgumentParser(description="Analyze weather v3 correlation")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--profile", default=None)
    parser.add_argument("--output", default="weather-correlation-v3.json")
    parser.add_argument("--workers", type=int, default=50)
    parser.add_argument("--no-cache", action="store_true")
    return parser.parse_args()


def load_weather_v3(s3, bucket, cache_path, workers, no_cache):
    """Load weather v3 files with caching."""
    if os.path.exists(cache_path) and not no_cache:
        print(f"Loading weather v3 from cache: {cache_path}")
        with open(cache_path) as f:
            data = json.load(f)
        print(f"  Loaded {len(data)} records from cache")
        return data
    
    print(f"Loading weather v3 from S3...")
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix="weather/v3/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
    
    print(f"  Found {len(keys)} files, loading with {workers} workers...")
    
    def fetch(key):
        resp = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read())
    
    data = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(fetch, k): k for k in keys}
        for i, future in enumerate(as_completed(futures)):
            try:
                record = future.result()
                if record and record.get("imageId"):
                    data[record["imageId"]] = record
            except:
                pass
            if (i + 1) % 2000 == 0:
                print(f"    Loaded {i + 1}/{len(keys)}...")
    
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, "w") as f:
        json.dump(data, f)
    print(f"  Cached {len(data)} records")
    return data


def load_analysis(s3, bucket, cache_path, workers, no_cache):
    """Load analysis data, reusing existing cache if available."""
    # Try to reuse v2 cache first (same data, different weather)
    v2_cache = cache_path.replace("_v3.json", "_v2.json")
    if os.path.exists(v2_cache) and not no_cache:
        print(f"Reusing analysis cache from v2: {v2_cache}")
        with open(v2_cache) as f:
            data = json.load(f)
        # Handle nested structure {"weather": ..., "analysis": ...}
        if isinstance(data, dict) and "analysis" in data:
            data = data["analysis"]
        print(f"  Loaded {len(data)} records")
        return data
    
    if os.path.exists(cache_path) and not no_cache:
        print(f"Loading analysis from cache: {cache_path}")
        with open(cache_path) as f:
            data = json.load(f)
        print(f"  Loaded {len(data)} records")
        return data
    
    print(f"Loading analysis from S3...")
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix="analysis/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
    
    print(f"  Found {len(keys)} files, loading with {workers} workers...")
    
    def fetch(key):
        resp = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read())
    
    data = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(fetch, k): k for k in keys}
        for i, future in enumerate(as_completed(futures)):
            try:
                record = future.result()
                img_id = record.get("imageId", "")
                if img_id:
                    # Normalize: 2025-01-10T12-00-00 -> 2025-01-10T12-00
                    normalized = img_id.rsplit("-", 1)[0] if img_id.count("-") > 3 else img_id
                    data[normalized] = record
            except:
                pass
            if (i + 1) % 2000 == 0:
                print(f"    Loaded {i + 1}/{len(keys)}...")
    
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, "w") as f:
        json.dump(data, f)
    print(f"  Cached {len(data)} records")
    return data


def analyze(weather, analysis):
    """Analyze correlation between weather v3 and visibility."""
    pairs = []
    for img_id, w in weather.items():
        a = analysis.get(img_id)
        if not a:
            continue
        
        # Handle both direct values and probability dicts
        frame_state = a.get("frame_state")
        if not frame_state and a.get("frame_state_probabilities"):
            probs = a["frame_state_probabilities"]
            frame_state = max(probs, key=probs.get)
        
        if frame_state != "good":
            continue
        
        vis_class = a.get("visibility_classification")
        if not vis_class and a.get("visibility_probabilities"):
            probs = a["visibility_probabilities"]
            vis_class = max(probs, key=probs.get)
        
        is_visible = vis_class in ["out", "partially_out"]
        pairs.append({"weather": w, "visible": is_visible, "class": vis_class})
    
    print(f"\nAnalyzing {len(pairs)} paired records (good frame state only)")
    visible = sum(1 for p in pairs if p["visible"])
    print(f"  Visible (out+partially): {visible} ({100*visible/len(pairs):.1f}%)")
    print(f"  Not visible: {len(pairs)-visible} ({100*(len(pairs)-visible)/len(pairs):.1f}%)")
    
    # Variables to analyze
    variables = [
        ("cam_visibility", True, 1000),
        ("path_min_visibility", True, 1000),
        ("path_max_cloud", False, 1),
        ("mtn_cloud_low", False, 1),
        ("mtn_cloud_mid", False, 1),
        ("mtn_cloud_850hPa", False, 1),
        ("mtn_cloud_800hPa", False, 1),
        ("mtn_cloud_700hPa", False, 1),
        ("mtn_cloud_600hPa", False, 1),
        ("mtn_humidity_700hPa", False, 1),
        ("dew_depression", True, 1),
        ("pressure_trend", True, 1),
        ("vis_trend", True, 1),
    ]
    
    # Mean values
    print("\n--- Mean Values by Visibility ---")
    print(f"{'Variable':<24} {'Visible':>10} {'Not Visible':>12} {'Diff':>10}")
    print("-" * 60)
    
    for var, higher_better, scale in variables:
        vis_vals = [p["weather"].get(var) for p in pairs if p["visible"] and p["weather"].get(var) is not None]
        not_vals = [p["weather"].get(var) for p in pairs if not p["visible"] and p["weather"].get(var) is not None]
        if vis_vals and not_vals:
            vis_mean = sum(vis_vals) / len(vis_vals) / scale
            not_mean = sum(not_vals) / len(not_vals) / scale
            unit = "km" if scale == 1000 else ""
            print(f"{var:<24} {vis_mean:>9.1f}{unit} {not_mean:>11.1f}{unit} {vis_mean - not_mean:>+10.1f}")
    
    # Threshold analysis
    print("\n--- Single Variable Threshold Analysis ---")
    results = {}
    
    for var, higher_better, scale in variables:
        vals = [(p["weather"].get(var), p["visible"]) for p in pairs if p["weather"].get(var) is not None]
        if not vals:
            continue
        
        best_f1, best_thresh, best_stats = 0, 0, None
        sorted_vals = sorted(set(v[0] for v in vals))
        
        for thresh in sorted_vals[::max(1, len(sorted_vals)//50)]:
            if higher_better:
                tp = sum(1 for v, vis in vals if v > thresh and vis)
                fp = sum(1 for v, vis in vals if v > thresh and not vis)
                fn = sum(1 for v, vis in vals if v <= thresh and vis)
            else:
                tp = sum(1 for v, vis in vals if v < thresh and vis)
                fp = sum(1 for v, vis in vals if v < thresh and not vis)
                fn = sum(1 for v, vis in vals if v >= thresh and vis)
            
            prec = tp / (tp + fp) if (tp + fp) > 0 else 0
            rec = tp / (tp + fn) if (tp + fn) > 0 else 0
            f1 = 2 * prec * rec / (prec + rec) if (prec + rec) > 0 else 0
            
            if f1 > best_f1:
                best_f1, best_thresh, best_stats = f1, thresh, (prec, rec, tp, fp, fn)
        
        if best_stats:
            prec, rec, tp, fp, fn = best_stats
            op = ">" if higher_better else "<"
            thresh_display = f"{best_thresh/scale:.0f}km" if scale == 1000 else f"{best_thresh:.0f}"
            print(f"{var}: if {op} {thresh_display}")
            print(f"  Precision: {prec*100:.0f}% | Recall: {rec*100:.0f}% | F1: {best_f1*100:.0f}%")
            results[var] = {"threshold": best_thresh, "higher_better": higher_better, "f1": best_f1, "precision": prec, "recall": rec}
    
    # Hypothesis tests
    print("\n--- Hypothesis Testing ---")
    
    print("\nH1: Mid-mountain clouds (800hPa) vs summit clouds (600hPa)")
    for var in ["mtn_cloud_800hPa", "mtn_cloud_600hPa"]:
        vals = [(p["weather"].get(var), p["visible"]) for p in pairs if p["weather"].get(var) is not None]
        for thresh in [5, 10, 20]:
            tp = sum(1 for v, vis in vals if v < thresh and vis)
            fp = sum(1 for v, vis in vals if v < thresh and not vis)
            fn = sum(1 for v, vis in vals if v >= thresh and vis)
            prec = tp / (tp + fp) if (tp + fp) > 0 else 0
            rec = tp / (tp + fn) if (tp + fn) > 0 else 0
            f1 = 2 * prec * rec / (prec + rec) if (prec + rec) > 0 else 0
            print(f"  {var} < {thresh}%: P={prec*100:.0f}% R={rec*100:.0f}% F1={f1*100:.0f}%")
    
    print("\nH2: Path minimum visibility vs camera visibility")
    for var in ["path_min_visibility", "cam_visibility"]:
        vals = [(p["weather"].get(var), p["visible"]) for p in pairs if p["weather"].get(var) is not None]
        for thresh in [15000, 20000, 25000]:
            tp = sum(1 for v, vis in vals if v > thresh and vis)
            fp = sum(1 for v, vis in vals if v > thresh and not vis)
            fn = sum(1 for v, vis in vals if v <= thresh and vis)
            prec = tp / (tp + fp) if (tp + fp) > 0 else 0
            rec = tp / (tp + fn) if (tp + fn) > 0 else 0
            f1 = 2 * prec * rec / (prec + rec) if (prec + rec) > 0 else 0
            print(f"  {var} > {thresh/1000:.0f}km: P={prec*100:.0f}% R={rec*100:.0f}% F1={f1*100:.0f}%")
    
    print("\nH3: Dew point depression (temp - dew point)")
    vals = [(p["weather"].get("dew_depression"), p["visible"]) for p in pairs if p["weather"].get("dew_depression") is not None]
    for thresh in [3, 5, 10]:
        tp = sum(1 for v, vis in vals if v > thresh and vis)
        fp = sum(1 for v, vis in vals if v > thresh and not vis)
        fn = sum(1 for v, vis in vals if v <= thresh and vis)
        prec = tp / (tp + fp) if (tp + fp) > 0 else 0
        rec = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) > 0 else 0
        print(f"  dew_depression > {thresh}°C: P={prec*100:.0f}% R={rec*100:.0f}% F1={f1*100:.0f}%")
    
    # Weighted score optimization
    print("\n--- Weighted Score Optimization ---")
    best_f1, best_weights, best_thresh = 0, None, 0
    
    # Grid search over weight combinations
    weight_options = [0, 0.1, 0.2, 0.3, 0.4]
    from itertools import product
    
    def calc_score(w, weights):
        """Calculate weighted score for a weather record."""
        score = 0
        # Visibility (higher = better, normalize to 0-100)
        if w.get("cam_visibility"): score += (w["cam_visibility"] / 500) * weights[0]
        # Path max cloud (lower = better, invert)
        if w.get("path_max_cloud") is not None: score += (100 - w["path_max_cloud"]) * weights[1]
        # 800hPa cloud (lower = better, invert)
        if w.get("mtn_cloud_800hPa") is not None: score += (100 - w["mtn_cloud_800hPa"]) * weights[2]
        # Humidity 700hPa (lower = better, invert)
        if w.get("mtn_humidity_700hPa") is not None: score += (100 - w["mtn_humidity_700hPa"]) * weights[3]
        # Dew depression (higher = better)
        if w.get("dew_depression"): score += min(w["dew_depression"] * 5, 100) * weights[4]
        return score
    
    # Pre-filter pairs with all required fields
    valid_pairs = [p for p in pairs if all(p["weather"].get(k) is not None for k in 
                   ["cam_visibility", "path_max_cloud", "mtn_cloud_800hPa", "mtn_humidity_700hPa", "dew_depression"])]
    print(f"  Testing {len(valid_pairs)} records with complete data...")
    
    for weights in product(weight_options, repeat=5):
        if sum(weights) == 0:
            continue
        
        scores = [(calc_score(p["weather"], weights), p["visible"]) for p in valid_pairs]
        
        # Find best threshold for these weights
        for thresh in range(20, 80, 5):
            tp = sum(1 for s, vis in scores if s > thresh and vis)
            fp = sum(1 for s, vis in scores if s > thresh and not vis)
            fn = sum(1 for s, vis in scores if s <= thresh and vis)
            
            prec = tp / (tp + fp) if (tp + fp) > 0 else 0
            rec = tp / (tp + fn) if (tp + fn) > 0 else 0
            f1 = 2 * prec * rec / (prec + rec) if (prec + rec) > 0 else 0
            
            if f1 > best_f1:
                best_f1, best_weights, best_thresh = f1, weights, thresh
                best_stats = (prec, rec, tp, fp, fn)
    
    if best_weights:
        prec, rec, tp, fp, fn = best_stats
        print(f"  Best weights: vis={best_weights[0]}, path_cloud={best_weights[1]}, cloud_800={best_weights[2]}, humid_700={best_weights[3]}, dew={best_weights[4]}")
        print(f"  Threshold: > {best_thresh}")
        print(f"  Precision: {prec*100:.0f}% | Recall: {rec*100:.0f}% | F1: {best_f1*100:.0f}%")
        results["weighted_score"] = {"weights": best_weights, "threshold": best_thresh, "f1": best_f1, "precision": prec, "recall": rec}
    
    # Also test with soft labels (using probability scores)
    print("\n--- Soft Label Analysis ---")
    soft_pairs = []
    for img_id, w in weather.items():
        a = analysis.get(img_id)
        if not a:
            continue
        frame_probs = a.get("frame_state_probabilities", {})
        if frame_probs.get("good", 0) < 0.7:
            continue
        vis_probs = a.get("visibility_probabilities", {})
        if not vis_probs:
            continue
        # Soft visibility score: out=1, partial=0.5, not_out=0
        soft_vis = vis_probs.get("out", 0) + 0.5 * vis_probs.get("partially_out", 0)
        soft_pairs.append({"weather": w, "soft_vis": soft_vis, "hard_vis": soft_vis > 0.3})
    
    print(f"  {len(soft_pairs)} records with probability data")
    if soft_pairs:
        # Correlation between weather and soft visibility
        for var in ["cam_visibility", "path_max_cloud", "mtn_cloud_800hPa", "dew_depression"]:
            vals = [(p["weather"].get(var), p["soft_vis"]) for p in soft_pairs if p["weather"].get(var) is not None]
            if len(vals) > 100:
                x = [v[0] for v in vals]
                y = [v[1] for v in vals]
                # Simple correlation
                x_mean, y_mean = sum(x)/len(x), sum(y)/len(y)
                num = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y))
                den = (sum((xi - x_mean)**2 for xi in x) * sum((yi - y_mean)**2 for yi in y)) ** 0.5
                corr = num / den if den > 0 else 0
                print(f"  {var}: correlation = {corr:.3f}")
    
    return results


def main():
    args = parse_args()
    
    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    s3 = session.client("s3")
    
    cache_base = f"{CACHE_DIR}/{args.bucket}"
    os.makedirs(CACHE_DIR, exist_ok=True)
    
    weather = load_weather_v3(s3, args.bucket, f"{cache_base}_v3_weather.json", args.workers, args.no_cache)
    analysis = load_analysis(s3, args.bucket, f"{cache_base}_v3.json", args.workers, args.no_cache)
    
    results = analyze(weather, analysis)
    
    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults written to {args.output}")


if __name__ == "__main__":
    main()
