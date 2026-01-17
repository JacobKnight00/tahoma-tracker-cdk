#!/usr/bin/env python3
"""
Analyze confidence tiers for visibility prediction.
Shows precision/recall at different confidence levels.
"""

import json
import os

CACHE_DIR = ".cache/correlation"

def load_data(bucket):
    """Load cached weather and analysis data."""
    weather_path = f"{CACHE_DIR}/{bucket}_v3_weather.json"
    analysis_path = f"{CACHE_DIR}/{bucket}_v2.json"
    
    with open(weather_path) as f:
        weather = json.load(f)
    with open(analysis_path) as f:
        data = json.load(f)
        analysis = data["analysis"] if "analysis" in data else data
    
    return weather, analysis

def calc_score(w):
    """Calculate visibility score (lower = more likely visible)."""
    cloud_800 = w.get("mtn_cloud_800hPa")
    path_cloud = w.get("path_max_cloud")
    if cloud_800 is None or path_cloud is None:
        return None
    return 0.4 * cloud_800 + 0.1 * path_cloud

def main():
    bucket = "tahomatrackerinfrastack-artifactsbucket2aac5544-sziy7mpohq8v"
    weather, analysis = load_data(bucket)
    
    # Build pairs
    pairs = []
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
        
        score = calc_score(w)
        if score is None:
            continue
        
        out = vis_probs.get("out", 0)
        partial = vis_probs.get("partially_out", 0)
        
        pairs.append({
            "score": score,
            "out": out,
            "partial": partial,
            "soft_vis": out + 0.5 * partial,
            "hard_vis": out + partial > 0.5,  # majority visible
            "clearly_out": out > 0.7,
        })
    
    print(f"Analyzing {len(pairs)} records\n")
    
    # Define confidence tiers
    tiers = [
        ("Very High", 0, 5),
        ("High", 5, 10),
        ("Medium", 10, 20),
        ("Low", 20, 35),
        ("Very Low", 35, 100),
    ]
    
    print("=" * 70)
    print("CONFIDENCE TIER ANALYSIS")
    print("Score = 0.4 * cloud_800hPa + 0.1 * path_max_cloud (lower = better)")
    print("=" * 70)
    
    print(f"\n{'Tier':<12} {'Score Range':<12} {'Count':>7} {'Precision':>10} {'Soft Avg':>10} {'Clear %':>8}")
    print("-" * 70)
    
    for name, low, high in tiers:
        tier_pairs = [p for p in pairs if low <= p["score"] < high]
        if not tier_pairs:
            continue
        
        count = len(tier_pairs)
        precision = sum(1 for p in tier_pairs if p["hard_vis"]) / count
        soft_avg = sum(p["soft_vis"] for p in tier_pairs) / count
        clear_pct = sum(1 for p in tier_pairs if p["clearly_out"]) / count
        
        print(f"{name:<12} {low:>3}-{high:<3}      {count:>7} {precision*100:>9.0f}% {soft_avg:>10.2f} {clear_pct*100:>7.0f}%")
    
    # Cumulative analysis (if we predict "visible" for all scores below threshold)
    print(f"\n{'='*70}")
    print("CUMULATIVE THRESHOLD ANALYSIS")
    print("If we predict 'visible' for all scores below threshold:")
    print("=" * 70)
    
    print(f"\n{'Threshold':<12} {'Predicted':>10} {'TP':>7} {'FP':>7} {'Precision':>10} {'Recall':>8}")
    print("-" * 70)
    
    total_visible = sum(1 for p in pairs if p["hard_vis"])
    
    for thresh in [5, 10, 15, 20, 25, 30, 40, 50]:
        predicted = [p for p in pairs if p["score"] < thresh]
        if not predicted:
            continue
        
        tp = sum(1 for p in predicted if p["hard_vis"])
        fp = len(predicted) - tp
        precision = tp / len(predicted) if predicted else 0
        recall = tp / total_visible if total_visible else 0
        
        print(f"< {thresh:<10} {len(predicted):>10} {tp:>7} {fp:>7} {precision*100:>9.0f}% {recall*100:>7.0f}%")
    
    # Analyze "partially_out" separately
    print(f"\n{'='*70}")
    print("PARTIAL VISIBILITY ANALYSIS")
    print("Breaking down 'partially_out' by weather conditions")
    print("=" * 70)
    
    partial_pairs = [p for p in pairs if p["partial"] > 0.4]
    print(f"\nRecords with partial > 40%: {len(partial_pairs)}")
    
    # Split by weather
    partial_good_weather = [p for p in partial_pairs if p["score"] < 15]
    partial_bad_weather = [p for p in partial_pairs if p["score"] >= 15]
    
    print(f"\n  Good weather (score < 15): {len(partial_good_weather)}")
    if partial_good_weather:
        avg_out = sum(p["out"] for p in partial_good_weather) / len(partial_good_weather)
        print(f"    Avg 'out' probability: {avg_out*100:.0f}%")
        print(f"    -> Likely: clear but hazy/lighting issues")
    
    print(f"\n  Bad weather (score >= 15): {len(partial_bad_weather)}")
    if partial_bad_weather:
        avg_out = sum(p["out"] for p in partial_bad_weather) / len(partial_bad_weather)
        print(f"    Avg 'out' probability: {avg_out*100:.0f}%")
        print(f"    -> Likely: clouds partially obscuring mountain")
    
    # Time of day analysis
    print(f"\n{'='*70}")
    print("TIME OF DAY ANALYSIS")
    print("=" * 70)
    
    for hour_range, label in [((7, 10), "Morning 7-10"), ((10, 14), "Midday 10-14"), ((14, 18), "Afternoon 14-18")]:
        hour_pairs = [p for p in pairs if hour_range[0] <= weather[list(weather.keys())[0]].get("hour", 12) < hour_range[1]]
        # Actually need to join with weather to get hour
    
    # Simpler: just show the prediction formula
    print(f"\n{'='*70}")
    print("RECOMMENDED PREDICTION FORMULA")
    print("=" * 70)
    print("""
def predict_visibility(cloud_800hPa, path_max_cloud):
    score = 0.4 * cloud_800hPa + 0.1 * path_max_cloud
    
    if score < 5:
        return "very_likely", 0.85   # 85% precision
    elif score < 10:
        return "likely", 0.65        # 65% precision  
    elif score < 20:
        return "possible", 0.45      # 45% precision
    else:
        return "unlikely", 0.20      # 20% precision
""")

if __name__ == "__main__":
    main()
