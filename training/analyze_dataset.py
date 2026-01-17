#!/usr/bin/env python3
"""
Analyze training dataset: class distribution, label sources, and filtering stats.
Saves output to models/v{N}/dataset_analysis.json for use by training scripts.

Usage:
    python analyze_dataset.py --version 2
"""

import argparse
import json
import sys
from pathlib import Path
from collections import Counter
from datetime import datetime

from config import TrainingConfig
from data_loader import DynamoDBLabelLoader, ManifestLoader


def compute_class_weights(counts: dict, labels: list) -> list:
    """Compute inverse-frequency weights for class balancing."""
    freqs = [counts.get(l, 1) for l in labels]
    max_freq = max(freqs)
    return [max_freq / f for f in freqs]


def main():
    parser = argparse.ArgumentParser(description='Analyze training dataset')
    parser.add_argument('--version', type=int, required=True, help='Model version (e.g., 2)')
    parser.add_argument('--config', default='config.yaml', help='Config file path')
    args = parser.parse_args()
    
    config = TrainingConfig(args.config)
    version = args.version
    
    # Initialize manifest loader for filtering stats
    manifest_loader = ManifestLoader(
        base_url=config.manifest_base_url,
        cache_dir=config.manifest_cache_dir
    )
    
    # Load all labels with filtering
    loader = DynamoDBLabelLoader(
        table_name=config.dynamodb_table_name,
        region=config.aws_region,
        profile=config.aws_profile
    )
    
    all_labels = loader.load_all_labels(
        manifest_loader=manifest_loader,
        confidence_threshold=config.manifest_confidence_threshold
    )
    
    # Separate included vs excluded
    included = [l for l in all_labels if not l.excluded]
    excluded = [l for l in all_labels if l.excluded]
    
    # Frame state counts
    fs_labels = ['good', 'dark', 'bad', 'off_target']
    fs_counts = Counter(l.frame_state for l in included)
    fs_weights = compute_class_weights(fs_counts, fs_labels)
    
    # Visibility counts (only good frames with visibility)
    vis_labels = ['out', 'partially_out', 'not_out']
    vis_data = [l for l in included if l.frame_state == 'good' and l.visibility]
    vis_counts = Counter(l.visibility for l in vis_data)
    vis_weights = compute_class_weights(vis_counts, vis_labels)
    
    # Exclusion breakdown
    fs_excluded = len([l for l in excluded if 'frameState' in (l.exclusion_reason or '')])
    vis_excluded = len([l for l in excluded if 'visibility' in (l.exclusion_reason or '')])
    
    # Build analysis result
    analysis = {
        'version': version,
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'confidence_threshold': config.manifest_confidence_threshold,
        'totals': {
            'all_labels': len(all_labels),
            'included': len(included),
            'excluded': len(excluded),
            'excluded_by_frame_state': fs_excluded,
            'excluded_by_visibility': vis_excluded,
        },
        'frame_state': {
            'labels': fs_labels,
            'counts': {l: fs_counts.get(l, 0) for l in fs_labels},
            'weights': {l: round(w, 4) for l, w in zip(fs_labels, fs_weights)},
        },
        'visibility': {
            'labels': vis_labels,
            'total_samples': len(vis_data),
            'counts': {l: vis_counts.get(l, 0) for l in vis_labels},
            'weights': {l: round(w, 4) for l, w in zip(vis_labels, vis_weights)},
        },
    }
    
    # Save to models/v{N}/dataset_analysis.json
    output_dir = config.output_models_dir / f'v{version}'
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / 'dataset_analysis.json'
    
    with open(output_path, 'w') as f:
        json.dump(analysis, f, indent=2)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"DATASET ANALYSIS (v{version})")
    print(f"{'='*60}")
    print(f"Total labels: {len(all_labels)}")
    print(f"Included for training: {len(included)}")
    print(f"Excluded by filtering: {len(excluded)}")
    
    print(f"\n--- Frame State Distribution ---")
    total_fs = sum(fs_counts.values())
    for label in fs_labels:
        count = fs_counts.get(label, 0)
        pct = count / total_fs * 100 if total_fs > 0 else 0
        weight = analysis['frame_state']['weights'][label]
        bar = '█' * int(pct / 2)
        print(f"  {label:12} {count:5} ({pct:5.1f}%) weight={weight:.2f} {bar}")
    
    print(f"\n--- Visibility Distribution ---")
    total_vis = len(vis_data)
    for label in vis_labels:
        count = vis_counts.get(label, 0)
        pct = count / total_vis * 100 if total_vis > 0 else 0
        weight = analysis['visibility']['weights'][label]
        bar = '█' * int(pct / 2)
        print(f"  {label:15} {count:5} ({pct:5.1f}%) weight={weight:.2f} {bar}")
    
    print(f"\nSaved to: {output_path}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
