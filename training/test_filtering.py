#!/usr/bin/env python3
"""Quick smoke test for label loading and filtering logic."""

from pathlib import Path
import sys

# Ensure we can find config.yaml relative to this script
script_dir = Path(__file__).parent
sys.path.insert(0, str(script_dir))

from data_loader import DynamoDBLabelLoader, ManifestLoader
from config import TrainingConfig

config = TrainingConfig(config_path=script_dir / "config.yaml")
manifest_loader = ManifestLoader(config.manifest_base_url, config.manifest_cache_dir)
loader = DynamoDBLabelLoader(config.dynamodb_table_name, config.aws_region, config.aws_profile)

labels = loader.load_all_labels(manifest_loader=manifest_loader, confidence_threshold=config.manifest_confidence_threshold)

# Show excluded examples if any
excluded = [l for l in labels if l.excluded]
if excluded:
    print(f"\nSample excluded labels (up to 10):")
    for l in excluded[:10]:
        print(f"  {l.image_id}: human_fs={l.frame_state}, model_fs={l.previous_model_frame_state} "
              f"({l.previous_model_frame_state_prob:.1%}), reason={l.exclusion_reason}")
else:
    print("\nNo labels were excluded (this is expected if model predictions mostly agree with humans)")
