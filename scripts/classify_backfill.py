#!/usr/bin/env python3
"""
Classification backfill script - runs frame_state and visibility models on existing cropped images.

Replaces the Java ClassificationBackfillRunner with equivalent Python implementation.

Usage:
    # Spot-check specific images
    python scripts/classify_backfill.py --bucket BUCKET --images 2024/12/25/1200,2024/12/25/1400 --version 2

    # Process a date range (by day)
    python scripts/classify_backfill.py --bucket BUCKET --start 2024-12-25 --end 2024-12-26 --version 2 --workers 4
"""

import argparse
import io
import json
import os
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any

import boto3
import numpy as np
import onnxruntime as ort
from PIL import Image

# Add training dir to path for config
sys.path.insert(0, str(Path(__file__).parent.parent / "training"))
from config import TrainingConfig

# Constants
CROPPED_PREFIX = "needle-cam/cropped-images"
PANOS_PREFIX = "needle-cam/panos"
ANALYSIS_PREFIX = "analysis"
LOCAL_TZ_OFFSET = -8  # Pacific time offset (simplified)
WINDOW_START_HOUR = 4
WINDOW_END_HOUR = 23


class OnnxClassifier:
    """ONNX model wrapper."""

    def __init__(self, model_path: str, labels: List[str]):
        self.session = ort.InferenceSession(model_path)
        self.input_name = self.session.get_inputs()[0].name
        self.labels = labels

    def classify(self, img_array: np.ndarray) -> Dict[str, Any]:
        """Run inference, return label and probabilities."""
        outputs = self.session.run(None, {self.input_name: img_array})
        logits = outputs[0][0]
        probs = np.exp(logits - np.max(logits))
        probs = probs / probs.sum()

        pred_idx = int(np.argmax(probs))
        return {
            "label": self.labels[pred_idx],
            "prob": float(probs[pred_idx]),
            "probabilities": {self.labels[i]: float(probs[i]) for i in range(len(self.labels))},
        }


def preprocess_image(image: Image.Image, size: int = 224) -> np.ndarray:
    """Preprocess image for ONNX model."""
    img = image.convert("RGB").resize((size, size))
    arr = np.array(img, dtype=np.float32) / 255.0
    mean = np.array([0.485, 0.456, 0.406])
    std = np.array([0.229, 0.224, 0.225])
    arr = (arr - mean) / std
    arr = arr.transpose(2, 0, 1)  # HWC -> CHW
    return arr[np.newaxis, ...].astype(np.float32)


def generate_image_ids(start_date: datetime, end_date: datetime, step_minutes: int = 10) -> List[str]:
    """Generate image IDs for date range (4am-11pm local time only)."""
    from datetime import timedelta
    ids = []
    current_date = start_date.date()
    end_date_only = end_date.date()

    while current_date <= end_date_only:
        # Generate times for this day within the window (4am-11pm local = 12pm-7am UTC next day)
        for hour in range(24):
            for minute in range(0, 60, step_minutes):
                # Check if local hour is in window
                local_hour = (hour + LOCAL_TZ_OFFSET) % 24
                if WINDOW_START_HOUR <= local_hour < WINDOW_END_HOUR:
                    ids.append(f"{current_date.year}/{current_date.month:02d}/{current_date.day:02d}/{hour:02d}{minute:02d}")
        current_date += timedelta(days=1)
    return ids


def load_model_from_s3(s3_client, bucket: str, key: str, labels: List[str], temp_dir: str) -> OnnxClassifier:
    """Download ONNX model (and .data file if exists) from S3 to temp dir and create classifier."""
    local_path = os.path.join(temp_dir, os.path.basename(key))
    print(f"Downloading s3://{bucket}/{key} -> {local_path}")
    s3_client.download_file(bucket, key, local_path)
    
    # Check for external data file
    data_key = key + ".data"
    data_path = local_path + ".data"
    try:
        s3_client.head_object(Bucket=bucket, Key=data_key)
        print(f"Downloading s3://{bucket}/{data_key} -> {data_path}")
        s3_client.download_file(bucket, data_key, data_path)
    except:
        pass  # No external data file
    
    return OnnxClassifier(local_path, labels)


def process_image(
    s3_client,
    bucket: str,
    image_id: str,
    version: str,
    frame_state_classifier: OnnxClassifier,
    visibility_classifier: OnnxClassifier,
    dry_run: bool,
) -> Dict[str, Any]:
    """Process a single image."""
    result = {"image_id": image_id, "skipped": False, "error": None}

    cropped_key = f"{CROPPED_PREFIX}/{image_id}.jpg"
    pano_key = f"{PANOS_PREFIX}/{image_id}.jpg"

    # Check if cropped image exists
    try:
        s3_client.head_object(Bucket=bucket, Key=cropped_key)
    except s3_client.exceptions.ClientError:
        result["skipped"] = True
        result["skip_reason"] = f"cropped image not found: {cropped_key}"
        return result

    try:
        # Download and preprocess image
        response = s3_client.get_object(Bucket=bucket, Key=cropped_key)
        image = Image.open(io.BytesIO(response["Body"].read()))
        img_array = preprocess_image(image)

        # Run frame_state classification
        fs_result = frame_state_classifier.classify(img_array)
        result["frame_state"] = fs_result["label"]
        result["frame_state_prob"] = fs_result["prob"]
        result["frame_state_probabilities"] = fs_result["probabilities"]

        # Run visibility only if frame_state is "good"
        if fs_result["label"] == "good":
            vis_result = visibility_classifier.classify(img_array)
            result["visibility"] = vis_result["label"]
            result["visibility_prob"] = vis_result["prob"]
            result["visibility_probabilities"] = vis_result["probabilities"]

        if dry_run:
            result["analysis_key"] = f"{ANALYSIS_PREFIX}/{version}/{image_id}.json (dry run)"
            return result

        # Build and save analysis JSON
        analysis = {
            "image_id": image_id,
            "frame_state_probabilities": result["frame_state_probabilities"],
            "frame_state_model_version": version,
            "visibility_model_version": version,
            "cropped_s3_key": cropped_key,
            "pano_s3_key": pano_key,
            "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        if "visibility_probabilities" in result:
            analysis["visibility_probabilities"] = result["visibility_probabilities"]

        analysis_key = f"{ANALYSIS_PREFIX}/{version}/{image_id}.json"
        s3_client.put_object(
            Bucket=bucket,
            Key=analysis_key,
            Body=json.dumps(analysis, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        result["analysis_key"] = analysis_key

    except Exception as e:
        result["error"] = str(e)

    return result


def print_result(result: Dict[str, Any]):
    """Print classification result."""
    print(f"\nProcessing {result['image_id']}...")

    if result.get("skipped"):
        print(f"  SKIPPED: {result.get('skip_reason')}")
        return

    if result.get("error"):
        print(f"  ERROR: {result['error']}")
        return

    fs = result.get("frame_state")
    print(f"  Frame State: {fs} ({result.get('frame_state_prob', 0):.2f})")
    if result.get("frame_state_probabilities"):
        probs = ", ".join(f"{k}={v:.2f}" for k, v in result["frame_state_probabilities"].items())
        print(f"    {probs}")

    if fs == "good" and result.get("visibility"):
        print(f"  Visibility: {result['visibility']} ({result.get('visibility_prob', 0):.2f})")
        if result.get("visibility_probabilities"):
            probs = ", ".join(f"{k}={v:.2f}" for k, v in result["visibility_probabilities"].items())
            print(f"    {probs}")
    elif fs != "good":
        print("  Visibility: (skipped - frame not GOOD)")

    print(f"  → Analysis written to {result.get('analysis_key')}")


def main():
    parser = argparse.ArgumentParser(description="Classification backfill for existing cropped images")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--images", help="Comma-separated image IDs (e.g., 2024/12/25/1200,2024/12/25/1400)")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--version", default="v2", help="Model version (default: v2)")
    parser.add_argument("--workers", type=int, default=1, help="Number of parallel workers")
    parser.add_argument("--profile", help="AWS profile name")
    parser.add_argument("--region", default="us-west-2", help="AWS region")
    parser.add_argument("--dry-run", action="store_true", help="Don't write analysis files")
    args = parser.parse_args()

    if not args.images and not (args.start and args.end):
        print("Error: Either --images or both --start and --end are required")
        sys.exit(1)

    # Create S3 client
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    s3_client = session.client("s3")

    # Load config for labels
    config_path = Path(__file__).parent.parent / "training" / "config.yaml"
    config = TrainingConfig(str(config_path))

    # Load models from S3 to temp directory
    version = args.version if args.version.startswith("v") else f"v{args.version}"
    fs_key = f"models/{version}/frame_state_{version}.onnx"
    vis_key = f"models/{version}/visibility_{version}.onnx"

    with tempfile.TemporaryDirectory() as temp_dir:
        frame_state_classifier = load_model_from_s3(s3_client, args.bucket, fs_key, config.get_frame_state_labels(), temp_dir)
        visibility_classifier = load_model_from_s3(s3_client, args.bucket, vis_key, config.get_visibility_labels(), temp_dir)

        # Get image IDs
        if args.images:
            image_ids = [img.strip() for img in args.images.split(",")]
        else:
            start = datetime.strptime(args.start, "%Y-%m-%d")
            end = datetime.strptime(args.end, "%Y-%m-%d")
            image_ids = generate_image_ids(start, end)
            print(f"Generated {len(image_ids)} candidate timestamps from {args.start} to {args.end}")

        # Process images
        processed, skipped, errors = 0, 0, 0

        if args.workers <= 1:
            for image_id in image_ids:
                result = process_image(
                    s3_client, args.bucket, image_id, version,
                    frame_state_classifier, visibility_classifier, args.dry_run
                )
                print_result(result)
                if result.get("error"):
                    errors += 1
                elif result.get("skipped"):
                    skipped += 1
                else:
                    processed += 1
        else:
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = {
                    executor.submit(
                        process_image, s3_client, args.bucket, img_id, version,
                        frame_state_classifier, visibility_classifier, args.dry_run
                    ): img_id
                    for img_id in image_ids
                }
                for future in as_completed(futures):
                    result = future.result()
                    print_result(result)
                    if result.get("error"):
                        errors += 1
                    elif result.get("skipped"):
                        skipped += 1
                    else:
                        processed += 1

        print(f"\nSummary: {processed} processed, {skipped} skipped, {errors} errors")


if __name__ == "__main__":
    main()
