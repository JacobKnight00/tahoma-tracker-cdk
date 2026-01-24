#!/usr/bin/env python3
"""
Generate classifications and manifests for a new model version.

Reads existing cropped images from S3, runs both classifiers with the specified
model version, and writes versioned analysis files and manifests.

This does NOT modify existing version data - it creates new versioned files:
  - analysis/{version}/YYYY/MM/DD/HHMM.json
  - manifests/daily/{version}/YYYY/MM/DD.json
  - manifests/monthly/{version}/YYYY/MM.json

Usage:
    python backfill_model.py --model-version v2 --start 2024-01-01 --end 2024-12-31
"""

import argparse
import json
import logging
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Any

import boto3
import numpy as np
import onnxruntime as ort
from PIL import Image

from config import TrainingConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ONNXClassifier:
    """ONNX model for classification."""

    def __init__(self, session: ort.InferenceSession, labels: List[str], input_size: tuple, mean: List[float], std: List[float]):
        self.session = session
        self.labels = labels
        self.input_name = session.get_inputs()[0].name
        self.output_name = session.get_outputs()[0].name
        self.input_size = input_size
        self.mean = np.array(mean, dtype=np.float32).reshape(1, 3, 1, 1)
        self.std = np.array(std, dtype=np.float32).reshape(1, 3, 1, 1)

    def classify(self, image: Image.Image) -> Dict[str, Any]:
        img = image.convert('RGB').resize(self.input_size)
        arr = np.array(img, dtype=np.float32).transpose(2, 0, 1) / 255.0
        arr = (arr.reshape(1, 3, *self.input_size) - self.mean) / self.std

        outputs = self.session.run([self.output_name], {self.input_name: arr})
        logits = outputs[0][0]
        probs = np.exp(logits - np.max(logits))
        probs = probs / probs.sum()

        idx = int(np.argmax(probs))
        return {
            'label': self.labels[idx],
            'confidence': float(probs[idx]),
            'probabilities': {label: float(probs[i]) for i, label in enumerate(self.labels)}
        }


def load_model_from_s3(s3_client, bucket: str, key: str) -> ort.InferenceSession:
    """Load ONNX model from S3."""
    logger.info(f"Loading model from s3://{bucket}/{key}")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    model_bytes = response['Body'].read()
    return ort.InferenceSession(model_bytes)


def load_image_from_s3(s3_client, bucket: str, key: str) -> Optional[Image.Image]:
    """Load image from S3."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return Image.open(BytesIO(response['Body'].read()))
    except s3_client.exceptions.NoSuchKey:
        return None
    except Exception as e:
        logger.warning(f"Failed to load {key}: {e}")
        return None


def generate_image_ids(start_date: datetime, end_date: datetime, step_minutes: int = 10) -> List[str]:
    """Generate image IDs for date range (4am-11pm local time window)."""
    ids = []
    current = start_date
    while current <= end_date:
        hour = current.hour
        if 4 <= hour < 23:
            ids.append(current.strftime('%Y/%m/%d/%H%M'))
        current += timedelta(minutes=step_minutes)
    return ids


def process_image(
    s3_client,
    bucket: str,
    cropped_prefix: str,
    image_id: str,
    frame_state_classifier: ONNXClassifier,
    visibility_classifier: ONNXClassifier,
) -> Optional[Dict[str, Any]]:
    """Process a single image through both classifiers."""
    cropped_key = f"{cropped_prefix}/{image_id}.jpg"
    image = load_image_from_s3(s3_client, bucket, cropped_key)
    if image is None:
        return None

    # Run frame state classifier
    fs_result = frame_state_classifier.classify(image)

    # Run visibility classifier only if frame is "good"
    vis_result = None
    if fs_result['label'] == 'good':
        vis_result = visibility_classifier.classify(image)

    return {
        'image_id': image_id,
        'frame_state': fs_result['label'],
        'frame_state_prob': fs_result['confidence'],
        'frame_state_probabilities': fs_result['probabilities'],
        'visibility': vis_result['label'] if vis_result else None,
        'visibility_prob': vis_result['confidence'] if vis_result else None,
        'visibility_probabilities': vis_result['probabilities'] if vis_result else None,
        'cropped_s3_key': cropped_key,
    }


def write_analysis(s3_client, bucket: str, analysis_prefix: str, version: str, result: Dict[str, Any]):
    """Write analysis JSON to S3."""
    image_id = result['image_id']
    key = f"{analysis_prefix}/{version}/{image_id}.json"

    analysis = {
        'image_id': image_id,
        'frame_state_probabilities': result['frame_state_probabilities'],
        'visibility_probabilities': result['visibility_probabilities'],
        'frame_state_model_version': version,
        'visibility_model_version': version,
        'cropped_s3_key': result['cropped_s3_key'],
        'updated_at': datetime.utcnow().isoformat() + 'Z',
    }

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(analysis, indent=2),
        ContentType='application/json'
    )


def build_daily_manifest(results: List[Dict[str, Any]], date_str: str) -> Dict[str, Any]:
    """Build daily manifest from results."""
    entries = []
    for r in sorted(results, key=lambda x: x['image_id']):
        entries.append({
            'time': r['image_id'].split('/')[-1],
            'frame_state': r['frame_state'],
            'frame_state_prob': r['frame_state_prob'],
            'visibility': r['visibility'],
            'visibility_prob': r['visibility_prob'],
        })

    # Calculate summary
    total = len(entries)
    good_count = sum(1 for e in entries if e['frame_state'] == 'good')
    visible_count = sum(1 for e in entries if e['visibility'] == 'out')

    return {
        'date': date_str,
        'entries': entries,
        'summary': {
            'total_images': total,
            'good_frames': good_count,
            'visible_count': visible_count,
        },
        'generated_at': datetime.utcnow().isoformat() + 'Z',
    }


def build_monthly_manifest(daily_summaries: Dict[str, Dict], month_str: str) -> Dict[str, Any]:
    """Build monthly manifest from daily summaries."""
    days = {}
    for day, summary in sorted(daily_summaries.items()):
        days[day] = summary

    total_images = sum(d.get('total_images', 0) for d in days.values())
    good_frames = sum(d.get('good_frames', 0) for d in days.values())
    visible_count = sum(d.get('visible_count', 0) for d in days.values())

    return {
        'month': month_str,
        'days': days,
        'stats': {
            'total_images': total_images,
            'good_frames': good_frames,
            'visible_count': visible_count,
            'days_with_data': len(days),
        },
        'generated_at': datetime.utcnow().isoformat() + 'Z',
    }


def write_manifests(s3_client, bucket: str, manifests_prefix: str, version: str, results: List[Dict[str, Any]]):
    """Write daily and monthly manifests."""
    # Group by date
    by_date = defaultdict(list)
    for r in results:
        date_str = '/'.join(r['image_id'].split('/')[:3])
        by_date[date_str].append(r)

    # Write daily manifests and collect summaries for monthly
    monthly_summaries = defaultdict(dict)
    for date_str, date_results in by_date.items():
        parts = date_str.split('/')
        year, month, day = parts[0], parts[1], parts[2]
        month_str = f"{year}/{month}"

        daily = build_daily_manifest(date_results, date_str.replace('/', '-'))
        daily_key = f"{manifests_prefix}/daily/{version}/{year}/{month}/{day}.json"

        s3_client.put_object(
            Bucket=bucket,
            Key=daily_key,
            Body=json.dumps(daily, indent=2),
            ContentType='application/json'
        )
        logger.info(f"Wrote daily manifest: {daily_key}")

        monthly_summaries[month_str][day] = daily['summary']

    # Write monthly manifests
    for month_str, daily_sums in monthly_summaries.items():
        parts = month_str.split('/')
        year, month = parts[0], parts[1]

        monthly = build_monthly_manifest(daily_sums, month_str.replace('/', '-'))
        monthly_key = f"{manifests_prefix}/monthly/{version}/{year}/{month}.json"

        s3_client.put_object(
            Bucket=bucket,
            Key=monthly_key,
            Body=json.dumps(monthly, indent=2),
            ContentType='application/json'
        )
        logger.info(f"Wrote monthly manifest: {monthly_key}")


def main():
    parser = argparse.ArgumentParser(
        description='Generate classifications and manifests for a new model version',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python backfill_model.py --model-version v2 --start 2024-01-01 --end 2024-01-31
  python backfill_model.py --model-version v2 --start 2024-01-01 --end 2024-12-31 --workers 4
"""
    )
    parser.add_argument('--model-version', required=True, help='Model version to use (e.g., v2)')
    parser.add_argument('--start', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--workers', type=int, default=4, help='Parallel workers (default: 4)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without writing')
    args = parser.parse_args()

    config = TrainingConfig()
    bucket = config.s3_bucket
    if not bucket:
        logger.error("No S3 bucket configured. Set s3.bucket in config.local.yaml")
        return 1

    version = args.model_version
    start_date = datetime.strptime(args.start, '%Y-%m-%d')
    end_date = datetime.strptime(args.end, '%Y-%m-%d').replace(hour=23, minute=59)

    logger.info(f"Generating {version} classifications from {args.start} to {args.end}")
    logger.info(f"Using bucket: {bucket}")

    # Load models from S3
    s3_client = boto3.client('s3', region_name=config.aws_region)

    models_prefix = config.s3_models_prefix
    frame_state_key = f"{models_prefix}/{version}/frame_state_{version}.onnx"
    visibility_key = f"{models_prefix}/{version}/visibility_{version}.onnx"

    fs_session = load_model_from_s3(s3_client, bucket, frame_state_key)
    vis_session = load_model_from_s3(s3_client, bucket, visibility_key)

    input_size = (config.image_width, config.image_height)
    mean = config.normalization_mean
    std = config.normalization_std

    frame_state_classifier = ONNXClassifier(
        fs_session, config.get_frame_state_labels(), input_size, mean, std
    )
    visibility_classifier = ONNXClassifier(
        vis_session, config.get_visibility_labels(), input_size, mean, std
    )

    # Generate image IDs
    image_ids = generate_image_ids(start_date, end_date)
    logger.info(f"Processing {len(image_ids)} timestamps")

    if args.dry_run:
        logger.info("DRY RUN - would process:")
        for img_id in image_ids[:10]:
            logger.info(f"  {img_id}")
        if len(image_ids) > 10:
            logger.info(f"  ... and {len(image_ids) - 10} more")
        return 0

    # Process images
    results = []
    cropped_prefix = config.s3_cropped_images_prefix

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                process_image, s3_client, bucket, cropped_prefix, img_id,
                frame_state_classifier, visibility_classifier
            ): img_id
            for img_id in image_ids
        }

        for future in as_completed(futures):
            img_id = futures[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
                    if len(results) % 100 == 0:
                        logger.info(f"Processed {len(results)} images...")
            except Exception as e:
                logger.error(f"Error processing {img_id}: {e}")

    logger.info(f"Successfully processed {len(results)} images")

    if not results:
        logger.warning("No images processed")
        return 0

    # Write analysis files
    logger.info("Writing analysis files...")
    for result in results:
        write_analysis(s3_client, bucket, config.s3_analysis_prefix, version, result)

    # Write manifests
    logger.info("Writing manifests...")
    write_manifests(s3_client, bucket, config.s3_manifests_prefix, version, results)

    logger.info("Done!")
    return 0


if __name__ == '__main__':
    sys.exit(main())
