"""
Compare v1 and v2 model predictions against human labels.
Helps validate model improvements before committing to a full backfill.
"""
import argparse
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from collections import defaultdict
import io

import boto3
import onnxruntime as ort
import numpy as np
from PIL import Image
from torchvision import transforms

from config import TrainingConfig
from data_loader import DynamoDBLabelLoader, LabelData

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ModelRunner:
    """Run inference with an ONNX model."""
    
    def __init__(self, model_path: Path, labels: List[str], image_size: int = 224):
        self.session = ort.InferenceSession(str(model_path))
        self.input_name = self.session.get_inputs()[0].name
        self.output_name = self.session.get_outputs()[0].name
        self.labels = labels
        self.transform = transforms.Compose([
            transforms.Resize((image_size, image_size)),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
        ])
    
    def predict(self, image: Image.Image) -> tuple[str, float]:
        """Returns (predicted_label, confidence)."""
        img_tensor = self.transform(image).unsqueeze(0).numpy()
        outputs = self.session.run([self.output_name], {self.input_name: img_tensor})
        logits = outputs[0][0]
        probs = np.exp(logits - np.max(logits))
        probs = probs / probs.sum()
        idx = int(np.argmax(probs))
        return self.labels[idx], float(probs[idx])


def download_image(s3_client, bucket: str, prefix: str, image_id: str) -> Optional[Image.Image]:
    """Download image from S3."""
    key = f"{prefix}/{image_id}.jpg"
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return Image.open(io.BytesIO(response['Body'].read())).convert('RGB')
    except Exception as e:
        logger.warning(f"Failed to download {image_id}: {e}")
        return None


def filter_labels_by_date(labels: List[LabelData], start_date: str, end_date: str) -> List[LabelData]:
    """Filter labels to a date range (YYYY-MM-DD format)."""
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d').replace(hour=23, minute=59)
    
    filtered = []
    for label in labels:
        parts = label.image_id.split('/')
        if len(parts) == 4:
            try:
                img_time = datetime(int(parts[0]), int(parts[1]), int(parts[2]),
                                    int(parts[3][:2]), int(parts[3][2:]))
                if start <= img_time <= end:
                    filtered.append(label)
            except ValueError:
                continue
    return filtered


def main():
    parser = argparse.ArgumentParser(description='Compare v1 and v2 model predictions against labels')
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--task', choices=['frame_state', 'visibility', 'both'], default='both')
    parser.add_argument('--limit', type=int, help='Limit number of images to compare')
    parser.add_argument('--output', help='Output JSON file for detailed results')
    args = parser.parse_args()
    
    config = TrainingConfig('config.yaml')
    
    # Load labels from DynamoDB
    logger.info("Loading labels from DynamoDB...")
    loader = DynamoDBLabelLoader(config.dynamodb_table_name, config.aws_region, config.aws_profile)
    all_labels = [l for l in loader.load_all_labels() if not l.excluded]
    
    # Filter by date range
    labels = filter_labels_by_date(all_labels, args.start_date, args.end_date)
    logger.info(f"Found {len(labels)} labeled images in date range")
    
    if args.limit:
        labels = labels[:args.limit]
        logger.info(f"Limited to {len(labels)} images")
    
    if not labels:
        logger.error("No labels found in date range")
        return
    
    # Load models
    models_dir = Path('models')
    runners = {}
    
    tasks = ['frame_state', 'visibility'] if args.task == 'both' else [args.task]
    
    for task in tasks:
        task_labels = config.get_frame_state_labels() if task == 'frame_state' else config.get_visibility_labels()
        v1_path = models_dir / 'v1' / f'{task}_v1.onnx'
        v2_path = models_dir / 'v2' / f'{task}_v2.onnx'
        
        if v1_path.exists() and v2_path.exists():
            runners[task] = {
                'v1': ModelRunner(v1_path, task_labels),
                'v2': ModelRunner(v2_path, task_labels)
            }
            logger.info(f"Loaded {task} models (v1 and v2)")
        else:
            logger.warning(f"Missing models for {task}: v1={v1_path.exists()}, v2={v2_path.exists()}")
    
    if not runners:
        logger.error("No models loaded")
        return
    
    # Setup S3
    session = boto3.Session(profile_name=config.aws_profile) if config.aws_profile else boto3.Session()
    s3 = session.client('s3', region_name=config.aws_region)
    
    # Compare predictions
    results = {task: {'v1_correct': 0, 'v2_correct': 0, 'both_correct': 0, 
                      'v1_only': 0, 'v2_only': 0, 'neither': 0, 'total': 0,
                      'details': []} for task in runners}
    
    for i, label in enumerate(labels):
        if (i + 1) % 50 == 0:
            logger.info(f"Processing {i + 1}/{len(labels)}...")
        
        image = download_image(s3, config.s3_bucket, config.s3_cropped_images_prefix, label.image_id)
        if not image:
            continue
        
        for task, task_runners in runners.items():
            human_label = label.frame_state if task == 'frame_state' else label.visibility
            if not human_label:
                continue
            
            v1_pred, v1_conf = task_runners['v1'].predict(image)
            v2_pred, v2_conf = task_runners['v2'].predict(image)
            
            v1_correct = v1_pred == human_label
            v2_correct = v2_pred == human_label
            
            results[task]['total'] += 1
            if v1_correct and v2_correct:
                results[task]['both_correct'] += 1
            elif v1_correct:
                results[task]['v1_only'] += 1
            elif v2_correct:
                results[task]['v2_only'] += 1
            else:
                results[task]['neither'] += 1
            
            results[task]['v1_correct'] += int(v1_correct)
            results[task]['v2_correct'] += int(v2_correct)
            
            # Store details for disagreements
            if v1_pred != v2_pred or not v1_correct:
                results[task]['details'].append({
                    'image_id': label.image_id,
                    'human': human_label,
                    'v1': v1_pred, 'v1_conf': round(v1_conf, 3),
                    'v2': v2_pred, 'v2_conf': round(v2_conf, 3)
                })
    
    # Print summary
    print("\n" + "=" * 70)
    print("MODEL COMPARISON RESULTS")
    print("=" * 70)
    
    for task, r in results.items():
        if r['total'] == 0:
            continue
        print(f"\n{task.upper()} ({r['total']} images)")
        print("-" * 40)
        v1_acc = r['v1_correct'] / r['total'] * 100
        v2_acc = r['v2_correct'] / r['total'] * 100
        print(f"  v1 accuracy: {v1_acc:.1f}% ({r['v1_correct']}/{r['total']})")
        print(f"  v2 accuracy: {v2_acc:.1f}% ({r['v2_correct']}/{r['total']})")
        print(f"  Improvement: {v2_acc - v1_acc:+.1f}%")
        print(f"\n  Both correct:    {r['both_correct']:4d}")
        print(f"  Only v1 correct: {r['v1_only']:4d}")
        print(f"  Only v2 correct: {r['v2_only']:4d}")
        print(f"  Neither correct: {r['neither']:4d}")
        
        # Show some disagreement examples
        disagreements = [d for d in r['details'] if d['v1'] != d['v2']]
        if disagreements:
            print(f"\n  Sample disagreements (showing up to 5):")
            for d in disagreements[:5]:
                marker = "✓" if d['v2'] == d['human'] else "✗"
                print(f"    {d['image_id']}: human={d['human']}, v1={d['v1']}({d['v1_conf']}), v2={d['v2']}({d['v2_conf']}) {marker}")
    
    # Save detailed results
    if args.output:
        # Remove details for JSON serialization size
        output_data = {
            'date_range': {'start': args.start_date, 'end': args.end_date},
            'results': {task: {k: v for k, v in r.items() if k != 'details'} for task, r in results.items()},
            'disagreements': {task: r['details'] for task, r in results.items()}
        }
        with open(args.output, 'w') as f:
            json.dump(output_data, f, indent=2)
        print(f"\nDetailed results saved to {args.output}")


if __name__ == '__main__':
    main()
