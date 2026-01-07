"""
Backfill script to classify already-scraped images using the trained ONNX model.
Similar to BackfillRunner but for running ML inference on existing images.
"""
import logging
import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any
import time
import io

import boto3
from botocore.exceptions import ClientError
import onnxruntime as ort
import numpy as np
from PIL import Image
from torchvision import transforms

from config import TrainingConfig


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ONNXClassifier:
    """ONNX model wrapper for image classification."""
    
    def __init__(self, model_path: Path, config: TrainingConfig):
        """
        Initialize ONNX classifier.
        
        Args:
            model_path: Path to ONNX model file
            config: Training configuration
        """
        logger.info(f"Loading ONNX model from {model_path}")
        self.session = ort.InferenceSession(str(model_path))
        self.config = config
        
        # Get input/output names from model
        self.input_name = self.session.get_inputs()[0].name
        self.output_name = self.session.get_outputs()[0].name
        
        # Create image transformation pipeline
        self.transform = transforms.Compose([
            transforms.Resize((config.image_height, config.image_width)),
            transforms.ToTensor(),
            transforms.Normalize(
                mean=config.normalization_mean,
                std=config.normalization_std
            )
        ])
        
        logger.info(f"Model loaded. Input: {self.input_name}, Output: {self.output_name}")
    
    def classify(self, image: Image.Image) -> Dict[str, Any]:
        """
        Classify a single image.
        
        Args:
            image: PIL Image
        
        Returns:
            Dictionary with classification results
        """
        # Preprocess image
        img_tensor = self.transform(image)
        img_batch = img_tensor.unsqueeze(0).numpy()  # Add batch dimension
        
        # Run inference
        outputs = self.session.run([self.output_name], {self.input_name: img_batch})
        logits = outputs[0][0]  # Remove batch dimension
        
        # Apply softmax to get probabilities
        exp_logits = np.exp(logits - np.max(logits))
        probabilities = exp_logits / exp_logits.sum()
        
        # Get predicted class
        predicted_idx = int(np.argmax(probabilities))
        predicted_label = self.config.classification_labels[predicted_idx]
        confidence = float(probabilities[predicted_idx])
        
        return {
            'classification': predicted_label,
            'confidence': confidence,
            'probabilities': {
                label: float(probabilities[i])
                for i, label in enumerate(self.config.classification_labels)
            }
        }


class ClassificationBackfill:
    """Backfill classification results for existing images."""
    
    def __init__(
        self,
        bucket: str,
        cropped_prefix: str,
        analysis_prefix: str,
        model_version: int,
        region: str = 'us-west-2'
    ):
        """
        Initialize classification backfill.
        
        Args:
            bucket: S3 bucket name
            cropped_prefix: S3 prefix for cropped images
            analysis_prefix: S3 prefix for analysis results
            model_version: Model version number
            region: AWS region
        """
        self.s3_client = boto3.client('s3', region_name=region)
        self.bucket = bucket
        self.cropped_prefix = cropped_prefix
        self.analysis_prefix = analysis_prefix
        self.model_version = model_version
        self.region = region
    
    def list_images_in_range(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> List[str]:
        """
        List all images in S3 within date range.
        
        Args:
            start_date: Start datetime
            end_date: End datetime
        
        Returns:
            List of image IDs (YYYY/MM/DD/HHmm format)
        """
        logger.info(f"Listing images from {start_date} to {end_date}")
        
        image_ids = []
        current_date = start_date.date()
        end_date_only = end_date.date()
        
        while current_date <= end_date_only:
            # List images for this day
            prefix = f"{self.cropped_prefix}/{current_date.year}/{current_date.month:02d}/{current_date.day:02d}/"
            
            try:
                paginator = self.s3_client.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                    for obj in page.get('Contents', []):
                        key = obj['Key']
                        # Extract image ID from key (remove prefix and .jpg extension)
                        if key.endswith('.jpg'):
                            image_id = key[len(self.cropped_prefix) + 1:-4]
                            
                            # Parse timestamp and check if in range
                            parts = image_id.split('/')
                            if len(parts) == 4:
                                try:
                                    img_time = datetime(
                                        int(parts[0]),
                                        int(parts[1]),
                                        int(parts[2]),
                                        int(parts[3][:2]),
                                        int(parts[3][2:])
                                    )
                                    if start_date <= img_time <= end_date:
                                        image_ids.append(image_id)
                                except ValueError:
                                    continue
            
            except ClientError as e:
                logger.warning(f"Failed to list images for {current_date}: {e}")
            
            current_date += timedelta(days=1)
        
        logger.info(f"Found {len(image_ids)} images in range")
        return sorted(image_ids)
    
    def analysis_exists(self, image_id: str) -> bool:
        """Check if analysis already exists for an image."""
        key = f"{self.analysis_prefix}/v{self.model_version}/{image_id}.json"
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False
    
    def download_image(self, image_id: str) -> Image.Image:
        """Download image from S3."""
        key = f"{self.cropped_prefix}/{image_id}.jpg"
        response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
        image_data = response['Body'].read()
        image = Image.open(io.BytesIO(image_data))
        return image.convert('RGB')
    
    def save_analysis(self, image_id: str, analysis: Dict[str, Any]) -> None:
        """Save analysis result to S3."""
        key = f"{self.analysis_prefix}/v{self.model_version}/{image_id}.json"
        
        # Create analysis payload
        payload = {
            'imageId': image_id,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'modelVersion': self.model_version,
            'classification': analysis['classification'],
            'confidence': analysis['confidence'],
            'probabilities': analysis['probabilities']
        }
        
        # Upload to S3
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(payload, indent=2).encode('utf-8'),
            ContentType='application/json'
        )
    
    def process_images(
        self,
        image_ids: List[str],
        classifier: ONNXClassifier,
        skip_existing: bool = True
    ) -> Dict[str, int]:
        """
        Process a list of images.
        
        Args:
            image_ids: List of image IDs to process
            classifier: ONNX classifier instance
            skip_existing: Whether to skip images with existing analysis
        
        Returns:
            Dictionary with processing statistics
        """
        stats = {
            'total': len(image_ids),
            'processed': 0,
            'skipped': 0,
            'failed': 0
        }
        
        for i, image_id in enumerate(image_ids):
            try:
                # Skip if analysis already exists
                if skip_existing and self.analysis_exists(image_id):
                    stats['skipped'] += 1
                    if (i + 1) % 100 == 0:
                        logger.info(f"Progress: {i + 1}/{len(image_ids)} (skipped existing)")
                    continue
                
                # Download and classify image
                image = self.download_image(image_id)
                analysis = classifier.classify(image)
                
                # Save analysis
                self.save_analysis(image_id, analysis)
                stats['processed'] += 1
                
                # Log progress
                if (i + 1) % 100 == 0:
                    logger.info(
                        f"Progress: {i + 1}/{len(image_ids)} "
                        f"({stats['processed']} processed, {stats['skipped']} skipped)"
                    )
                
                # Rate limiting
                time.sleep(0.01)
            
            except Exception as e:
                logger.error(f"Failed to process {image_id}: {e}")
                stats['failed'] += 1
        
        return stats


def main(args):
    """Main backfill execution."""
    try:
        # Load configuration
        config = TrainingConfig(args.config)
        
        # Parse date range
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
        
        if args.start_time:
            start_hour, start_min = map(int, args.start_time.split(':'))
            start_date = start_date.replace(hour=start_hour, minute=start_min)
        
        if args.end_time:
            end_hour, end_min = map(int, args.end_time.split(':'))
            end_date = end_date.replace(hour=end_hour, minute=end_min)
        else:
            end_date = end_date.replace(hour=23, minute=59)
        
        logger.info(f"Classifying images from {start_date} to {end_date}")
        
        # Load ONNX model
        model_path = Path(args.model) if args.model else (config.models_dir / f"model_v{config.model_version}.onnx")
        classifier = ONNXClassifier(model_path, config)
        
        # Initialize backfill
        bucket_name = args.bucket if args.bucket else config.s3_bucket
        backfill = ClassificationBackfill(
            bucket_name,
            config.s3_cropped_images_prefix,
            config.s3_analysis_prefix,
            config.model_version,
            config.aws_region
        )
        
        # List images in range
        image_ids = backfill.list_images_in_range(start_date, end_date)
        
        if not image_ids:
            logger.warning("No images found in the specified range")
            return 0
        
        # Process images
        logger.info(f"Starting classification of {len(image_ids)} images...")
        stats = backfill.process_images(image_ids, classifier, skip_existing=not args.force)
        
        # Print summary
        logger.info("=" * 60)
        logger.info("Classification Backfill Complete")
        logger.info(f"Total images: {stats['total']}")
        logger.info(f"Processed: {stats['processed']}")
        logger.info(f"Skipped (already exists): {stats['skipped']}")
        logger.info(f"Failed: {stats['failed']}")
        logger.info("=" * 60)
        
        return 0
    
    except Exception as e:
        logger.exception(f"Backfill failed: {e}")
        return 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Backfill ML classification for existing images')
    parser.add_argument(
        '--start-date',
        type=str,
        required=True,
        help='Start date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        required=True,
        help='End date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--start-time',
        type=str,
        help='Start time (HH:MM), defaults to 00:00'
    )
    parser.add_argument(
        '--end-time',
        type=str,
        help='End time (HH:MM), defaults to 23:59'
    )
    parser.add_argument(
        '--model',
        type=str,
        help='Path to ONNX model (defaults to models/model_v{version}.onnx)'
    )
    parser.add_argument(
        '--bucket',
        type=str,
        help='S3 bucket name (overrides config.yaml)'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config.yaml',
        help='Path to config.yaml file'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Re-process images even if analysis already exists'
    )
    
    args = parser.parse_args()
    exit_code = main(args)
    import sys
    sys.exit(exit_code)
