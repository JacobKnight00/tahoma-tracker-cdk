"""
Data loading utilities for training: fetch labels from DynamoDB and images from S3.
"""
import logging
from typing import List, Tuple, Dict, Any, Optional
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
import torch
from torch.utils.data import Dataset
from PIL import Image
import io
import requests


logger = logging.getLogger(__name__)


class LabelData:
    """Simple data class for label information."""
    
    def __init__(
        self, 
        image_id: str, 
        frame_state: str, 
        visibility: str = None,
        previous_model_frame_state: str = None,
        previous_model_frame_state_prob: float = None,
        previous_model_visibility: str = None,
        previous_model_visibility_prob: float = None,
        excluded: bool = False,
        exclusion_reason: str = None
    ):
        self.image_id = image_id
        self.frame_state = frame_state
        self.visibility = visibility
        self.previous_model_frame_state = previous_model_frame_state
        self.previous_model_frame_state_prob = previous_model_frame_state_prob
        self.previous_model_visibility = previous_model_visibility
        self.previous_model_visibility_prob = previous_model_visibility_prob
        self.excluded = excluded
        self.exclusion_reason = exclusion_reason
    
    def __repr__(self) -> str:
        return f"LabelData({self.image_id}, {self.frame_state}, {self.visibility}, excluded={self.excluded})"


class ManifestLoader:
    """Loads daily manifests from CloudFront with local caching."""
    
    def __init__(self, base_url: str, cache_dir: str = None):
        """
        Initialize manifest loader.
        
        Args:
            base_url: Base URL for manifests (e.g., "https://example.com/manifests/daily")
            cache_dir: Local directory to cache manifests
        """
        self.base_url = base_url.rstrip('/')
        self.cache_dir = Path(cache_dir) if cache_dir else None
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized ManifestLoader with base_url: {base_url}")
    
    def get_manifest(self, date_str: str) -> Optional[Dict[str, Dict]]:
        """
        Get manifest for a specific date, indexed by time.
        
        Args:
            date_str: Date string in format "YYYY/MM/DD"
        
        Returns:
            Dictionary mapping time to prediction dict, or None if not found
        """
        # Check cache first
        if self.cache_dir:
            cache_path = self.cache_dir / f"{date_str.replace('/', '_')}.json"
            if cache_path.exists():
                try:
                    with open(cache_path, 'r') as f:
                        manifest = json.load(f)
                    return self._index_by_time(manifest)
                except Exception as e:
                    logger.warning(f"Failed to load cached manifest for {date_str}: {e}")
        
        # Download from CloudFront
        url = f"{self.base_url}/{date_str}.json"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            manifest = response.json()
            
            # Save to cache
            if self.cache_dir:
                cache_path = self.cache_dir / f"{date_str.replace('/', '_')}.json"
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                with open(cache_path, 'w') as f:
                    json.dump(manifest, f)
            
            return self._index_by_time(manifest)
        except Exception as e:
            logger.debug(f"Failed to fetch manifest for {date_str}: {e}")
            return None
    
    def _index_by_time(self, manifest: Dict) -> Dict[str, Dict]:
        """Index manifest images by time for quick lookup."""
        indexed = {}
        for image in manifest.get('images', []):
            time = image.get('time')
            if time:
                indexed[time] = image
        return indexed
    
    def get_prediction(self, image_id: str) -> Optional[Dict]:
        """
        Get prediction for a specific image.
        
        Args:
            image_id: Image ID in format "YYYY/MM/DD/HHMM"
        
        Returns:
            Prediction dict with frame_state, visibility, and probabilities, or None
        """
        parts = image_id.split('/')
        if len(parts) != 4:
            logger.warning(f"Invalid image_id format: {image_id}")
            return None
        
        date_str = '/'.join(parts[:3])  # "YYYY/MM/DD"
        time_str = parts[3]  # "HHMM"
        
        manifest = self.get_manifest(date_str)
        if not manifest:
            return None
        
        return manifest.get(time_str)


class DynamoDBLabelLoader:
    """Loads labeled images from DynamoDB."""
    
    def __init__(self, table_name: str, region: str = 'us-west-2', profile: str = None):
        """
        Initialize DynamoDB loader.
        
        Args:
            table_name: DynamoDB table name
            region: AWS region
            profile: Optional AWS profile name
        """
        session = boto3.Session(profile_name=profile) if profile else boto3.Session()
        self.dynamodb = session.resource('dynamodb', region_name=region)
        self.table = self.dynamodb.Table(table_name)
        self.table_name = table_name
        logger.info(f"Initialized DynamoDBLabelLoader for table: {table_name}")
    
    def load_all_labels(
        self, 
        batch_size: int = 25,
        manifest_loader: Optional[ManifestLoader] = None,
        confidence_threshold: float = 0.95
    ) -> List[LabelData]:
        """
        Load all labeled images from DynamoDB using pagination.
        Optionally filter out labels that conflict with high-confidence previous model predictions.
        
        Args:
            batch_size: Batch size for pagination
            manifest_loader: Optional ManifestLoader to fetch previous model predictions
            confidence_threshold: Confidence threshold for filtering (default 0.95)
        
        Returns:
            List of LabelData objects (including excluded ones for logging)
        """
        labels = []
        excluded_frame_state = []
        excluded_visibility = []
        
        try:
            paginator = self.dynamodb.meta.client.get_paginator('scan')
            page_iterator = paginator.paginate(
                TableName=self.table_name,
                PaginationConfig={'PageSize': batch_size}
            )
            
            for page in page_iterator:
                for item in page.get('Items', []):
                    label = self._parse_label_item(item, manifest_loader, confidence_threshold)
                    if label:
                        labels.append(label)
                        if label.excluded:
                            if 'frameState' in label.exclusion_reason:
                                excluded_frame_state.append(label)
                            else:
                                excluded_visibility.append(label)
            
            # Log statistics
            total = len(labels)
            excluded_total = len(excluded_frame_state) + len(excluded_visibility)
            included = total - excluded_total
            
            logger.info(f"Loaded {total} labels from DynamoDB")
            logger.info(f"  Included for training: {included}")
            logger.info(f"  Excluded (frameState mismatch): {len(excluded_frame_state)}")
            logger.info(f"  Excluded (visibility mismatch): {len(excluded_visibility)}")
            
            # Log sample exclusions
            if excluded_frame_state:
                logger.info("Sample frameState exclusions:")
                for label in excluded_frame_state[:5]:
                    logger.info(f"  {label.image_id}: {label.exclusion_reason}")
            
            if excluded_visibility:
                logger.info("Sample visibility exclusions:")
                for label in excluded_visibility[:5]:
                    logger.info(f"  {label.image_id}: {label.exclusion_reason}")
            
            return labels
        
        except ClientError as e:
            logger.error(f"Failed to load labels from DynamoDB: {e}")
            raise
    
    def _parse_label_item(
        self, 
        item: Dict[str, Any],
        manifest_loader: Optional[ManifestLoader] = None,
        confidence_threshold: float = 0.95
    ) -> Optional[LabelData]:
        """
        Parse a DynamoDB item into LabelData with optional filtering.
        
        Args:
            item: DynamoDB item (already deserialized)
            manifest_loader: Optional ManifestLoader for filtering
            confidence_threshold: Confidence threshold for filtering
        
        Returns:
            LabelData object or None if invalid
        """
        try:
            image_id = item.get('imageId')
            frame_state = item.get('frameState')
            visibility = item.get('visibility')
            
            # Initialize with no filtering
            excluded = False
            exclusion_reason = None
            prev_frame_state = None
            prev_frame_state_prob = None
            prev_visibility = None
            prev_visibility_prob = None
            
            # Apply filtering if manifest loader provided
            if manifest_loader:
                prediction = manifest_loader.get_prediction(image_id)
                if prediction:
                    prev_frame_state = prediction.get('frame_state')
                    prev_frame_state_prob = prediction.get('frame_state_prob')
                    prev_visibility = prediction.get('visibility')
                    prev_visibility_prob = prediction.get('visibility_prob')
                    
                    # Check frameState mismatch
                    if (prev_frame_state and 
                        prev_frame_state_prob and 
                        prev_frame_state_prob > confidence_threshold and
                        prev_frame_state != frame_state):
                        excluded = True
                        exclusion_reason = (
                            f"frameState mismatch: model={prev_frame_state}@{prev_frame_state_prob:.2%}, "
                            f"human={frame_state}"
                        )
                    
                    # Check visibility mismatch (only if both have frameState == "good")
                    elif (not excluded and
                          prev_visibility and 
                          visibility and
                          prev_frame_state == "good" and
                          frame_state == "good" and
                          prev_visibility_prob and
                          prev_visibility_prob > confidence_threshold and
                          prev_visibility != visibility):
                        excluded = True
                        exclusion_reason = (
                            f"visibility mismatch: model={prev_visibility}@{prev_visibility_prob:.2%}, "
                            f"human={visibility}"
                        )
            
            return LabelData(
                image_id=image_id,
                frame_state=frame_state,
                visibility=visibility,
                previous_model_frame_state=prev_frame_state,
                previous_model_frame_state_prob=prev_frame_state_prob,
                previous_model_visibility=prev_visibility,
                previous_model_visibility_prob=prev_visibility_prob,
                excluded=excluded,
                exclusion_reason=exclusion_reason
            )
        except (KeyError, TypeError) as e:
            logger.warning(f"Failed to parse label item: {e}")
            return None


class S3ImageLoader:
    """Loads cropped images from S3 with caching and bulk download support."""
    
    def __init__(self, bucket: str, prefix: str, region: str = 'us-west-2', cache_dir = None, profile: str = None):
        """
        Initialize S3 image loader.
        
        Args:
            bucket: S3 bucket name
            prefix: S3 prefix for cropped images
            region: AWS region
            cache_dir: Local directory to cache downloaded images (optional, can be str or Path)
            profile: Optional AWS profile name
        """
        # Don't initialize S3 client here - lazy init for multiprocessing compatibility
        self._s3_client = None
        self.bucket = bucket
        self.prefix = prefix
        self.region = region
        self.profile = profile
        self.cache_dir = Path(cache_dir) if cache_dir else None
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized S3ImageLoader for s3://{bucket}/{prefix}")
    
    @property
    def s3_client(self):
        """Lazy initialization of S3 client for multiprocessing compatibility."""
        if self._s3_client is None:
            session = boto3.Session(profile_name=self.profile) if self.profile else boto3.Session()
            self._s3_client = session.client('s3', region_name=self.region)
        return self._s3_client
    
    def bulk_download(self, image_ids: List[str], max_workers: int = 20) -> Dict[str, Path]:
        """
        Bulk download multiple images in parallel.
        
        Args:
            image_ids: List of image IDs to download
            max_workers: Number of parallel download threads
        
        Returns:
            Dictionary mapping image_id to local file path
        """
        if not self.cache_dir:
            raise ValueError("cache_dir must be set to use bulk_download")
        
        downloaded = {}
        
        def download_one(image_id: str) -> Tuple[str, Path]:
            local_path = self.cache_dir / f"{image_id.replace('/', '_')}.jpg"
            
            # Skip if already cached
            if local_path.exists():
                return (image_id, local_path)
            
            key = f"{self.prefix}/{image_id}.jpg"
            try:
                local_path.parent.mkdir(parents=True, exist_ok=True)
                self.s3_client.download_file(self.bucket, key, str(local_path))
                return (image_id, local_path)
            except ClientError as e:
                logger.debug(f"Failed to download {image_id}: {e}")
                return (image_id, None)
        
        logger.info(f"Bulk downloading {len(image_ids)} images with {max_workers} workers...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(download_one, img_id): img_id for img_id in image_ids}
            
            for i, future in enumerate(as_completed(futures)):
                image_id, local_path = future.result()
                if local_path:
                    downloaded[image_id] = local_path
                
                if (i + 1) % 100 == 0:
                    logger.info(f"Downloaded {i + 1}/{len(image_ids)} images...")
        
        logger.info(f"Bulk download complete: {len(downloaded)}/{len(image_ids)} succeeded")
        return downloaded
    
    def load_image(self, image_id: str) -> Image.Image:
        """
        Load a single image from S3 or cache.
        
        Args:
            image_id: Image ID (e.g., "2024/01/15/1430")
        
        Returns:
            PIL Image object
        
        Raises:
            ClientError if image not found
        """
        # Try cache first if enabled
        if self.cache_dir:
            cached_path = self.cache_dir / f"{image_id.replace('/', '_')}.jpg"
            if cached_path.exists():
                image = Image.open(cached_path)
                return image.convert('RGB')
        
        # Download from S3
        key = f"{self.prefix}/{image_id}.jpg"
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            image_data = response['Body'].read()
            image = Image.open(io.BytesIO(image_data))
            
            # Save to cache if enabled
            if self.cache_dir:
                cached_path = self.cache_dir / f"{image_id.replace('/', '_')}.jpg"
                cached_path.parent.mkdir(parents=True, exist_ok=True)
                with open(cached_path, 'wb') as f:
                    f.write(image_data)
            
            return image.convert('RGB')
        except ClientError as e:
            logger.error(f"Failed to load image from S3: s3://{self.bucket}/{key}: {e}")
            raise


class TrainingDataset(Dataset):
    """PyTorch Dataset for training with labels and images."""
    
    def __init__(
        self,
        labels: List[LabelData],
        image_loader: S3ImageLoader,
        label_to_index: Dict[str, int],
        transform=None,
        prefetch: bool = True,
        label_field: str = 'frame_state'
    ):
        """
        Initialize training dataset.
        
        Args:
            labels: List of LabelData objects
            image_loader: S3ImageLoader instance
            label_to_index: Mapping from classification label to index
            transform: Torchvision transforms to apply to images
            prefetch: Whether to prefetch all images before training
            label_field: Which field to use for labels ('frame_state' or 'visibility')
        """
        self.labels = labels
        self.image_loader = image_loader
        self.label_to_index = label_to_index
        self.transform = transform
        self.label_field = label_field
        self.failed_images = []
        
        # Prefetch all images if caching is enabled
        if prefetch and image_loader.cache_dir:
            logger.info("Prefetching all images to local cache...")
            image_ids = [label.image_id for label in labels]
            downloaded = image_loader.bulk_download(image_ids)
            
            # Filter out labels for images that failed to download
            original_count = len(labels)
            self.labels = [label for label in labels if label.image_id in downloaded]
            self.failed_images = [label.image_id for label in labels if label.image_id not in downloaded]
            
            if self.failed_images:
                logger.warning(
                    f"Excluded {len(self.failed_images)} images that failed to download. "
                    f"Training on {len(self.labels)}/{original_count} images."
                )
    
    def __len__(self) -> int:
        return len(self.labels)
    
    def __getitem__(self, idx: int) -> Tuple[torch.Tensor, int]:
        """
        Get a single training sample.
        
        Args:
            idx: Index in dataset
        
        Returns:
            Tuple of (image tensor, label index)
        """
        label_data = self.labels[idx]
        
        # Load image from cache (should always succeed since we filtered failed images)
        image = self.image_loader.load_image(label_data.image_id)
        
        if self.transform:
            image = self.transform(image)
        
        # Get the label value from the appropriate field
        label_value = getattr(label_data, self.label_field)
        label_idx = self.label_to_index[label_value]
        return image, label_idx


def load_training_data(
    dynamodb_loader: DynamoDBLabelLoader,
    image_loader: S3ImageLoader,
    label_to_index: Dict[str, int],
    transform=None,
    prefetch: bool = True
) -> TrainingDataset:
    """
    Load complete training dataset from DynamoDB and S3.
    
    Args:
        dynamodb_loader: DynamoDBLabelLoader instance
        image_loader: S3ImageLoader instance
        label_to_index: Mapping from classification label to index
        transform: Torchvision transforms
        prefetch: Whether to prefetch all images before training
    
    Returns:
        TrainingDataset instance
    """
    logger.info("Loading labels from DynamoDB...")
    labels = dynamodb_loader.load_all_labels()
    
    logger.info(f"Creating dataset with {len(labels)} labels")
    dataset = TrainingDataset(labels, image_loader, label_to_index, transform, prefetch)
    
    return dataset
