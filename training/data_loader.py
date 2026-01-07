"""
Data loading utilities for training: fetch labels from DynamoDB and images from S3.
"""
import logging
from typing import List, Tuple, Dict, Any
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

import boto3
from botocore.exceptions import ClientError
import torch
from torch.utils.data import Dataset
from PIL import Image
import io


logger = logging.getLogger(__name__)


class LabelData:
    """Simple data class for label information."""
    
    def __init__(self, image_id: str, frame_state: str, visibility: str = None):
        self.image_id = image_id
        self.frame_state = frame_state
        self.visibility = visibility
    
    def __repr__(self) -> str:
        return f"LabelData({self.image_id}, {self.frame_state}, {self.visibility})"


class DynamoDBLabelLoader:
    """Loads labeled images from DynamoDB."""
    
    def __init__(self, table_name: str, region: str = 'us-west-2'):
        """
        Initialize DynamoDB loader.
        
        Args:
            table_name: DynamoDB table name
            region: AWS region
        """
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.table = self.dynamodb.Table(table_name)
        self.table_name = table_name
        logger.info(f"Initialized DynamoDBLabelLoader for table: {table_name}")
    
    def load_all_labels(self, batch_size: int = 25) -> List[LabelData]:
        """
        Load all labeled images from DynamoDB using pagination.
        
        Args:
            batch_size: Batch size for pagination
        
        Returns:
            List of LabelData objects
        """
        labels = []
        try:
            paginator = self.dynamodb.meta.client.get_paginator('scan')
            page_iterator = paginator.paginate(
                TableName=self.table_name,
                PaginationConfig={'PageSize': batch_size}
            )
            
            for page in page_iterator:
                for item in page.get('Items', []):
                    label = self._parse_label_item(item)
                    if label:
                        labels.append(label)
            
            logger.info(f"Loaded {len(labels)} labels from DynamoDB")
            return labels
        
        except ClientError as e:
            logger.error(f"Failed to load labels from DynamoDB: {e}")
            raise
    
    def _parse_label_item(self, item: Dict[str, Any]) -> LabelData:
        """
        Parse a DynamoDB item into LabelData.
        
        Args:
            item: DynamoDB item (already deserialized)
        
        Returns:
            LabelData object or None if invalid
        """
        try:
            return LabelData(
                image_id=item.get('imageId'),
                frame_state=item.get('frameState'),
                visibility=item.get('visibility')
            )
        except (KeyError, TypeError) as e:
            logger.warning(f"Failed to parse label item: {e}")
            return None


class S3ImageLoader:
    """Loads cropped images from S3 with caching and bulk download support."""
    
    def __init__(self, bucket: str, prefix: str, region: str = 'us-west-2', cache_dir = None):
        """
        Initialize S3 image loader.
        
        Args:
            bucket: S3 bucket name
            prefix: S3 prefix for cropped images
            region: AWS region
            cache_dir: Local directory to cache downloaded images (optional, can be str or Path)
        """
        # Don't initialize S3 client here - lazy init for multiprocessing compatibility
        self._s3_client = None
        self.bucket = bucket
        self.prefix = prefix
        self.region = region
        self.cache_dir = Path(cache_dir) if cache_dir else None
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized S3ImageLoader for s3://{bucket}/{prefix}")
    
    @property
    def s3_client(self):
        """Lazy initialization of S3 client for multiprocessing compatibility."""
        if self._s3_client is None:
            self._s3_client = boto3.client('s3', region_name=self.region)
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
