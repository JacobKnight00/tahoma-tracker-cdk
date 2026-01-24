#!/usr/bin/env python3
"""
Train image classification models for Tahoma Tracker.

Tasks:
  frame_state - 4 classes (good, dark, bad, off_target), uses all labeled images
  visibility  - 3 classes (out, partially_out, not_out), uses only "good" frame images

Exports trained models to ONNX format for Java inference.

Usage:
    python train.py --task frame_state
    python train.py --task visibility
    python train.py --task frame_state --upload
"""

import argparse
import logging
import sys
from pathlib import Path
from dataclasses import dataclass
from typing import List, Callable

import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from torchvision import transforms

from config import TrainingConfig
from data_loader import DynamoDBLabelLoader, S3ImageLoader, TrainingDataset, LabelData
from model import ImageClassifier, ModelTrainer, ONNXExporter, save_metadata
from s3_uploader import S3ModelUploader


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class TaskConfig:
    """Configuration for a specific training task."""
    name: str
    label_field: str
    labels: List[str]
    label_to_index: dict
    model_version: int
    checkpoint_name: str
    onnx_name: str
    filter_fn: Callable[[List[LabelData]], List[LabelData]]


def setup_device():
    """Detect and return the best available device (MPS for Apple Silicon, CUDA, or CPU)."""
    if torch.cuda.is_available():
        device = torch.device('cuda')
        logger.info(f"Using CUDA GPU: {torch.cuda.get_device_name(0)}")
    elif torch.backends.mps.is_available():
        device = torch.device('mps')
        logger.info("Using Apple Metal GPU (MPS)")
    else:
        device = torch.device('cpu')
        logger.info("Using CPU (this will be slow)")
    return device


def create_data_transforms(config: TrainingConfig):
    """Create training (with augmentation) and validation transforms."""
    train_transform = transforms.Compose([
        transforms.Resize((config.image_width, config.image_height)),
        transforms.RandomHorizontalFlip(),
        transforms.RandomRotation(10),
        transforms.ColorJitter(brightness=0.2, contrast=0.2),
        transforms.ToTensor(),
        transforms.Normalize(
            mean=config.image_normalization['mean'],
            std=config.image_normalization['std']
        )
    ])
    
    val_transform = transforms.Compose([
        transforms.Resize((config.image_width, config.image_height)),
        transforms.ToTensor(),
        transforms.Normalize(
            mean=config.image_normalization['mean'],
            std=config.image_normalization['std']
        )
    ])
    
    return train_transform, val_transform


def filter_frame_state_labels(labels: List[LabelData]) -> List[LabelData]:
    """Filter for frame_state training: all labels with a frame_state value."""
    return [label for label in labels if label.frame_state]


def filter_visibility_labels(labels: List[LabelData]) -> List[LabelData]:
    """Filter for visibility training: only 'good' frames with visibility labels."""
    return [
        label for label in labels 
        if label.frame_state == "good" and label.visibility
    ]


def get_task_config(task: str, config: TrainingConfig) -> TaskConfig:
    """Get task-specific configuration."""
    if task == 'frame_state':
        return TaskConfig(
            name='frame_state',
            label_field='frame_state',
            labels=config.get_frame_state_labels(),
            label_to_index=config.get_frame_state_label_to_index(),
            model_version=config.training_frame_state_model_version,
            checkpoint_name='best_frame_state_model.pt',
            onnx_name=f"frame_state_v{config.training_frame_state_model_version}.onnx",
            filter_fn=filter_frame_state_labels
        )
    elif task == 'visibility':
        return TaskConfig(
            name='visibility',
            label_field='visibility',
            labels=config.get_visibility_labels(),
            label_to_index=config.get_visibility_label_to_index(),
            model_version=config.training_visibility_model_version,
            checkpoint_name='best_visibility_model.pt',
            onnx_name=f"visibility_v{config.training_visibility_model_version}.onnx",
            filter_fn=filter_visibility_labels
        )
    else:
        raise ValueError(f"Unknown task: {task}")


def train_model(config: TrainingConfig, task_config: TaskConfig, bucket: str, upload: bool) -> int:
    """Train a single model for the given task."""
    logger.info(f"=== Training {task_config.name} model ===")
    logger.info(f"Classes: {task_config.labels}")
    
    device = setup_device()
    
    # Load labels from DynamoDB
    logger.info("Loading labels from DynamoDB...")
    label_loader = DynamoDBLabelLoader(
        table_name=config.dynamodb_table_name,
        region=config.aws_region
    )
    all_labels = label_loader.load_all_labels()
    
    # Filter labels for this task
    filtered_labels = task_config.filter_fn(all_labels)
    logger.info(f"Found {len(filtered_labels)} images for {task_config.name} training")
    
    if len(filtered_labels) < 100:
        logger.warning(f"Only {len(filtered_labels)} images found. Consider labeling more data.")
    
    # Create image loader with local cache
    image_loader = S3ImageLoader(
        bucket=bucket,
        prefix=config.s3_cropped_images_prefix,
        region=config.aws_region,
        cache_dir=str(config.cache_images_dir)
    )
    
    train_transform, val_transform = create_data_transforms(config)
    num_classes = len(task_config.labels)
    
    # Prefetch images to populate cache
    prefetch_dataset = TrainingDataset(
        labels=filtered_labels,
        image_loader=image_loader,
        label_to_index=task_config.label_to_index,
        transform=None,
        prefetch=True,
        label_field=task_config.label_field
    )
    
    if prefetch_dataset.failed_images:
        logger.warning(f"Skipped {len(prefetch_dataset.failed_images)} images that failed to download")
    
    available_labels = prefetch_dataset.labels
    logger.info(f"Successfully loaded {len(available_labels)} images")
    
    if len(available_labels) == 0:
        logger.error("No valid images found. Exiting.")
        return 1
    
    # Split into train/validation
    train_size = int(config.training_train_split * len(available_labels))
    generator = torch.Generator().manual_seed(config.training_random_seed)
    indices = torch.randperm(len(available_labels), generator=generator).tolist()
    
    train_labels = [available_labels[i] for i in indices[:train_size]]
    val_labels = [available_labels[i] for i in indices[train_size:]]
    
    train_dataset = TrainingDataset(
        labels=train_labels,
        image_loader=image_loader,
        label_to_index=task_config.label_to_index,
        transform=train_transform,
        prefetch=False,
        label_field=task_config.label_field
    )
    
    val_dataset = TrainingDataset(
        labels=val_labels,
        image_loader=image_loader,
        label_to_index=task_config.label_to_index,
        transform=val_transform,
        prefetch=False,
        label_field=task_config.label_field
    )
    
    logger.info(f"Train set: {len(train_dataset)}, Validation set: {len(val_dataset)}")
    
    train_loader = DataLoader(
        train_dataset,
        batch_size=config.training_batch_size,
        shuffle=True,
        num_workers=config.training_num_workers
    )
    val_loader = DataLoader(
        val_dataset,
        batch_size=config.training_batch_size,
        shuffle=False,
        num_workers=config.training_num_workers
    )
    
    # Create and train model
    model = ImageClassifier(num_classes=num_classes).to(device)
    trainer = ModelTrainer(model=model, learning_rate=config.training_learning_rate, device=device)
    
    logger.info(f"Training for {config.training_epochs} epochs...")
    best_val_acc = 0.0
    best_val_loss = float('inf')
    best_epoch = 0
    
    for epoch in range(config.training_epochs):
        train_loss = trainer.train_epoch(train_loader)
        val_loss, val_acc = trainer.validate(val_loader)
        
        logger.info(f"Epoch {epoch + 1}/{config.training_epochs} - "
                    f"Train Loss: {train_loss:.4f}, Val Loss: {val_loss:.4f}, Val Acc: {val_acc*100:.2f}%")
        
        if val_acc > best_val_acc:
            best_val_acc = val_acc
            best_val_loss = val_loss
            best_epoch = epoch + 1
            checkpoint_path = config.output_checkpoints_dir / task_config.checkpoint_name
            torch.save({
                'epoch': epoch,
                'model_state_dict': model.state_dict(),
                'optimizer_state_dict': trainer.optimizer.state_dict(),
                'val_acc': val_acc,
                'val_loss': val_loss,
            }, checkpoint_path)
            logger.info(f"  New best model saved! Val Acc: {val_acc*100:.2f}%")
    
    logger.info(f"Training complete. Best accuracy: {best_val_acc*100:.2f}% (epoch {best_epoch})")
    
    # Export to ONNX
    logger.info("Exporting to ONNX...")
    onnx_path = config.output_models_dir / f"v{task_config.model_version}" / task_config.onnx_name
    onnx_path.parent.mkdir(parents=True, exist_ok=True)
    
    ONNXExporter.export_model(
        model=model,
        output_path=onnx_path,
        input_names=config.onnx_input_names,
        output_names=config.onnx_output_names,
        opset_version=config.onnx_opset_version,
        image_size=(config.image_height, config.image_width),
        device=str(device)
    )
    
    metadata = ONNXExporter.create_metadata(
        model_version=task_config.model_version,
        metrics={
            'best_val_accuracy': best_val_acc,
            'best_val_loss': best_val_loss if best_val_loss != float('inf') else None,
            'final_train_loss': train_loss,
            'final_val_loss': val_loss,
            'final_val_accuracy': val_acc,
            'best_epoch': best_epoch
        },
        classification_labels=task_config.labels,
        image_size=(config.image_height, config.image_width),
        normalization=config.image_normalization,
        label_to_index=task_config.label_to_index
    )
    
    metadata_path = onnx_path.with_suffix('.onnx.json')
    save_metadata(metadata, metadata_path)
    
    logger.info(f"Model exported to: {onnx_path}")
    logger.info(f"Metadata saved to: {metadata_path}")
    
    if upload:
        s3_prefix = config.get_s3_model_version_prefix(task_config.model_version)
        logger.info(f"Uploading to s3://{bucket}/{s3_prefix}")
        uploader = S3ModelUploader(bucket, config.aws_region)
        uploader.upload_model(
            onnx_path, metadata_path, s3_prefix, task_config.model_version,
            model_filename=task_config.onnx_name,
            metadata_filename=metadata_path.name
        )
        logger.info("Upload complete")
    
    return 0


def main():
    parser = argparse.ArgumentParser(
        description='Train Tahoma Tracker image classification models',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Tasks:
  frame_state  Train frame state classifier (good, dark, bad, off_target)
  visibility   Train visibility classifier (out, partially_out, not_out)

Examples:
  python train.py --task frame_state
  python train.py --task visibility --upload
"""
    )
    parser.add_argument('--task', required=True, choices=['frame_state', 'visibility'],
                        help='Training task (required)')
    parser.add_argument('--upload', action='store_true', help='Upload model to S3 after training')
    args = parser.parse_args()
    
    config = TrainingConfig()
    bucket = config.s3_bucket
    if not bucket:
        logger.error("No S3 bucket configured. Set s3.bucket in config.local.yaml")
        return 1
    
    logger.info(f"Using S3 bucket: {bucket}")
    
    task_config = get_task_config(args.task, config)
    return train_model(config, task_config, bucket, args.upload)


if __name__ == '__main__':
    sys.exit(main())
