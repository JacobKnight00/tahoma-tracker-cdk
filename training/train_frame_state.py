#!/usr/bin/env python3
"""
Train frame state classifier (4 classes: good, dark, bad, off_target)

Uses ALL labeled images from DynamoDB.
Exports to ONNX format for Java inference.

Usage:
    python train_frame_state.py --bucket <bucket-name>
"""

import argparse
import logging
import sys
from pathlib import Path

import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from torchvision import transforms

from config import TrainingConfig
from data_loader import DynamoDBLabelLoader, S3ImageLoader, TrainingDataset
from model import ImageClassifier, ModelTrainer, ONNXExporter
from s3_uploader import S3ModelUploader


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def setup_device():
    """Detect and return the best available device."""
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
    """Create data augmentation and normalization transforms."""
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


def filter_frame_state_labels(labels):
    """Filter labels for frame state training (all labels with frameState)."""
    return [label for label in labels if label.frame_state]


def main():
    parser = argparse.ArgumentParser(description='Train frame state classifier')
    parser.add_argument('--bucket', help='S3 bucket name (overrides config.yaml)')
    parser.add_argument('--config', default='config.yaml', help='Config file path')
    parser.add_argument('--upload', action='store_true', help='Upload exported model to S3')
    args = parser.parse_args()
    
    # Load configuration
    logger.info(f"Loading configuration from {args.config}")
    config = TrainingConfig(args.config)
    
    # Use bucket from args if provided, otherwise from config
    bucket = args.bucket if args.bucket else config.s3_bucket
    logger.info(f"Using S3 bucket: {bucket}")
    
    # Setup device
    device = setup_device()
    
    # Load labels from DynamoDB
    logger.info("Loading labels from DynamoDB...")
    label_loader = DynamoDBLabelLoader(
        table_name=config.dynamodb_table_name,
        region=config.aws_region
    )
    all_labels = label_loader.load_all_labels()
    
    # Filter for frame state training (all labels)
    frame_state_data = filter_frame_state_labels(all_labels)
    logger.info(f"Found {len(frame_state_data)} images for frame state training")
    
    # Create image loader with local cache
    cache_dir = Path(config.output_logs_dir) / 'image_cache'
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    image_loader = S3ImageLoader(
        bucket=bucket,
        prefix=config.s3_cropped_images_prefix,
        region=config.aws_region,
        cache_dir=str(cache_dir)
    )
    
    # Create datasets
    train_transform, val_transform = create_data_transforms(config)
    
    # Get frame state label mapping
    frame_state_labels = config.get_frame_state_labels()
    label_to_idx = config.get_frame_state_label_to_index()
    num_classes = len(frame_state_labels)
    
    logger.info(f"Frame state classes: {frame_state_labels}")
    logger.info(f"Number of classes: {num_classes}")
    
    # Prefetch all images once to populate cache and filter out missing assets
    prefetch_dataset = TrainingDataset(
        labels=frame_state_data,
        image_loader=image_loader,
        label_to_index=label_to_idx,
        transform=None,
        prefetch=True,
        label_field='frame_state'
    )
    
    if prefetch_dataset.failed_images:
        logger.warning(
            "Skipped %d images that failed to download (details in memory cache).",
            len(prefetch_dataset.failed_images)
        )
    
    available_labels = prefetch_dataset.labels
    logger.info(f"Successfully loaded {len(available_labels)} images after prefetch/filtering")
    
    if len(available_labels) == 0:
        logger.error("No valid images found. Exiting.")
        return 1
    
    # Split into train and validation using deterministic seed
    train_size = int(config.training_train_split * len(available_labels))
    val_size = len(available_labels) - train_size
    
    generator = torch.Generator().manual_seed(config.training_random_seed)
    indices = torch.randperm(len(available_labels), generator=generator).tolist()
    train_indices = indices[:train_size]
    val_indices = indices[train_size:]
    
    train_labels = [available_labels[i] for i in train_indices]
    val_labels = [available_labels[i] for i in val_indices]
    
    # Create datasets with the appropriate transforms (prefetch skipped since cache is warm)
    train_dataset = TrainingDataset(
        labels=train_labels,
        image_loader=image_loader,
        label_to_index=label_to_idx,
        transform=train_transform,
        prefetch=False,
        label_field='frame_state'
    )
    
    val_dataset = TrainingDataset(
        labels=val_labels,
        image_loader=image_loader,
        label_to_index=label_to_idx,
        transform=val_transform,
        prefetch=False,
        label_field='frame_state'
    )
    
    logger.info(f"Train set: {len(train_dataset)} images")
    logger.info(f"Validation set: {len(val_dataset)} images")
    
    # Create data loaders
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
    
    # Create model
    model = ImageClassifier(num_classes=num_classes)
    model = model.to(device)
    
    # Create trainer (criterion and optimizer are created internally)
    trainer = ModelTrainer(
        model=model,
        learning_rate=config.training_learning_rate,
        device=device
    )
    
    # Train model
    logger.info(f"Starting training for {config.training_epochs} epochs...")
    best_val_acc = 0.0
    best_val_loss = float('inf')
    best_epoch = 0
    
    for epoch in range(config.training_epochs):
        train_loss = trainer.train_epoch(train_loader)
        val_loss, val_acc = trainer.validate(val_loader)
        
        logger.info(f"Epoch {epoch + 1}/{config.training_epochs}")
        logger.info(f"  Train Loss: {train_loss:.4f}")
        logger.info(f"  Val Loss: {val_loss:.4f}, Val Acc: {val_acc*100:.2f}%")
        
        # Save best model
        if val_acc > best_val_acc:
            best_val_acc = val_acc
            best_val_loss = val_loss
            best_epoch = epoch + 1
            checkpoint_path = config.output_checkpoint_dir / 'best_frame_state_model.pt'
            torch.save({
                'epoch': epoch,
                'model_state_dict': model.state_dict(),
                'optimizer_state_dict': trainer.optimizer.state_dict(),
                'val_acc': val_acc,
                'val_loss': val_loss,
            }, checkpoint_path)
            logger.info(f"  New best model saved! Val Acc: {val_acc*100:.2f}%")
    
    logger.info(f"\nTraining complete. Best validation accuracy: {best_val_acc*100:.2f}%")
    
    # Export to ONNX
    logger.info("Exporting model to ONNX...")
    onnx_filename = f"frame_state_v{config.training_frame_state_model_version}.onnx"
    onnx_path = config.output_models_dir / f"v{config.training_frame_state_model_version}" / onnx_filename
    onnx_path.parent.mkdir(parents=True, exist_ok=True)
    
    from model import ONNXExporter, save_metadata
    
    ONNXExporter.export_model(
        model=model,
        output_path=onnx_path,
        input_names=config.onnx_input_names,
        output_names=config.onnx_output_names,
        opset_version=config.onnx_opset_version,
        image_size=(config.image_height, config.image_width),
        device=str(device)
    )
    
    # Create metadata
    metadata = ONNXExporter.create_metadata(
        model_version=config.training_frame_state_model_version,
        metrics={
            'best_val_accuracy': best_val_acc,
            'best_val_loss': best_val_loss if best_val_loss != float('inf') else None,
            'final_train_loss': train_loss,
            'final_val_loss': val_loss,
            'final_val_accuracy': val_acc,
            'best_epoch': best_epoch or None
        },
        classification_labels=frame_state_labels,
        image_size=(config.image_height, config.image_width),
        normalization=config.image_normalization,
        label_to_index=label_to_idx
    )
    
    metadata_path = onnx_path.with_suffix('.onnx.json')
    save_metadata(metadata, metadata_path)

    if args.upload:
        bucket_name = bucket
        s3_prefix = config.get_s3_model_version_prefix(config.training_frame_state_model_version)
        logger.info(f"Uploading frame state model to s3://{bucket_name}/{s3_prefix}")
        uploader = S3ModelUploader(bucket_name, config.aws_region)
        uploader.upload_model(
            onnx_path,
            metadata_path,
            s3_prefix,
            config.training_frame_state_model_version,
            model_filename=onnx_filename,
            metadata_filename=metadata_path.name
        )
        logger.info("Frame state model upload complete")
    else:
        logger.info("Skipping S3 upload (use --upload to upload artifacts)")
    
    logger.info(f"Model exported to: {onnx_path}")
    logger.info(f"Metadata saved to: {metadata_path}")
    logger.info("Training complete!")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
