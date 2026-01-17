#!/usr/bin/env python3
"""
Train visibility classifier (3 classes: out, partially_out, not_out)

Usage:
    python analyze_dataset.py --version 2   # Run first to generate class weights
    python train_visibility.py --version 2
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from collections import Counter

import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from torchvision import transforms

from config import TrainingConfig
from data_loader import DynamoDBLabelLoader, S3ImageLoader, TrainingDataset, ManifestLoader
from model import ImageClassifier, ModelTrainer, ONNXExporter, save_metadata
from s3_uploader import S3ModelUploader


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

VISIBILITY_LABELS = ['out', 'partially_out', 'not_out']


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


def load_class_weights(version: int, models_dir: Path, device) -> torch.Tensor:
    """Load class weights from dataset_analysis.json if available."""
    analysis_path = models_dir / f'v{version}' / 'dataset_analysis.json'
    if not analysis_path.exists():
        logger.warning(f"No dataset_analysis.json found at {analysis_path}")
        logger.warning("Run 'python analyze_dataset.py --version %d' first for class balancing", version)
        return None
    
    with open(analysis_path) as f:
        analysis = json.load(f)
    
    weights_dict = analysis.get('visibility', {}).get('weights', {})
    if not weights_dict:
        return None
    
    weights = [weights_dict.get(label, 1.0) for label in VISIBILITY_LABELS]
    logger.info(f"Loaded class weights: {dict(zip(VISIBILITY_LABELS, weights))}")
    return torch.tensor(weights, dtype=torch.float32, device=device)


def compute_confusion_matrix(model, data_loader, num_classes, device):
    """Compute confusion matrix on a dataset."""
    model.eval()
    matrix = torch.zeros(num_classes, num_classes, dtype=torch.int64)
    
    with torch.no_grad():
        for images, labels in data_loader:
            images = images.to(device)
            outputs = model(images)
            _, preds = torch.max(outputs, 1)
            for t, p in zip(labels.view(-1), preds.cpu().view(-1)):
                matrix[t.long(), p.long()] += 1
    
    return matrix


def log_confusion_matrix(matrix, labels):
    """Log confusion matrix and per-class metrics. Returns metrics dict."""
    logger.info("\nConfusion Matrix:")
    header = "               " + " ".join(f"{l[:12]:>12}" for l in labels)
    logger.info(header)
    
    for i, row_label in enumerate(labels):
        row = matrix[i].tolist()
        row_str = " ".join(f"{v:>12}" for v in row)
        logger.info(f"{row_label:>15} {row_str}")
    
    # Per-class precision, recall, F1
    logger.info("\nPer-class metrics:")
    per_class = {}
    for i, label in enumerate(labels):
        tp = matrix[i, i].item()
        fp = matrix[:, i].sum().item() - tp
        fn = matrix[i, :].sum().item() - tp
        
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
        
        logger.info(f"  {label:15} P={precision:.3f} R={recall:.3f} F1={f1:.3f}")
        per_class[label] = {'precision': round(precision, 3), 'recall': round(recall, 3), 'f1': round(f1, 3)}
    
    return per_class


def filter_visibility_labels(labels):
    """Filter labels for visibility training (only good frames with visibility)."""
    return [l for l in labels if l.frame_state == "good" and l.visibility and not l.excluded]


def main():
    parser = argparse.ArgumentParser(description='Train visibility classifier')
    parser.add_argument('--version', type=int, required=True, help='Model version (e.g., 2)')
    parser.add_argument('--bucket', help='S3 bucket name (overrides config.yaml)')
    parser.add_argument('--config', default='config.yaml', help='Config file path')
    parser.add_argument('--upload', action='store_true', help='Upload exported model to S3')
    parser.add_argument('--no-weights', action='store_true', help='Disable class weighting')
    args = parser.parse_args()
    
    version = args.version
    
    # Load configuration
    logger.info(f"Loading configuration from {args.config}")
    config = TrainingConfig(args.config)
    
    bucket = args.bucket if args.bucket else config.s3_bucket
    logger.info(f"Using S3 bucket: {bucket}")
    logger.info(f"Training model version: v{version}")
    
    # Set random seeds for reproducibility
    torch.manual_seed(config.training_random_seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(config.training_random_seed)
    
    device = setup_device()
    
    # Load class weights if available
    class_weights = None
    if not args.no_weights:
        class_weights = load_class_weights(version, config.output_models_dir, device)
    
    # Initialize manifest loader for filtering
    manifest_loader = ManifestLoader(
        base_url=config.manifest_base_url,
        cache_dir=config.manifest_cache_dir
    )
    
    # Load labels from DynamoDB with filtering
    logger.info("Loading labels from DynamoDB...")
    label_loader = DynamoDBLabelLoader(
        table_name=config.dynamodb_table_name,
        region=config.aws_region,
        profile=config.aws_profile
    )
    all_labels = label_loader.load_all_labels(
        manifest_loader=manifest_loader,
        confidence_threshold=config.manifest_confidence_threshold
    )
    
    visibility_data = filter_visibility_labels(all_labels)
    logger.info(f"Found {len(visibility_data)} 'good' images for visibility training")
    
    # Log class distribution
    dist = Counter(l.visibility for l in visibility_data)
    logger.info(f"Class distribution: {dict(dist)}")
    
    if len(visibility_data) < 100:
        logger.warning(f"Warning: Only {len(visibility_data)} images. Consider labeling more data.")
    
    # Create image loader with local cache
    cache_dir = Path(config.output_logs_dir) / 'image_cache'
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    image_loader = S3ImageLoader(
        bucket=bucket,
        prefix=config.s3_cropped_images_prefix,
        region=config.aws_region,
        cache_dir=str(cache_dir),
        profile=config.aws_profile
    )
    
    train_transform, val_transform = create_data_transforms(config)
    label_to_idx = {l: i for i, l in enumerate(VISIBILITY_LABELS)}
    num_classes = len(VISIBILITY_LABELS)
    
    # Prefetch all images
    prefetch_dataset = TrainingDataset(
        labels=visibility_data,
        image_loader=image_loader,
        label_to_index=label_to_idx,
        transform=None,
        prefetch=True,
        label_field='visibility'
    )
    
    available_labels = prefetch_dataset.labels
    logger.info(f"Successfully loaded {len(available_labels)} images")
    
    if len(available_labels) == 0:
        logger.error("No valid images found. Exiting.")
        return 1
    
    # Split into train and validation
    train_size = int(config.training_train_split * len(available_labels))
    generator = torch.Generator().manual_seed(config.training_random_seed)
    indices = torch.randperm(len(available_labels), generator=generator).tolist()
    
    train_labels = [available_labels[i] for i in indices[:train_size]]
    val_labels = [available_labels[i] for i in indices[train_size:]]
    
    train_dataset = TrainingDataset(
        labels=train_labels, image_loader=image_loader, label_to_index=label_to_idx,
        transform=train_transform, prefetch=False, label_field='visibility'
    )
    val_dataset = TrainingDataset(
        labels=val_labels, image_loader=image_loader, label_to_index=label_to_idx,
        transform=val_transform, prefetch=False, label_field='visibility'
    )
    
    logger.info(f"Train set: {len(train_dataset)}, Validation set: {len(val_dataset)}")
    
    train_loader = DataLoader(train_dataset, batch_size=config.training_batch_size, shuffle=True, num_workers=0)
    val_loader = DataLoader(val_dataset, batch_size=config.training_batch_size, shuffle=False, num_workers=0)
    
    # Create model and trainer with weighted loss
    model = ImageClassifier(num_classes=num_classes).to(device)
    criterion = nn.CrossEntropyLoss(weight=class_weights)
    
    trainer = ModelTrainer(model=model, learning_rate=config.training_learning_rate, device=device)
    trainer.criterion = criterion  # Override with weighted loss
    
    # Training loop
    logger.info(f"Starting training for {config.training_epochs} epochs...")
    best_val_acc = 0.0
    best_val_loss = float('inf')
    best_epoch = 0
    
    output_dir = config.output_models_dir / f'v{version}'
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path = output_dir / 'best_visibility_model.pt'
    
    for epoch in range(config.training_epochs):
        train_loss = trainer.train_epoch(train_loader)
        val_loss, val_acc = trainer.validate(val_loader)
        
        logger.info(f"Epoch {epoch + 1}/{config.training_epochs} - Train Loss: {train_loss:.4f}, Val Loss: {val_loss:.4f}, Val Acc: {val_acc*100:.2f}%")
        
        if val_acc > best_val_acc:
            best_val_acc = val_acc
            best_val_loss = val_loss
            best_epoch = epoch + 1
            torch.save({'epoch': epoch, 'model_state_dict': model.state_dict(), 'val_acc': val_acc}, checkpoint_path)
            logger.info(f"  New best model saved! Val Acc: {val_acc*100:.2f}%")
    
    logger.info(f"\nTraining complete. Best validation accuracy: {best_val_acc*100:.2f}% at epoch {best_epoch}")
    
    # Load best model for evaluation and export
    checkpoint = torch.load(checkpoint_path, weights_only=True)
    model.load_state_dict(checkpoint['model_state_dict'])
    
    # Confusion matrix on validation set
    conf_matrix = compute_confusion_matrix(model, val_loader, num_classes, device)
    per_class_metrics = log_confusion_matrix(conf_matrix, VISIBILITY_LABELS)
    
    # Save metrics for export_only.py
    metrics = {
        'best_epoch': best_epoch,
        'best_val_accuracy': round(best_val_acc, 4),
        'confusion_matrix': conf_matrix.tolist(),
        'per_class_metrics': per_class_metrics
    }
    metrics_path = output_dir / 'visibility_metrics.json'
    with open(metrics_path, 'w') as f:
        json.dump(metrics, f, indent=2)
    logger.info(f"Saved metrics to {metrics_path}")

    # Export to ONNX
    onnx_filename = f"visibility_v{version}.onnx"
    onnx_path = output_dir / onnx_filename
    
    ONNXExporter.export_model(
        model=model, output_path=onnx_path,
        input_names=config.onnx_input_names, output_names=config.onnx_output_names,
        opset_version=config.onnx_opset_version,
        image_size=(config.image_height, config.image_width), device=str(device)
    )
    
    # Save metadata with confusion matrix
    metadata = ONNXExporter.create_metadata(
        model_version=version,
        metrics={
            'best_val_accuracy': best_val_acc,
            'best_val_loss': best_val_loss,
            'best_epoch': best_epoch,
            'confusion_matrix': conf_matrix.tolist(),
        },
        classification_labels=VISIBILITY_LABELS,
        image_size=(config.image_height, config.image_width),
        normalization=config.image_normalization,
        label_to_index=label_to_idx
    )
    
    metadata_path = onnx_path.with_suffix('.onnx.json')
    save_metadata(metadata, metadata_path)
    
    if args.upload:
        s3_prefix = f"{config.s3_models_prefix}/v{version}"
        logger.info(f"Uploading to s3://{bucket}/{s3_prefix}")
        uploader = S3ModelUploader(bucket, config.aws_region)
        uploader.upload_model(onnx_path, metadata_path, s3_prefix, version,
                              model_filename=onnx_filename, metadata_filename=metadata_path.name)
    
    logger.info(f"Model exported to: {onnx_path}")
    logger.info(f"Metadata saved to: {metadata_path}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
