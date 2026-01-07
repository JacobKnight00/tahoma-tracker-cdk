"""
Main training script: orchestrates data loading, model training, and ONNX export.
"""
import logging
import sys
from pathlib import Path
from typing import Tuple
import argparse
import json

import torch
from torch.utils.data import DataLoader, random_split
from torchvision.transforms import transforms

from config import TrainingConfig
from data_loader import DynamoDBLabelLoader, S3ImageLoader, load_training_data
from model import ImageClassifier, ModelTrainer, ONNXExporter, save_metadata
from s3_uploader import S3ModelUploader


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def setup_device() -> str:
    """Determine device to use for training."""
    if torch.cuda.is_available():
        device = 'cuda'
        logger.info(f"Using CUDA GPU: {torch.cuda.get_device_name(0)}")
    elif torch.backends.mps.is_available():
        device = 'mps'
        logger.info("Using Apple Metal (MPS) GPU")
    else:
        device = 'cpu'
        logger.info("No GPU available, using CPU (training will be slow)")
    
    return device


def create_data_transforms(config: TrainingConfig) -> transforms.Compose:
    """Create image transformation pipeline."""
    mean = config.normalization_mean
    std = config.normalization_std
    
    transform = transforms.Compose([
        transforms.Resize((config.image_height, config.image_width)),
        transforms.ToTensor(),
        transforms.Normalize(mean=mean, std=std)
    ])
    
    return transform


def split_dataset(
    dataset,
    train_split: float,
    random_seed: int
) -> Tuple:
    """
    Split dataset into training and validation sets.
    
    Args:
        dataset: Full dataset
        train_split: Fraction for training
        random_seed: Random seed for reproducibility
    
    Returns:
        Tuple of (train_dataset, val_dataset)
    """
    train_size = int(len(dataset) * train_split)
    val_size = len(dataset) - train_size
    
    train_dataset, val_dataset = random_split(
        dataset,
        [train_size, val_size],
        generator=torch.Generator().manual_seed(random_seed)
    )
    
    logger.info(
        f"Split dataset: {train_size} training, {val_size} validation "
        f"({train_split*100:.0f}% / {(1-train_split)*100:.0f}%)"
    )
    
    return train_dataset, val_dataset


def main(args) -> int:
    """
    Main training orchestration.
    
    Args:
        args: Command line arguments
    
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    try:
        # Load configuration
        logger.info(f"Loading configuration from {args.config}")
        config = TrainingConfig(args.config)
        
        # Set random seed
        torch.manual_seed(config.random_seed)
        
        # Setup device
        device = setup_device()
        
        # Create image transformations
        logger.info("Creating image transformations...")
        transform = create_data_transforms(config)
        
        # Load labels from DynamoDB
        logger.info("Initializing data loaders...")
        
        # Override bucket if provided in args
        bucket_name = args.bucket if args.bucket else config.s3_bucket
        
        # Create cache directory for faster loading
        cache_dir = config.logs_dir / "image_cache"
        
        dynamodb_loader = DynamoDBLabelLoader(config.table_name, config.aws_region)
        image_loader = S3ImageLoader(
            bucket_name, 
            config.s3_cropped_images_prefix, 
            config.aws_region,
            cache_dir=cache_dir
        )
        
        # Load complete dataset (will prefetch all images in parallel)
        logger.info("Loading dataset from DynamoDB and S3...")
        dataset = load_training_data(
            dynamodb_loader,
            image_loader,
            config.label_to_index,
            transform,
            prefetch=True
        )
        
        if len(dataset) == 0:
            logger.error("No data available for training")
            return 1
        
        # Split into train/validation
        train_dataset, val_dataset = split_dataset(
            dataset,
            config.train_split,
            config.random_seed
        )
        
        # Create data loaders
        # Use num_workers=0 to avoid multiprocessing pickle issues with boto3
        # Since images are cached locally, loading from disk is fast enough
        train_loader = DataLoader(
            train_dataset,
            batch_size=config.training_batch_size,
            shuffle=True,
            num_workers=0,
            pin_memory=True
        )
        val_loader = DataLoader(
            val_dataset,
            batch_size=config.training_batch_size,
            shuffle=False,
            num_workers=0,
            pin_memory=True
        )
        
        # Create model
        logger.info(f"Creating model with {len(config.classification_labels)} classes")
        model = ImageClassifier(len(config.classification_labels), pretrained=True)
        
        # Train model
        logger.info("Starting model training...")
        trainer = ModelTrainer(model, config.learning_rate, device)
        metrics = trainer.train(train_loader, val_loader, config.epochs, config.checkpoint_dir)
        
        # Load best checkpoint
        best_model_path = config.checkpoint_dir / "best_model.pt"
        if best_model_path.exists():
            logger.info(f"Loading best model from {best_model_path}")
            model.load_state_dict(torch.load(best_model_path, map_location=device))
        
        # Export to ONNX
        logger.info("Exporting model to ONNX format...")
        onnx_dir = config.models_dir / f"v{config.model_version}"
        onnx_dir.mkdir(parents=True, exist_ok=True)
        onnx_path = onnx_dir / "model.onnx"
        ONNXExporter.export_model(
            model,
            onnx_path,
            config.onnx_input_names,
            config.onnx_output_names,
            config.onnx_opset_version,
            (config.image_height, config.image_width),
            device
        )
        
        # Create and save metadata
        logger.info("Creating model metadata...")
        metadata = ONNXExporter.create_metadata(
            config.model_version,
            metrics,
            config.classification_labels,
            (config.image_height, config.image_width),
            {
                'mean': config.normalization_mean,
                'std': config.normalization_std
            },
            config.label_to_index
        )
        metadata_path = onnx_dir / "metadata.json"
        save_metadata(metadata, metadata_path)
        
        # Upload to S3
        if args.upload:
            logger.info("Uploading model and metadata to S3...")
            uploader = S3ModelUploader(bucket_name, config.aws_region)
            model_s3_path = config.get_s3_model_version_prefix(config.model_version)
            uploader.upload_model(
                onnx_path,
                metadata_path,
                model_s3_path,
                config.model_version,
                model_filename=onnx_path.name,
                metadata_filename=metadata_path.name
            )
            logger.info(f"Model uploaded to S3: {model_s3_path}")
        else:
            logger.info(f"Skipping S3 upload (use --upload flag to upload)")
            logger.info(f"Local model: {onnx_path}")
            logger.info(f"Local metadata: {metadata_path}")
        
        logger.info("Training complete!")
        logger.info(f"Model metrics: {metrics}")
        
        # Save missing images if any
        if dataset.failed_images:
            unique_missing = sorted(list(set(dataset.failed_images)))
            missing_images_path = config.logs_dir / "missing_images.json"
            with open(missing_images_path, 'w') as f:
                json.dump(unique_missing, f, indent=2)
            logger.warning(f"Found {len(unique_missing)} missing images. IDs saved to {missing_images_path}")
        
        return 0
    
    except Exception as e:
        logger.exception(f"Training failed with error: {e}")
        return 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Train image classification model')
    parser.add_argument(
        '--config',
        type=str,
        default='config.yaml',
        help='Path to config.yaml file (default: config.yaml)'
    )
    parser.add_argument(
        '--upload',
        action='store_true',
        help='Upload trained model and metadata to S3'
    )
    parser.add_argument(
        '--bucket',
        type=str,
        help='S3 bucket name (overrides config.yaml)'
    )
    
    args = parser.parse_args()
    exit_code = main(args)
    sys.exit(exit_code)
