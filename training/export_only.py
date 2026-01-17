"""
Export a trained PyTorch checkpoint to ONNX format.
Use this script when you have a trained model checkpoint but need to re-export.
"""
import json
import logging
import sys
import argparse
from pathlib import Path

import torch

from config import TrainingConfig
from model import ImageClassifier, ONNXExporter, save_metadata
from s3_uploader import S3ModelUploader


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def _load_checkpoint_into_model(model: ImageClassifier, checkpoint_path: Path) -> None:
    """
    Load checkpoint supporting both raw state_dict and wrapped dictionaries.
    """
    checkpoint = torch.load(checkpoint_path, map_location='cpu')
    state_dict = checkpoint
    if isinstance(checkpoint, dict) and 'model_state_dict' in checkpoint:
        state_dict = checkpoint['model_state_dict']
    if isinstance(state_dict, dict) and all(k.startswith('module.') for k in state_dict.keys()):
        # Strip DistributedDataParallel/DataParallel prefix if present
        state_dict = {k.replace('module.', '', 1): v for k, v in state_dict.items()}
    model.load_state_dict(state_dict)


def main(args):
    """Export checkpoint to ONNX."""
    try:
        # Load configuration
        logger.info(f"Loading configuration from {args.config}")
        config = TrainingConfig(args.config)

        model_version = args.version
        task = args.task
        version_dir = config.output_models_dir / f"v{model_version}"
        
        if task == 'frame_state':
            labels = config.get_frame_state_labels()
            label_to_index = config.get_frame_state_label_to_index()
            default_checkpoint = version_dir / 'best_frame_state_model.pt'
            onnx_filename = f"frame_state_v{model_version}.onnx"
        else:
            labels = config.get_visibility_labels()
            label_to_index = config.get_visibility_label_to_index()
            default_checkpoint = version_dir / 'best_visibility_model.pt'
            onnx_filename = f"visibility_v{model_version}.onnx"
        
        # Determine checkpoint
        checkpoint_path = Path(args.checkpoint) if args.checkpoint else default_checkpoint
        if not checkpoint_path.exists():
            logger.error(f"Checkpoint not found: {checkpoint_path}")
            return 1
        
        # Create model
        logger.info(f"Creating {task} model with {len(labels)} classes")
        model = ImageClassifier(len(labels), pretrained=False)
        
        # Load checkpoint
        logger.info(f"Loading checkpoint from {checkpoint_path}")
        _load_checkpoint_into_model(model, checkpoint_path)
        model.eval()
        
        # Export to ONNX
        logger.info("Exporting model to ONNX format...")
        onnx_path = version_dir / onnx_filename
        onnx_path.parent.mkdir(parents=True, exist_ok=True)
        ONNXExporter.export_model(
            model,
            onnx_path,
            config.onnx_input_names,
            config.onnx_output_names,
            config.onnx_opset_version,
            (config.image_height, config.image_width),
            'cpu'
        )
        
        # Create metadata from args or metrics file
        logger.info("Creating model metadata...")
        metrics_file = version_dir / f'{task}_metrics.json'
        metadata_path = onnx_path.with_suffix('.onnx.json')
        
        if metadata_path.exists():
            logger.info(f"Metadata already exists at {metadata_path}, skipping")
        elif metrics_file.exists():
            logger.info(f"Loading metrics from {metrics_file}")
            with open(metrics_file) as f:
                metrics = json.load(f)
            metadata = ONNXExporter.create_metadata(
                model_version,
                metrics,
                labels,
                (config.image_height, config.image_width),
                {'mean': config.normalization_mean, 'std': config.normalization_std},
                label_to_index
            )
            save_metadata(metadata, metadata_path)
        else:
            logger.warning(f"No metrics file found at {metrics_file}, skipping metadata generation")
        
        logger.info(f"Export complete!")
        logger.info(f"ONNX model: {onnx_path}")
        logger.info(f"Metadata: {metadata_path}")

        if args.upload:
            bucket = args.bucket or config.s3_bucket
            s3_prefix = config.get_s3_model_version_prefix(model_version)
            logger.info(f"Uploading artifacts to s3://{bucket}/{s3_prefix}")
            uploader = S3ModelUploader(bucket, config.aws_region, profile=config.aws_profile)
            uploader.upload_model(
                onnx_path,
                metadata_path,
                s3_prefix,
                model_version,
                model_filename=onnx_filename,
                metadata_filename=metadata_path.name
            )
            logger.info("Upload complete")
        else:
            logger.info("Skipping S3 upload (use --upload to upload model and metadata)")
        
        return 0
    
    except Exception as e:
        logger.exception(f"Export failed: {e}")
        return 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Export trained model to ONNX')
    parser.add_argument(
        '--version',
        type=int,
        required=True,
        help='Model version number (e.g., 2 for v2)'
    )
    parser.add_argument(
        '--task',
        choices=['frame_state', 'visibility'],
        default='frame_state',
        help='Which model checkpoint to export'
    )
    parser.add_argument(
        '--checkpoint',
        type=str,
        help='Path to PyTorch checkpoint file (defaults to best_<task>_model.pt)'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config.yaml',
        help='Path to config.yaml file'
    )
    parser.add_argument(
        '--upload',
        action='store_true',
        help='Upload exported model and metadata to S3'
    )
    parser.add_argument(
        '--bucket',
        type=str,
        help='S3 bucket name (overrides config.yaml)'
    )
    parser.add_argument('--best-epoch', type=int, help='Best epoch number')
    parser.add_argument('--best-val-loss', type=float, help='Best validation loss')
    parser.add_argument('--final-val-accuracy', type=float, help='Final validation accuracy')
    parser.add_argument('--final-train-loss', type=float, help='Final training loss')
    parser.add_argument('--final-val-loss', type=float, help='Final validation loss')
    
    args = parser.parse_args()
    exit_code = main(args)
    sys.exit(exit_code)
