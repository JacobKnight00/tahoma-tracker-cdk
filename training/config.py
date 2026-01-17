"""
Configuration management for model training.
"""
import os
import logging
from pathlib import Path
from typing import Dict, Any, Optional

import yaml
import boto3
from dotenv import load_dotenv


logger = logging.getLogger(__name__)


class TrainingConfig:
    """Loads and manages training configuration from YAML and environment."""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize configuration from YAML file and environment variables.
        
        Args:
            config_path: Path to config.yaml file
        """
        load_dotenv()
        
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_file, 'r') as f:
            self.config = yaml.safe_load(f) or {}
            
        # Ensure top-level sections are dicts (handles empty YAML sections which parse as None)
        for key in self.config:
            if self.config[key] is None:
                self.config[key] = {}
        
        self._validate_config()
    
    def _validate_config(self) -> None:
        """Validate that required configuration sections exist."""
        required_sections = ['dynamodb', 's3', 'training', 'image', 'frame_state', 'visibility']
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"Missing required config section: {section}")
    
    @property
    def aws_profile(self) -> Optional[str]:
        """AWS profile name (optional)."""
        aws_config = self.config.get('aws') or {}
        return aws_config.get('profile')
    
    @property
    def aws_region(self) -> str:
        """AWS region for DynamoDB and S3."""
        # Try to get from config, then env vars/profile via boto3
        aws_config = self.config.get('aws') or {}
        region = aws_config.get('region')
        if not region:
            session = boto3.session.Session()
            region = session.region_name
        return region or 'us-west-2'
    
    @property
    def dynamodb_table_name(self) -> str:
        """DynamoDB table name."""
        return self.config['dynamodb'].get('table_name', 'TahomaTrackerImageLabels')
    
    @property
    def dynamodb_batch_size(self) -> int:
        """DynamoDB query batch size."""
        return self.config['dynamodb'].get('batch_size', 25)
    
    @property
    def s3_bucket(self) -> str:
        """S3 bucket name."""
        return self.config['s3'].get('bucket', 'tahoma-tracker-artifacts')
    
    @property
    def s3_cropped_images_prefix(self) -> str:
        """S3 prefix for cropped images."""
        return self.config['s3'].get('cropped_images_prefix', 'cropped-images')
    
    @property
    def s3_models_prefix(self) -> str:
        """S3 prefix for model artifacts."""
        return self.config['s3'].get('models_prefix', 'models')
    
    @property
    def s3_analysis_prefix(self) -> str:
        """S3 prefix for analysis outputs."""
        return self.config['s3'].get('analysis_prefix', 'analysis')
    
    # Manifest configuration (for filtering training data)
    @property
    def manifest_base_url(self) -> str:
        """Base URL for daily manifests."""
        manifest_config = self.config.get('manifest') or {}
        return manifest_config.get('base_url', 'https://deaf937kouf5m.cloudfront.net/manifests/daily')
    
    @property
    def manifest_cache_dir(self) -> str:
        """Local directory to cache manifests."""
        manifest_config = self.config.get('manifest') or {}
        return manifest_config.get('cache_dir', './cache/manifests')
    
    @property
    def manifest_confidence_threshold(self) -> float:
        """Confidence threshold for filtering labels (default 0.95)."""
        manifest_config = self.config.get('manifest') or {}
        return manifest_config.get('confidence_threshold', 0.95)
    
    @property
    def training_frame_state_model_version(self) -> int:
        """Frame state model version for training."""
        return self.config['training'].get('frame_state_model_version', 1)
    
    @property
    def training_visibility_model_version(self) -> int:
        """Visibility model version for training."""
        return self.config['training'].get('visibility_model_version', 1)
    
    @property
    def training_batch_size(self) -> int:
        """Batch size for training."""
        return self.config['training'].get('batch_size', 32)
    
    @property
    def training_learning_rate(self) -> float:
        """Learning rate for optimizer."""
        return self.config['training'].get('learning_rate', 0.001)
    
    @property
    def training_train_split(self) -> float:
        """Fraction of data to use for training (vs validation)."""
        return self.config['training'].get('train_split', 0.8)
    
    @property
    def training_random_seed(self) -> int:
        """Random seed for reproducibility."""
        return self.config['training'].get('random_seed', 42)
    
    @property
    def training_num_workers(self) -> int:
        """Number of data loader workers."""
        return self.config['training'].get('num_workers', 0)
    
    @property
    def training_epochs(self) -> int:
        """Number of epochs to train."""
        return self.config['training'].get('epochs', 8)
    
    # Legacy aliases used by older scripts
    @property
    def train_split(self) -> float:
        return self.training_train_split
    
    @property
    def random_seed(self) -> int:
        return self.training_random_seed
    
    @property
    def learning_rate(self) -> float:
        return self.training_learning_rate
    
    @property
    def epochs(self) -> int:
        return self.training_epochs
    
    @property
    def image_width(self) -> int:
        """Image width in pixels."""
        return self.config['image'].get('width', 224)
    
    @property
    def image_height(self) -> int:
        """Image height in pixels."""
        return self.config['image'].get('height', 224)
    
    @property
    def image_normalization(self) -> Dict[str, list]:
        """Normalization parameters for image preprocessing."""
        return self.config['image'].get('normalization', {
            'mean': [0.485, 0.456, 0.406],
            'std': [0.229, 0.224, 0.225]
        })
    
    @property
    def normalization_mean(self) -> list:
        """Convenience accessor for normalization mean values."""
        return self.image_normalization.get('mean', [0.485, 0.456, 0.406])
    
    @property
    def normalization_std(self) -> list:
        """Convenience accessor for normalization std values."""
        return self.image_normalization.get('std', [0.229, 0.224, 0.225])
    
    def get_frame_state_labels(self) -> list:
        """List of frame state classification labels."""
        return self.config.get('frame_state', {}).get('labels', ['good', 'dark', 'bad', 'off_target'])
    
    def get_frame_state_label_to_index(self) -> Dict[str, int]:
        """Mapping from frame state label to numeric index."""
        return self.config.get('frame_state', {}).get('label_to_index', {
            'good': 0, 'dark': 1, 'bad': 2, 'off_target': 3
        })
    
    @property
    def classification_labels(self) -> list:
        """Alias for frame state labels (legacy single-model scripts)."""
        return self.get_frame_state_labels()
    
    @property
    def label_to_index(self) -> Dict[str, int]:
        """Alias for frame state label mapping (legacy single-model scripts)."""
        return self.get_frame_state_label_to_index()
    
    @property
    def model_version(self) -> int:
        """Alias for frame state model version (legacy single-model scripts)."""
        return self.training_frame_state_model_version
    
    def get_visibility_labels(self) -> list:
        """List of visibility classification labels."""
        return self.config.get('visibility', {}).get('labels', ['out', 'partially_out', 'not_out'])
    
    def get_visibility_label_to_index(self) -> Dict[str, int]:
        """Mapping from visibility label to numeric index."""
        return self.config.get('visibility', {}).get('label_to_index', {
            'out': 0, 'partially_out': 1, 'not_out': 2
        })
    
    @property
    def onnx_opset_version(self) -> int:
        """ONNX opset version for export."""
        return self.config.get('onnx', {}).get('opset_version', 14)
    
    @property
    def onnx_input_names(self) -> list:
        """ONNX input node names."""
        return self.config.get('onnx', {}).get('input_names', ['input_image'])
    
    @property
    def onnx_output_names(self) -> list:
        """ONNX output node names."""
        return self.config.get('onnx', {}).get('output_names', ['output_probability'])
    
    @property
    def output_models_dir(self) -> Path:
        """Local directory for model artifacts."""
        path = Path(self.config.get('output', {}).get('models_dir', './models'))
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    @property
    def output_logs_dir(self) -> Path:
        """Local directory for logs."""
        path = Path(self.config.get('output', {}).get('logs_dir', './logs'))
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    @property
    def output_checkpoint_dir(self) -> Path:
        """Local directory for training checkpoints."""
        path = Path(self.config.get('output', {}).get('checkpoint_dir', './checkpoints'))
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    # Backwards-compatible aliases for earlier scripts
    @property
    def models_dir(self) -> Path:
        """Alias for output_models_dir."""
        return self.output_models_dir
    
    @property
    def logs_dir(self) -> Path:
        """Alias for output_logs_dir."""
        return self.output_logs_dir
    
    @property
    def checkpoint_dir(self) -> Path:
        """Alias for output_checkpoint_dir."""
        return self.output_checkpoint_dir
    
    @property
    def table_name(self) -> str:
        """Alias for dynamodb_table_name."""
        return self.dynamodb_table_name
    
    def get_s3_model_version_prefix(self, version: int = None) -> str:
        """Base S3 prefix for a specific model version (e.g., models/v1)."""
        v = version or self.training_frame_state_model_version
        return f"{self.s3_models_prefix}/v{v}"

    def get_s3_frame_state_model_path(self, version: int = None) -> str:
        """Full S3 key for frame state model artifact."""
        prefix = self.get_s3_model_version_prefix(version or self.training_frame_state_model_version)
        ver = version or self.training_frame_state_model_version
        return f"{prefix}/frame_state_v{ver}.onnx"
    
    def get_s3_visibility_model_path(self, version: int = None) -> str:
        """Full S3 key for visibility model artifact."""
        prefix = self.get_s3_model_version_prefix(version or self.training_visibility_model_version)
        ver = version or self.training_visibility_model_version
        return f"{prefix}/visibility_v{ver}.onnx"

    def get_s3_model_path(self, version: int = None) -> str:
        """Alias for frame state model path (legacy single-model scripts)."""
        return self.get_s3_frame_state_model_path(version)
