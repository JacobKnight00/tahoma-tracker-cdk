"""
Configuration management for model training.

Loads config.yaml (defaults) and merges with config.local.yaml (overrides) if present.
"""
import logging
from pathlib import Path
from typing import Dict, Optional

import yaml
import boto3
from dotenv import load_dotenv


logger = logging.getLogger(__name__)


class TrainingConfig:
    """Loads and manages training configuration from YAML files."""
    
    def __init__(self, config_path: str = "config.yaml"):
        load_dotenv()
        
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_file, 'r') as f:
            self.config = yaml.safe_load(f) or {}
        
        # Merge local overrides if present
        local_config_file = config_file.parent / "config.local.yaml"
        if local_config_file.exists():
            logger.info(f"Loading local config overrides from {local_config_file}")
            with open(local_config_file, 'r') as f:
                local_config = yaml.safe_load(f) or {}
            self._deep_merge(self.config, local_config)
        
        # Ensure top-level sections are dicts
        for key in self.config:
            if self.config[key] is None:
                self.config[key] = {}
        
        self._validate_config()
    
    def _deep_merge(self, base: dict, override: dict) -> None:
        """Recursively merge override into base dict."""
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = value
    
    def _validate_config(self) -> None:
        """Validate required configuration sections exist."""
        required = ['dynamodb', 's3', 'training', 'image', 'frame_state', 'visibility']
        for section in required:
            if section not in self.config:
                raise ValueError(f"Missing required config section: {section}")

    # AWS
    @property
    def aws_profile(self) -> Optional[str]:
        return (self.config.get('aws') or {}).get('profile')
    
    @property
    def aws_region(self) -> str:
        region = (self.config.get('aws') or {}).get('region')
        if not region:
            session = boto3.session.Session()
            region = session.region_name
        return region or 'us-west-2'

    # DynamoDB
    @property
    def dynamodb_table_name(self) -> str:
        return self.config['dynamodb']['table_name']
    
    @property
    def dynamodb_batch_size(self) -> int:
        return self.config['dynamodb'].get('batch_size', 25)

    # S3
    @property
    def s3_bucket(self) -> str:
        return self.config['s3']['bucket']
    
    @property
    def s3_cropped_images_prefix(self) -> str:
        return self.config['s3'].get('cropped_images_prefix', 'cropped-images')
    
    @property
    def s3_models_prefix(self) -> str:
        return self.config['s3'].get('models_prefix', 'models')
    
    @property
    def s3_analysis_prefix(self) -> str:
        return self.config['s3'].get('analysis_prefix', 'analysis')

    @property
    def s3_manifests_prefix(self) -> str:
        return self.config['s3'].get('manifests_prefix', 'manifests')

    # Manifest
    @property
    def manifest_base_url(self) -> str:
        return (self.config.get('manifest') or {}).get('base_url', '')
    
    @property
    def manifest_confidence_threshold(self) -> float:
        return (self.config.get('manifest') or {}).get('confidence_threshold', 0.95)

    # Cache (downloaded data)
    @property
    def cache_images_dir(self) -> Path:
        path = Path((self.config.get('cache') or {}).get('images_dir', './cache/images'))
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def cache_manifests_dir(self) -> Path:
        path = Path((self.config.get('cache') or {}).get('manifests_dir', './cache/manifests'))
        path.mkdir(parents=True, exist_ok=True)
        return path

    # Training
    @property
    def training_frame_state_model_version(self) -> int:
        return self.config['training'].get('frame_state_model_version', 1)
    
    @property
    def training_visibility_model_version(self) -> int:
        return self.config['training'].get('visibility_model_version', 1)
    
    @property
    def training_batch_size(self) -> int:
        return self.config['training'].get('batch_size', 32)
    
    @property
    def training_learning_rate(self) -> float:
        return self.config['training'].get('learning_rate', 0.001)
    
    @property
    def training_train_split(self) -> float:
        return self.config['training'].get('train_split', 0.8)
    
    @property
    def training_random_seed(self) -> int:
        return self.config['training'].get('random_seed', 42)
    
    @property
    def training_num_workers(self) -> int:
        return self.config['training'].get('num_workers', 0)
    
    @property
    def training_epochs(self) -> int:
        return self.config['training'].get('epochs', 8)

    # Image
    @property
    def image_width(self) -> int:
        return self.config['image'].get('width', 224)
    
    @property
    def image_height(self) -> int:
        return self.config['image'].get('height', 224)
    
    @property
    def image_normalization(self) -> Dict[str, list]:
        return self.config['image'].get('normalization', {
            'mean': [0.485, 0.456, 0.406],
            'std': [0.229, 0.224, 0.225]
        })
    
    @property
    def normalization_mean(self) -> list:
        return self.image_normalization.get('mean', [0.485, 0.456, 0.406])
    
    @property
    def normalization_std(self) -> list:
        return self.image_normalization.get('std', [0.229, 0.224, 0.225])

    # Frame State Classification
    def get_frame_state_labels(self) -> list:
        return self.config['frame_state'].get('labels', ['good', 'dark', 'bad', 'off_target'])
    
    def get_frame_state_label_to_index(self) -> Dict[str, int]:
        return self.config['frame_state'].get('label_to_index', {
            'good': 0, 'dark': 1, 'bad': 2, 'off_target': 3
        })

    # Visibility Classification
    def get_visibility_labels(self) -> list:
        return self.config['visibility'].get('labels', ['out', 'partially_out', 'not_out'])
    
    def get_visibility_label_to_index(self) -> Dict[str, int]:
        return self.config['visibility'].get('label_to_index', {
            'out': 0, 'partially_out': 1, 'not_out': 2
        })

    # ONNX Export
    @property
    def onnx_opset_version(self) -> int:
        return self.config.get('onnx', {}).get('opset_version', 14)
    
    @property
    def onnx_input_names(self) -> list:
        return self.config.get('onnx', {}).get('input_names', ['input_image'])
    
    @property
    def onnx_output_names(self) -> list:
        return self.config.get('onnx', {}).get('output_names', ['output_probability'])

    # Output (generated artifacts)
    @property
    def output_models_dir(self) -> Path:
        path = Path(self.config.get('output', {}).get('models_dir', './output/models'))
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    @property
    def output_logs_dir(self) -> Path:
        path = Path(self.config.get('output', {}).get('logs_dir', './output/logs'))
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    @property
    def output_checkpoints_dir(self) -> Path:
        path = Path(self.config.get('output', {}).get('checkpoints_dir', './output/checkpoints'))
        path.mkdir(parents=True, exist_ok=True)
        return path

    # S3 path helpers
    def get_s3_model_version_prefix(self, version: int) -> str:
        return f"{self.s3_models_prefix}/v{version}"

    def get_s3_frame_state_model_path(self, version: int = None) -> str:
        v = version or self.training_frame_state_model_version
        return f"{self.get_s3_model_version_prefix(v)}/frame_state_v{v}.onnx"
    
    def get_s3_visibility_model_path(self, version: int = None) -> str:
        v = version or self.training_visibility_model_version
        return f"{self.get_s3_model_version_prefix(v)}/visibility_v{v}.onnx"
