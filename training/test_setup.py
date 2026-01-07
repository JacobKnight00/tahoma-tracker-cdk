"""
Test script to verify setup and configuration.
"""
import sys
import logging
import torch
import torchvision
import onnx
import onnxruntime
import boto3

from config import TrainingConfig
from model import ImageClassifier

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_imports():
    logger.info("Testing imports...")
    logger.info(f"Torch version: {torch.__version__}")
    logger.info(f"Torchvision version: {torchvision.__version__}")
    logger.info(f"ONNX version: {onnx.__version__}")
    logger.info(f"ONNX Runtime version: {onnxruntime.__version__}")
    logger.info(f"Boto3 version: {boto3.__version__}")
    return True

def test_config():
    logger.info("Testing configuration loading...")
    try:
        config = TrainingConfig("config.yaml")
        logger.info("Config loaded successfully.")
        logger.info(f"AWS Region: {config.aws_region}")
        logger.info(f"Frame state model version: {config.training_frame_state_model_version}")
        logger.info(f"Visibility model version: {config.training_visibility_model_version}")
        logger.info(f"Image Size: {config.image_width}x{config.image_height}")
        return True
    except Exception as e:
        logger.error(f"Config loading failed: {e}")
        return False

def test_model_creation():
    logger.info("Testing model creation...")
    try:
        config = TrainingConfig("config.yaml")
        
        frame_state_labels = config.get_frame_state_labels()
        visibility_labels = config.get_visibility_labels()
        
        frame_state_model = ImageClassifier(len(frame_state_labels))
        visibility_model = ImageClassifier(len(visibility_labels))
        logger.info("Models created successfully.")
        
        # Test forward pass with dummy data
        dummy_input = torch.randn(1, 3, config.image_height, config.image_width)
        fs_output = frame_state_model(dummy_input)
        vis_output = visibility_model(dummy_input)
        logger.info(f"Frame state forward pass successful. Output shape: {fs_output.shape}")
        logger.info(f"Visibility forward pass successful. Output shape: {vis_output.shape}")
        return True
    except Exception as e:
        logger.error(f"Model creation failed: {e}")
        return False

def main():
    logger.info("Starting setup verification...")
    
    if not test_imports():
        sys.exit(1)
        
    if not test_config():
        sys.exit(1)
        
    if not test_model_creation():
        sys.exit(1)
        
    logger.info("All checks passed! Environment is ready for training.")

if __name__ == "__main__":
    main()
