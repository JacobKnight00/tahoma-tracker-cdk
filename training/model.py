"""
Model training and ONNX export utilities.
"""
import logging
import json
from pathlib import Path
from typing import Tuple
from datetime import datetime

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader
import torchvision.models as models


logger = logging.getLogger(__name__)


class ImageClassifier(nn.Module):
    """PyTorch CNN for image classification."""
    
    def __init__(self, num_classes: int, pretrained: bool = True):
        """
        Initialize classifier using ResNet50 backbone.
        
        Args:
            num_classes: Number of output classes
            pretrained: Whether to use ImageNet pretrained weights
        """
        super().__init__()
        self.backbone = models.resnet50(pretrained=pretrained)
        # Replace final fc layer for num_classes
        num_features = self.backbone.fc.in_features
        self.backbone.fc = nn.Linear(num_features, num_classes)
        self.num_classes = num_classes
    
    def forward(self, x):
        """Forward pass."""
        return self.backbone(x)


class ModelTrainer:
    """Handles model training loop."""
    
    def __init__(
        self,
        model: nn.Module,
        learning_rate: float,
        device: str = 'cpu'
    ):
        """
        Initialize trainer.
        
        Args:
            model: PyTorch model
            learning_rate: Learning rate for optimizer
            device: Device to train on ('cpu' or 'cuda')
        """
        self.model = model.to(device)
        self.device = device
        self.optimizer = optim.Adam(model.parameters(), lr=learning_rate)
        self.criterion = nn.CrossEntropyLoss()
        self.train_losses = []
        self.val_losses = []
    
    def train_epoch(self, train_loader: DataLoader) -> float:
        """
        Train for one epoch.
        
        Args:
            train_loader: DataLoader for training data
        
        Returns:
            Average loss for the epoch
        """
        self.model.train()
        total_loss = 0.0
        num_batches = 0
        total_batches = len(train_loader)
        
        for batch_idx, (images, labels) in enumerate(train_loader):
            images = images.to(self.device)
            labels = labels.to(self.device)
            
            self.optimizer.zero_grad()
            outputs = self.model(images)
            loss = self.criterion(outputs, labels)
            loss.backward()
            self.optimizer.step()
            
            total_loss += loss.item()
            num_batches += 1
            
            # Log progress every 10 batches
            if (batch_idx + 1) % 10 == 0:
                logger.info(f"  Batch [{batch_idx + 1}/{total_batches}] - Loss: {loss.item():.4f}")
        
        avg_loss = total_loss / num_batches
        self.train_losses.append(avg_loss)
        return avg_loss
    
    def validate(self, val_loader: DataLoader) -> Tuple[float, float]:
        """
        Validate model on validation set.
        
        Args:
            val_loader: DataLoader for validation data
        
        Returns:
            Tuple of (average loss, accuracy)
        """
        self.model.eval()
        total_loss = 0.0
        correct = 0
        total = 0
        
        logger.info("  Running validation...")
        with torch.no_grad():
            for images, labels in val_loader:
                images = images.to(self.device)
                labels = labels.to(self.device)
                
                outputs = self.model(images)
                loss = self.criterion(outputs, labels)
                total_loss += loss.item()
                
                _, predicted = torch.max(outputs.data, 1)
                total += labels.size(0)
                correct += (predicted == labels).sum().item()
        
        avg_loss = total_loss / len(val_loader)
        accuracy = correct / total
        self.val_losses.append(avg_loss)
        
        return avg_loss, accuracy
    
    def train(
        self,
        train_loader: DataLoader,
        val_loader: DataLoader,
        epochs: int,
        checkpoint_dir: Path = None
    ) -> dict:
        """
        Train model for multiple epochs.
        
        Args:
            train_loader: DataLoader for training data
            val_loader: DataLoader for validation data
            epochs: Number of epochs to train
            checkpoint_dir: Directory to save checkpoints
        
        Returns:
            Dictionary with training metrics
        """
        logger.info(f"Starting training for {epochs} epochs")
        best_val_loss = float('inf')
        best_epoch = 0
        
        for epoch in range(epochs):
            train_loss = self.train_epoch(train_loader)
            val_loss, val_accuracy = self.validate(val_loader)
            
            logger.info(
                f"Epoch [{epoch + 1}/{epochs}] - "
                f"Train Loss: {train_loss:.4f}, "
                f"Val Loss: {val_loss:.4f}, "
                f"Val Accuracy: {val_accuracy:.4f}"
            )
            
            # Save checkpoint if validation loss improved
            if val_loss < best_val_loss:
                best_val_loss = val_loss
                best_epoch = epoch + 1
                if checkpoint_dir:
                    checkpoint_path = checkpoint_dir / f"best_model.pt"
                    torch.save(self.model.state_dict(), checkpoint_path)
                    logger.info(f"Saved checkpoint to {checkpoint_path}")
        
        metrics = {
            'best_epoch': best_epoch,
            'best_val_loss': best_val_loss,
            'final_train_loss': train_loss,
            'final_val_loss': val_loss,
            'final_val_accuracy': val_accuracy,
            'train_losses': self.train_losses,
            'val_losses': self.val_losses
        }
        
        logger.info(f"Training complete. Best val loss: {best_val_loss:.4f} at epoch {best_epoch}")
        return metrics


class ONNXExporter:
    """Handles ONNX model export."""
    
    @staticmethod
    def export_model(
        model: nn.Module,
        output_path: Path,
        input_names: list,
        output_names: list,
        opset_version: int = 14,
        image_size: Tuple[int, int] = (224, 224),
        device: str = 'cpu'
    ) -> None:
        """
        Export PyTorch model to ONNX format.
        
        Args:
            model: PyTorch model to export
            output_path: Path to save ONNX model
            input_names: List of input node names
            output_names: List of output node names
            opset_version: ONNX opset version
            image_size: Input image size (height, width)
            device: Device model is on
        
        Raises:
            RuntimeError if export fails
        """
        logger.info(f"Exporting model to ONNX: {output_path}")
        
        model.eval()
        
        # Create dummy input
        dummy_input = torch.randn(1, 3, image_size[0], image_size[1], device=device)
        
        try:
            torch.onnx.export(
                model,
                dummy_input,
                str(output_path),
                input_names=input_names,
                output_names=output_names,
                opset_version=opset_version,
                do_constant_folding=True,
                verbose=False,
                dynamic_axes={
                    input_names[0]: {0: 'batch_size'},
                    output_names[0]: {0: 'batch_size'}
                }
            )
            logger.info(f"Successfully exported model to {output_path}")
        except Exception as e:
            logger.error(f"Failed to export model to ONNX: {e}")
            raise RuntimeError(f"ONNX export failed: {e}") from e
    
    @staticmethod
    def create_metadata(
        model_version: int,
        metrics: dict,
        classification_labels: list,
        image_size: Tuple[int, int],
        normalization: dict,
        label_to_index: dict,
        training_timestamp: str = None
    ) -> dict:
        """
        Create metadata JSON for model.
        
        Args:
            model_version: Version number of the model
            metrics: Training metrics from ModelTrainer
            classification_labels: List of classification labels
            image_size: Input image size (height, width)
            normalization: Dict with 'mean' and 'std' keys
            label_to_index: Mapping from label to index
            training_timestamp: ISO timestamp of training (uses now if not provided)
        
        Returns:
            Dictionary with model metadata
        """
        if training_timestamp is None:
            training_timestamp = datetime.utcnow().isoformat() + "Z"
        
        metadata = {
            'model_version': model_version,
            'training_timestamp': training_timestamp,
            'metrics': {
                'best_epoch': metrics.get('best_epoch'),
                'best_val_loss': metrics.get('best_val_loss'),
                'best_val_accuracy': metrics.get('best_val_accuracy'),
                'final_val_accuracy': metrics.get('final_val_accuracy'),
                'final_train_loss': metrics.get('final_train_loss'),
                'final_val_loss': metrics.get('final_val_loss')
            },
            'input': {
                'size': list(image_size),
                'normalization': normalization
            },
            'output': {
                'num_classes': len(classification_labels),
                'class_labels': classification_labels,
                'class_indices': label_to_index
            },
            'inference': {
                'confidence_threshold': 0.7,  # TODO: tune based on business requirements
                'false_negative_bias': True   # prefer false negatives (safe side)
            }
        }
        
        return metadata


def save_metadata(metadata: dict, output_path: Path) -> None:
    """
    Save model metadata to JSON file.
    
    Args:
        metadata: Dictionary with model metadata
        output_path: Path to save JSON file
    """
    with open(output_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    logger.info(f"Saved metadata to {output_path}")
