# Model Training

This directory contains the model training pipeline for the Tahoma Tracker image classification system.

## Overview

The training system:
- Loads labeled images from DynamoDB (sparse table design: only labeled images)
- Downloads cropped images from S3
- Trains a ResNet50-based classifier on the labeled data
- Exports the trained model to ONNX format
- Uploads model and metadata to S3 for use by inference Lambda

## Architecture

### Components

1. **config.py** - Configuration management
   - Loads `config.yaml` for training parameters
   - Provides typed properties for all configuration values
   - Creates local directories (models/, logs/, checkpoints/)

2. **data_loader.py** - Data loading from AWS
   - `DynamoDBLabelLoader` - Fetches labels from DynamoDB table
   - `S3ImageLoader` - Downloads cropped images from S3
   - `TrainingDataset` - PyTorch Dataset implementation

3. **model.py** - Model architecture and training
   - `ImageClassifier` - ResNet50-based CNN
   - `ModelTrainer` - Training loop with validation
   - `ONNXExporter` - ONNX export and metadata generation

4. **s3_uploader.py** - S3 operations
   - `S3ModelUploader` - Uploads trained models and metadata
   - `S3ModelDownloader` - Downloads models for inference

5. **train_model.py** - Main orchestration script

## Setup

### Prerequisites

- Python 3.9+
- AWS credentials configured (via `~/.aws/credentials` or environment variables)
- Access to DynamoDB table `TahomaTrackerImageLabels`
- Access to S3 bucket `tahoma-tracker-artifacts`

### Install Dependencies

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Verify Setup

Run the test script to ensure your environment is correctly configured:

```bash
python test_setup.py
```

## Configuration

Edit `config.yaml` to adjust training parameters:

```yaml
training:
  model_version: 1        # Increment for each new model
  epochs: 50
  batch_size: 32
  learning_rate: 0.001
  train_split: 0.8        # 80% train, 20% validation
  
image:
  width: 224
  height: 224
  normalization:
    mean: [0.485, 0.456, 0.406]
    std: [0.229, 0.224, 0.225]
```

**Important**: After analyzing your dataset statistics, update the `normalization.mean` and `normalization.std` values for better training performance.

## Running Training

### Local training (save model locally first)

```bash
python train_model.py --bucket your-bucket-name
```

This will:
- Load labels from DynamoDB
- Download images from S3 (using provided bucket)
- Train the model
- Save ONNX model to `models/model_v1.onnx`
- Save metadata to `models/metadata_v1.json`

### Training with S3 upload

```bash
python train_model.py --bucket your-bucket-name --upload
```

This will additionally upload the trained model and metadata to S3:
- Model: `s3://tahoma-tracker-artifacts/models/v1/model.onnx`
- Metadata: `s3://tahoma-tracker-artifacts/models/v1/metadata.json`
- Version manifest: `s3://tahoma-tracker-artifacts/models/versions.json`

## Output

After training, you'll find:

- **models/model_v{N}.onnx** - Trained model in ONNX format
- **models/metadata_v{N}.json** - Model metadata (metrics, normalization, class labels)
- **logs/** - Training logs
- **checkpoints/best_model.pt** - Best model checkpoint in PyTorch format

## Classification Labels

The model predicts one of 6 classifications:

```
- dark              (image is too dark)
- bad               (image is blurry, corrupt, etc.)
- off_target        (not Mount Rainier or wrong mountain)
- good:out          (clear view, mountain fully visible outside cloud)
- good:partially_out (clear view, mountain partially in cloud)
- good:not_out      (clear view, mountain covered by cloud)
```

These match the `Classification` enum in Java code.

## Data Flow

```
DynamoDB (labels)
    ↓
    ├─→ ImageId: "2024/01/15/1430"
    ├─→ Classification: "good:not_out"
    └─→ FrameState, Visibility, VoteCounts
    
S3 (cropped images)
    ↓
    └─→ cropped-images/2024/01/15/1430.jpg
    
     ↓
[Training Dataset loads both]
     ↓
[PyTorch Model Training]
     ↓
[ONNX Export]
     ↓
S3 (model artifacts)
    ├─→ models/v1/model.onnx
    ├─→ models/v1/metadata.json
    └─→ models/versions.json
```

## Troubleshooting

### "No data available for training"
- Check DynamoDB table has labeled images
- Verify DynamoDB credentials and table name in config.yaml
- Ensure labels are complete (not in "good:pending" state)

### "Failed to load image from S3"
- Verify S3 bucket name and prefix in config.yaml
- Check S3 credentials and permissions
- Ensure cropped images exist for all labeled images

### Out of memory during training
- Reduce `training.batch_size` in config.yaml
- Reduce image dimensions (`image.width`, `image.height`)
- Reduce `epochs` for quick testing

### CUDA/GPU issues
- The script auto-detects GPU; set `device='cpu'` for testing
- Ensure PyTorch CUDA support is installed if using GPU

## Future Improvements

- [ ] Data augmentation (rotation, flip, color jitter)
- [ ] Hyperparameter tuning (grid search, Bayesian optimization)
- [ ] Model ensemble (multiple model versions)
- [ ] Dataset analysis and statistics (class distribution, image quality metrics)
- [ ] Automated retraining on a schedule
- [ ] A/B testing framework for model comparison
- [ ] ROC curve and confusion matrix analysis

## Architecture Notes

### Why Separate Training Module?

1. **Separation of Concerns** - Training is independent from inference
2. **Python Optimized** - Leverage PyTorch and data science ecosystem
3. **Scalability** - Can migrate to SageMaker or other services later
4. **Development** - Non-blocking for Java/Lambda development
5. **Reproducibility** - Version models, track training runs

### ONNX Format Choice

- **Portability** - Run on any platform (Java, C++, web)
- **Performance** - Optimized runtime inference
- **Format Stability** - Long-term compatibility
- **Java Support** - ONNX Runtime provides Java bindings

### Model Version Strategy

- One S3 path per model: `models/v{N}/`
- Analysis outputs use version-specific paths: `analysis/v{N}/`
- Java Lambda reads `MODEL_VERSION` env var to select which model to use
- Version manifest tracks all deployed versions

## Development Guidelines

- Run tests frequently: `python -m pytest` (tests not yet implemented)
- Use `.gitignore` to prevent committing large model files
- Log important steps for debugging
- Handle AWS exceptions gracefully
- Validate configuration before starting expensive operations
