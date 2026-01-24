# Model Training

Training pipeline for Tahoma Tracker image classification models.

## Overview

The system uses two sequential classifiers:
1. **Frame State** (4 classes) - Determines if the image is usable
2. **Visibility** (3 classes) - Determines mountain visibility (only for "good" frames)

Models are trained in PyTorch and exported to ONNX for Java inference.

## Setup

### Prerequisites

- Python 3.9+
- AWS credentials configured
- Access to DynamoDB labels table and S3 bucket

### Install Dependencies

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Configuration

Copy the example config and add your AWS resources:

```bash
cp config.yaml config.local.yaml
```

Edit `config.local.yaml`:
```yaml
s3:
  bucket: your-bucket-name

dynamodb:
  table_name: YourLabelsTable
```

The `config.local.yaml` file is gitignored and contains your resource-specific values.

## Training Models

### Train Frame State Classifier

```bash
python train.py --task frame_state
```

### Train Visibility Classifier

```bash
python train.py --task visibility
```

### Upload to S3

Add `--upload` to upload the trained model to S3:

```bash
python train.py --task frame_state --upload
python train.py --task visibility --upload
```

## Scripts

| Script | Purpose |
|--------|---------|
| `train.py` | Train frame_state or visibility model |
| `export_only.py` | Re-export a checkpoint to ONNX without retraining |
| `backfill_model.py` | Generate classifications for a new model version |

### Re-export a Model

If you have a trained checkpoint and need to re-export to ONNX:

```bash
python export_only.py --task frame_state
python export_only.py --task visibility --upload
```

### Generate Classifications for New Model

After training a new model version (e.g., v2), generate classifications for existing images:

```bash
python backfill_model.py --model-version v2 --start 2024-01-01 --end 2024-12-31
```

This reads existing cropped images from S3 and creates:
- `analysis/v2/YYYY/MM/DD/HHMM.json` - Classification results
- `manifests/daily/v2/YYYY/MM/DD.json` - Daily summaries
- `manifests/monthly/v2/YYYY/MM.json` - Monthly summaries

## Directory Structure

```
training/
├── train.py              # Main training script
├── export_only.py        # Re-export checkpoint to ONNX
├── backfill_model.py     # Generate classifications for new model
├── config.py             # Configuration loader
├── config.yaml           # Default config (committed)
├── config.local.yaml     # Your resources (gitignored)
├── data_loader.py        # DynamoDB/S3 data loading
├── model.py              # PyTorch model and training
├── s3_uploader.py        # S3 upload utilities
├── cache/                # Downloaded images (gitignored)
│   └── images/
└── output/               # Generated artifacts (gitignored)
    ├── models/
    ├── checkpoints/
    └── logs/
```

## Classification Labels

### Frame State (4 classes)
| Label | Description |
|-------|-------------|
| `good` | Clear, usable image |
| `dark` | Too dark (night/heavy clouds) |
| `bad` | Corrupted, blurry, or unusable |
| `off_target` | Camera pointed wrong direction |

### Visibility (3 classes, only for "good" frames)
| Label | Description |
|-------|-------------|
| `out` | Mountain fully visible |
| `partially_out` | Mountain partially obscured |
| `not_out` | Mountain not visible (clouds/fog) |

## Model Architecture

- Base: ResNet50 (pretrained on ImageNet)
- Input: 224x224 RGB images
- Normalization: ImageNet mean/std
- Output: Softmax probabilities

## Output Files

After training:
- `output/checkpoints/best_frame_state_model.pt` - Best PyTorch checkpoint
- `output/checkpoints/best_visibility_model.pt`
- `output/models/v{N}/frame_state_v{N}.onnx` - ONNX model
- `output/models/v{N}/visibility_v{N}.onnx`
- `output/models/v{N}/*.onnx.json` - Model metadata

## Troubleshooting

### "No S3 bucket configured"
Set `s3.bucket` in `config.local.yaml`

### "No valid images found"
- Check DynamoDB table has labeled images
- Verify AWS credentials
- Check S3 bucket contains cropped images

### Out of memory
- Reduce `training.batch_size` in config.yaml
- Set `training.num_workers: 0`

### Apple Silicon (M1/M2)
The training script auto-detects MPS (Metal Performance Shaders) for GPU acceleration on Apple Silicon.

## Version Strategy

- Models are versioned: `v1`, `v2`, etc.
- Each version has separate S3 paths
- Analysis and manifests are also versioned
- Lambda uses `MODEL_VERSION` env var to select active version
