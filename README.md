# Tahoma Tracker

A system for tracking Mount Rainier visibility using automated image capture and ML classification.

## Overview

This project captures images from a public webcam, classifies them using trained ML models, and provides an API for querying visibility data. The infrastructure runs on AWS (Lambda, S3, DynamoDB, CloudFront).

**Key Features:**
- Automated image scraping every 10 minutes during daylight hours
- Two-stage ML classification: frame quality → visibility assessment
- Historical data with daily/monthly manifests for efficient querying
- Crowdsourced labeling API for model improvement

## Project Structure

```
src/main/java/com/tahomatracker/
├── cdk/                    # CDK infrastructure definitions
├── service/                # Lambda service code
│   ├── classifier/         # ONNX model inference
│   ├── scraper/            # Image processing pipeline
│   ├── api/                # API handlers
│   └── external/           # External integrations (S3, HTTP)
├── backfill/               # Backfill runner for historical images
└── domain/                 # Domain models

scripts/
└── image-backfill.sh       # Wrapper script for Java backfill runner

training/                   # ML model training (Python)
├── train.py                # Train frame_state or visibility models
├── export_only.py          # Re-export checkpoint to ONNX
├── backfill_model.py       # Generate classifications for new model version
└── ...                     # Supporting modules
```

## Quick Start

### Prerequisites

- Java 17+
- Maven 3.8+
- Python 3.9+ (for training)
- AWS CLI configured with appropriate credentials
- Docker (for CDK Lambda deployment)

### Build & Deploy

```bash
# Compile Java code
mvn compile

# Deploy infrastructure
cdk deploy
```

### CDK Commands

- `mvn package` - Compile and run tests
- `cdk ls` - List all stacks
- `cdk synth` - Emit CloudFormation template
- `cdk deploy` - Deploy stack (requires Docker)
- `cdk diff` - Compare deployed vs current state

## Backfill Tools

### Scraping Historical Images (Java)

Fetches panoramas from the camera, crops them, classifies, and persists analysis + manifests.

```bash
# Set AWS credentials
eval $(aws configure export-credentials --profile tahoma --format env)

# Using wrapper script (recommended)
./scripts/image-backfill.sh --start 2024-01-01 --end 2024-01-31 --bucket my-bucket

# Or directly with Java
mvn compile
java -cp "target/classes:$(mvn dependency:build-classpath -q -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout)" \
  com.tahomatracker.backfill.ImageBackfillRunner \
  --start 2024-01-01 --end 2024-01-31 --bucket my-bucket
```

**Options:**
| Flag | Description | Default |
|------|-------------|---------|
| `--start` | Start date (YYYY-MM-DD) | Required |
| `--end` | End date (YYYY-MM-DD) | Required |
| `--bucket` | S3 bucket name | Required |
| `--workers` | Parallel workers | 1 |
| `--concurrency` | HTTP concurrency for pano fetching | 8 |
| `--batch-size` | Batch size for pano slices | 32 |
| `--dry-run` | Preview without executing | false |

### Generating Classifications for New Model (Python)

When you've trained a new model version, generate classifications for existing images:

```bash
cd training
python backfill_model.py --model-version v2 --start 2024-01-01 --end 2024-12-31
```

This creates new versioned files without modifying existing data:
- `analysis/v2/YYYY/MM/DD/HHMM.json`
- `manifests/daily/v2/YYYY/MM/DD.json`
- `manifests/monthly/v2/YYYY/MM.json`

See `training/README.md` for details.

## Model Training

See [`training/README.md`](training/README.md) for ML model training documentation.

**Quick overview:**
```bash
cd training
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Train frame state classifier
python train.py --task frame_state

# Train visibility classifier  
python train.py --task visibility --upload
```

## AWS Credentials

**Java tools** require credentials as environment variables:
```bash
eval $(aws configure export-credentials --profile tahoma --format env)
```

**Python scripts** use boto3's default credential chain (environment, ~/.aws/credentials, IAM role).

## Data Model

### Classification Pipeline

1. **Frame State** (4 classes): Determines image quality
   - `good` - Clear, usable image
   - `dark` - Too dark (night/heavy clouds)
   - `bad` - Corrupted, blurry, or unusable
   - `off_target` - Camera pointed wrong direction

2. **Visibility** (3 classes): Only for "good" frames
   - `out` - Mountain fully visible
   - `partially_out` - Mountain partially obscured
   - `not_out` - Mountain not visible (clouds/fog)

### S3 Layout

```
{bucket}/
├── needle-cam/
│   ├── panos/YYYY/MM/DD/HHMM.jpg        # Full panoramas
│   └── cropped-images/YYYY/MM/DD/HHMM.jpg  # Cropped mountain region
├── analysis/{version}/YYYY/MM/DD/HHMM.json  # Classification results
├── manifests/
│   ├── daily/{version}/YYYY/MM/DD.json   # Daily summaries
│   └── monthly/{version}/YYYY/MM.json    # Monthly summaries
└── models/{version}/
    ├── frame_state_{version}.onnx
    └── visibility_{version}.onnx
```

## License

MIT
