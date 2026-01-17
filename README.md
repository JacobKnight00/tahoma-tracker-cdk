# Tahoma Tracker CDK (Java)

Java CDK app for the Tahoma Tracker backend infrastructure.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

It is a [Maven](https://maven.apache.org/) based project, so you can open this project with any Maven compatible Java IDE to build and run tests.

## Project Structure

```
src/main/java/com/tahomatracker/
├── cdk/                    # CDK infrastructure definitions
├── service/                # Lambda service code
│   ├── classifier/         # ONNX model inference
│   ├── scraper/            # Image processing pipeline
│   ├── api/                # API handlers
│   └── external/           # External integrations (S3, HTTP)
├── backfill/               # Backfill runners
└── domain/                 # Domain models

scripts/                    # Python utility scripts
training/                   # ML model training code
```

## CDK Commands

 * `mvn package`     compile and run tests
 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region (Docker required for Lambda image)
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

## Backfill Tools

### Image Backfill (Java)

Processes images through the full pipeline or re-classifies existing crops.

**Full mode** (fetch panos, crop, classify, persist analysis & manifests):
```bash
# Set AWS credentials
eval $(aws configure export-credentials --profile tahoma --format env)

# Run backfill
mvn compile
java -cp "target/classes:$(mvn dependency:build-classpath -q -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout)" \
  com.tahomatracker.backfill.ImageBackfillRunner \
  --start 2024-01-01 --end 2024-01-31 --bucket my-bucket
```

**Classify-only mode** (re-run classification on existing crops):
```bash
java -cp "target/classes:$(mvn dependency:build-classpath -q -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout)" \
  com.tahomatracker.backfill.ImageBackfillRunner \
  --start 2024-01-01 --end 2024-01-31 --bucket my-bucket --mode classify-only
```

Options:
- `--mode <full|classify-only>` - Processing mode (default: full)
- `--workers <n>` - Parallel workers (default: 1)
- `--concurrency <n>` - HTTP concurrency for pano fetching (default: 8)
- `--batch-size <n>` - Batch size for pano slices (default: 32)
- `--publish-latest` - Update latest.json after each image
- `--dry-run` - Print what would be done without executing

### Manifest Backfill (Python)

Rebuilds daily and monthly manifest files from existing analysis JSONs.

```bash
python scripts/backfill_manifests.py \
  --bucket my-bucket \
  --start 2024-01-01 \
  --end 2024-12-31 \
  --workers 4 \
  --profile tahoma
```

Options:
- `--workers <n>` - Parallel workers (default: 4)
- `--profile <name>` - AWS profile to use
- `--dry-run` - Print what would be done without writing

## Model Training

See `training/README.md` for ML model training documentation.

## AWS Credentials

Java tools require AWS credentials to be exported as environment variables:
```bash
eval $(aws configure export-credentials --profile tahoma --format env)
```

Python scripts support the `--profile` flag directly.
