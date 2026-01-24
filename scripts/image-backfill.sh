#!/bin/bash
#
# Wrapper script for ImageBackfillRunner - scrapes historical images
#
# Usage:
#   ./scripts/image-backfill.sh --start 2024-01-01 --end 2024-01-31 --bucket my-bucket
#
# This script fetches panos, crops, classifies, persists analysis, and updates manifests.
# For generating classifications for a new model version, use training/backfill_model.py
#
# Required:
#   --start DATE        Start date (YYYY-MM-DD)
#   --end DATE          End date (YYYY-MM-DD)
#   --bucket NAME       S3 bucket name
#
# Optional:
#   --workers N         Parallel workers (default: 1)
#   --concurrency N     HTTP concurrency for pano fetching (default: 8)
#   --batch-size N      Batch size for pano slices (default: 32)
#   --dry-run           Print what would be done without executing
#
# Prerequisites:
#   - AWS credentials must be available (use: eval $(aws configure export-credentials --profile <profile> --format env))
#   - Maven must be installed
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if ! command -v mvn &> /dev/null; then
    echo "Error: Maven (mvn) is required but not installed." >&2
    exit 1
fi

if ! command -v java &> /dev/null; then
    echo "Error: Java is required but not installed." >&2
    exit 1
fi

if [[ -z "$AWS_ACCESS_KEY_ID" ]] && [[ -z "$AWS_SESSION_TOKEN" ]]; then
    echo "Warning: AWS credentials not found in environment." >&2
    echo "Run: eval \$(aws configure export-credentials --profile <profile> --format env)" >&2
fi

cd "$PROJECT_ROOT"

if [[ ! -d "target/classes" ]] || [[ $(find src -name "*.java" -newer target/classes 2>/dev/null | head -1) ]]; then
    echo "Compiling..."
    mvn compile -q
fi

CLASSPATH="target/classes:$(mvn dependency:build-classpath -q -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout)"

exec java -cp "$CLASSPATH" com.tahomatracker.backfill.ImageBackfillRunner "$@"
