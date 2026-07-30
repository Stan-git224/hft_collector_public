#!/bin/bash
# Pull market data from S3 into local ./data
# Set your own bucket (don't commit the real bucket name):
#   export S3_BUCKET="s3://your-bucket-name/data"
set -euo pipefail

BUCKET="${S3_BUCKET:-s3://your-bucket-name/data}"

echo "Pulling latest market data from ${BUCKET}..."
aws s3 sync "${BUCKET}" ./data
echo "Sync complete."
