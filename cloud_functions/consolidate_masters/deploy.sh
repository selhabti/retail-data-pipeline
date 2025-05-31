#!/bin/bash
# cloud_functions/consolidate_masters/deploy.sh

set -e

FUNCTION_NAME="consolidate-masters"
PROJECT_ID="sound-machine-457008-i6"
REGION="us-central1"
BUCKET="retail-data-landing-zone"

echo "Deploying ${FUNCTION_NAME} with GCS trigger..."

gcloud functions deploy ${FUNCTION_NAME} \
  --gen2 \
  --runtime=python312 \
  --trigger-bucket=${BUCKET} \
  --entry-point=main \
  --region=${REGION} \
  --source=. \
  --project=${PROJECT_ID} \
  --memory=512MB \
  --timeout=300s

echo "✅ ${FUNCTION_NAME} deployed successfully!"
echo "🎯 Function will trigger on new files in gs://${BUCKET}"
