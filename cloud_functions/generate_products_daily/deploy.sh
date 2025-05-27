#!/bin/bash
# cloud_functions/generate_products_daily/deploy.sh

set -e

FUNCTION_NAME="generate_products_daily"
PROJECT_ID="sound-machine-457008-i6"
REGION="us-central1"
SERVICE_ACCOUNT="generate-products-daily-sa@${PROJECT_ID}.iam.gserviceaccount.com"

echo "Deploying ${FUNCTION_NAME}..."

gcloud functions deploy ${FUNCTION_NAME} \
  --runtime python310 \
  --trigger-http \
  --entry-point=generate_products_daily \
  --region=${REGION} \
  --service-account=${SERVICE_ACCOUNT} \
  --source=. \
  --project=${PROJECT_ID}

echo "✅ ${FUNCTION_NAME} deployed successfully!"
