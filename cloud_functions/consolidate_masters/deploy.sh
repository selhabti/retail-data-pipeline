#!/bin/bash
# cloud_functions/consolidate_masters/deploy.sh

set -e

FUNCTION_NAME="consolidate_masters"
PROJECT_ID="sound-machine-457008-i6"
REGION="us-central1"
SERVICE_ACCOUNT="consolidate-masters-sa@${PROJECT_ID}.iam.gserviceaccount.com"

echo "Deploying ${FUNCTION_NAME}..."

gcloud functions deploy ${FUNCTION_NAME} \
  --runtime python310 \
  --trigger-resource=retail-data-landing-zone \
  --trigger-event=google.storage.object.finalize \
  --entry-point=main \
  --region=${REGION} \
  --service-account=${SERVICE_ACCOUNT} \
  --source=. \
  --project=${PROJECT_ID}

echo "✅ ${FUNCTION_NAME} deployed successfully!"
