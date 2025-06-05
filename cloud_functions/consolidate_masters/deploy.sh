#!/bin/bash
set -e

# Charger les variables d'environnement depuis le fichier .env à la racine (si disponible)
if [ -f ../../.env ]; then
  echo "📁 Loading environment variables from ../../.env"
  set -a
  source ../../.env
  set +a
else
  echo "⚠️  .env file not found at ../../.env - using environment variables"
fi

# Check required environment variables
required_vars=("CONSOLIDATE_MASTERS_FUNCTION" "CONSOLIDATE_MASTERS_BUCKET" "PROJECT_ID" "REGION")
missing_vars=()

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        missing_vars+=("$var")
    fi
done

if [ ${#missing_vars[@]} -ne 0 ]; then
    echo "❌ ERROR: Missing environment variables: ${missing_vars[*]}"
    echo "   Please set these variables before running the script."
    exit 1
fi

FUNCTION_NAME="${CONSOLIDATE_MASTERS_FUNCTION}"
BUCKET="${CONSOLIDATE_MASTERS_BUCKET}"

echo "Deploying ${FUNCTION_NAME}..."
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Bucket: ${BUCKET}"

gcloud functions deploy "${FUNCTION_NAME}" \
  --gen2 \
  --runtime=python312 \
  --entry-point=main \
  --region="${REGION}" \
  --source=. \
  --project="${PROJECT_ID}" \
  --memory=512MB \
  --timeout=300s \
  --trigger-event=google.cloud.storage.object.v1.finalized \
  --trigger-resource="${BUCKET}" \
  --set-env-vars RETAIL_DATA_LANDING_ZONE_BUCKET="${BUCKET}"

echo "✅ ${FUNCTION_NAME} deployed successfully!"
