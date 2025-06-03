#!/bin/bash
set -e

# Charger les variables d'environnement centralisées
ENV_FILE="../../.env"
if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
    echo "✅ Fichier .env chargé : $ENV_FILE"
else
    echo "❌ ERREUR: Fichier .env introuvable : $ENV_FILE"
    exit 1
fi

# Définir les variables spécifiques
FUNCTION_NAME="${CONSOLIDATE_MASTERS_FUNCTION}"
BUCKET="${CONSOLIDATE_MASTERS_BUCKET}"

# Vérification des variables
declare -a required_vars=("FUNCTION_NAME" "PROJECT_ID" "REGION" "BUCKET")
missing_vars=()

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        missing_vars+=("$var")
    fi
done

if [ ${#missing_vars[@]} -ne 0 ]; then
    echo "❌ ERREUR: Variables manquantes : ${missing_vars[*]}"
    echo "   Vérifiez le fichier .env ou les définitions de variables"
    exit 1
fi

echo "Déploiement de ${FUNCTION_NAME}..."
echo "Projet: ${PROJECT_ID}"
echo "Région: ${REGION}"
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

echo "✅ ${FUNCTION_NAME} déployée avec succès!"
