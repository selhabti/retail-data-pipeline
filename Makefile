.PHONY: deploy-functions deploy-consolidate status create-service-accounts

# Variables
PROJECT_ID := sound-machine-457008-i6
REGION := us-central1

# Déployer toutes les fonctions
deploy-functions: deploy-consolidate

# Déployer individuellement
deploy-consolidate:
	@echo "🚀 Deploying consolidate_masters..."
	cd cloud_functions/consolidate_masters && ./deploy.sh

# Vérifier le statut des fonctions
status:
	@echo "📊 Cloud Functions Status:"
	gcloud functions list --format="table(name,status,serviceAccountEmail)"

# Créer les service accounts
create-service-accounts:
	@echo "🔑 Creating service accounts..."
	gcloud iam service-accounts create consolidate-masters-sa --display-name="Service Account for consolidate-masters"
	
	@echo "🔑 Granting permissions..."
	gcloud projects add-iam-policy-binding $(PROJECT_ID) \
	  --member="serviceAccount:consolidate-masters-sa@$(PROJECT_ID).iam.gserviceaccount.com" \
	  --role="roles/storage.objectAdmin"
	gcloud projects add-iam-policy-binding $(PROJECT_ID) \
	  --member="serviceAccount:consolidate-masters-sa@$(PROJECT_ID).iam.gserviceaccount.com" \
	  --role="roles/eventarc.eventReceiver"
	gcloud projects add-iam-policy-binding $(PROJECT_ID) \
	  --member="serviceAccount:consolidate-masters-sa@$(PROJECT_ID).iam.gserviceaccount.com" \
	  --role="roles/eventarc.admin"
