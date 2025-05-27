.PHONY: deploy-functions deploy-consolidate deploy-customers deploy-products deploy-suppliers status create-service-accounts

# Variables
PROJECT_ID := sound-machine-457008-i6
REGION := us-central1

# Déployer toutes les fonctions
deploy-functions: deploy-consolidate deploy-customers deploy-products deploy-suppliers

# Déployer individuellement
deploy-consolidate:
	@echo "🚀 Deploying consolidate_masters..."
	cd cloud_functions/consolidate_masters && ./deploy.sh

deploy-customers:
	@echo "🚀 Deploying generate_customers_daily..."
	cd cloud_functions/generate_customers_daily && ./deploy.sh

deploy-products:
	@echo "🚀 Deploying generate_products_daily..."
	cd cloud_functions/generate_products_daily && ./deploy.sh

deploy-suppliers:
	@echo "🚀 Deploying generate_suppliers_daily..."
	cd cloud_functions/generate_suppliers_daily && ./deploy.sh

# Vérifier le statut des fonctions
status:
	@echo "📊 Cloud Functions Status:"
	gcloud functions list --format="table(name,status,serviceAccountEmail)"

# Créer les service accounts
create-service-accounts:
	@echo "🔑 Creating service accounts..."
	gcloud iam service-accounts create consolidate-masters-sa --display-name="Service Account for consolidate-masters"
	gcloud iam service-accounts create generate-customers-daily-sa --display-name="Service Account for generate-customers-daily"
	gcloud iam service-accounts create generate-products-daily-sa --display-name="Service Account for generate-products-daily"
	gcloud iam service-accounts create generate-suppliers-daily-sa --display-name="Service Account for generate-suppliers-daily"
	
	@echo "🔑 Granting permissions..."
	gcloud projects add-iam-policy-binding $(PROJECT_ID) \
	  --member="serviceAccount:consolidate-masters-sa@$(PROJECT_ID).iam.gserviceaccount.com" \
	  --role="roles/storage.objectAdmin"
	gcloud projects add-iam-policy-binding $(PROJECT_ID) \
	  --member="serviceAccount:consolidate-masters-sa@$(PROJECT_ID).iam.gserviceaccount.com" \
	  --role="roles/eventarc.eventReceiver"
	gcloud projects add-iam-policy-binding $(PROJECT_ID) \
	  --member="serviceAccount:generate-customers-daily-sa@$(PROJECT_ID).iam.gserviceaccount.com" \
	  --role="roles/storage.objectAdmin"
	gcloud projects add-iam-policy-binding $(PROJECT_ID) \
	  --member="serviceAccount:generate-products-daily-sa@$(PROJECT_ID).iam.gserviceaccount.com" \
	  --role="roles/storage.objectAdmin"
	gcloud projects add-iam-policy-binding $(PROJECT_ID) \
	  --member="serviceAccount:generate-suppliers-daily-sa@$(PROJECT_ID).iam.gserviceaccount.com" \
	  --role="roles/storage.objectAdmin"
