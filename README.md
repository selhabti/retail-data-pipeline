# 📊 Pipeline de Données Retail

Un pipeline complet pour générer, traiter et analyser des données retail synthétiques sur **Google Cloud Platform (GCP)**.

---

## 📌 Aperçu
Ce projet automatise le flux de données retail de la génération à l'analyse en utilisant les services GCP :
- **Génération de données** → **Stockage** → **Orchestration ETL** → **Analytics** → **Infrastructure as Code (IaC)**

---

## 🏗 Architecture
![Diagramme d'architecture](docs/architecture.png) *(Ajoutez un diagramme si possible)*

| Composant               | Technologie utilisée     | Rôle                                   |
|-------------------------|--------------------------|----------------------------------------|
| **Génération de données** | Cloud Functions (Python) | Génère des données clients/commandes   |
| **Stockage brut**       | Cloud Storage (GCS)      | Stocke les fichiers CSV/JSON bruts     |
| **Orchestration**       | Airflow (Cloud Composer) | Planifie et gère les workflows ETL     |
| **Data Warehouse**      | BigQuery                 | Tables partitionnées pour l'analyse    |
| **Infrastructure**      | Terraform                | Provisionnement automatisé des ressources GCP |

---

```markdown
## Architecture

```mermaid
graph TD
    A[📦 Cloud Functions] -->|Données synthétiques| B[🗄 Cloud Storage]
    B --> C[🔄 Airflow DAG]
    C -->|ETL| D[📊 BigQuery]
    E[🛠 Terraform] --> A
    E[🛠 Terraform] --> B
    E[🛠 Terraform] --> C
    E[🛠 Terraform] --> D

## 📁 Structure du Projet
```bash
retail-data-pipeline/
├── .github/                  # Workflows GitHub Actions
│   └── workflows/
│       └── deploy.yml        # Pipeline CI/CD
├── airflow/                  # DAGs et plugins Airflow
│   ├── dags/                 # Workflows ETL
│   └── plugins/operators/    # Opérateurs Airflow personnalisés
├── cloud_functions/          # Cloud Functions GCP
│   ├── data_generator/       # Génération de données synthétiques
│   └── bq_loader/            # Chargement des données dans BigQuery
├── bigquery/                 # Schémas et SQL BigQuery
│   ├── schemas/              # Schémas de tables (JSON)
│   └── sql/views/            # Vues analytiques
├── terraform/                # Infrastructure as Code
│   ├── main.tf               # Ressources GCP
│   └── variables.tf          # Variables d'environnement
├── tests/                    # Tests unitaires/intégration
├── .gitignore               # Règles Git ignore
└── README.md                # Ce fichier
