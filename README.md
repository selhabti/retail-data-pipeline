# 📊 Pipeline de Données Retail
Un pipeline complet pour générer, traiter et analyser des données retail synthétiques sur **Google Cloud Platform (GCP)**.
---
## 📌 Aperçu
Ce projet automatise le flux de données retail de la génération à l'analyse en utilisant les services GCP :
- **Génération de données** → **Stockage** → **Orchestration ETL** → **Analytics** → **Infrastructure as Code (IaC)**
---
## 🏗 Architecture

```mermaid
graph TD
    A[Cloud Functions] --> B[Cloud Storage]
    B --> C[Airflow DAG]
    C --> D[BigQuery]
    E[Terraform] --> A
    E --> B
    E --> C
    E --> D
```
---
| Composant               | Technologie utilisée     | Rôle                                   |
|-------------------------|--------------------------|----------------------------------------|
| **Génération de données** | Cloud Functions (Python) | Génère des données clients/commandes   |
| **Stockage brut**       | Cloud Storage (GCS)      | Stocke les fichiers CSV/JSON bruts     |
| **Orchestration**       | Airflow (Cloud Composer) | Planifie et gère les workflows ETL     |
| **Data Warehouse**      | BigQuery                 | Tables partitionnées pour l'analyse    |
| **Infrastructure**      | Terraform                | Provisionnement automatisé des ressources GCP |

---



## 📁 Structure du Projet
```bash
.
├── airflow
│   └── dags
│       └── retail_data_dag.py
├── cloud_functions
│   ├── consolidate_masters
│   │   ├── deploy.sh
│   │   ├── main.py
│   │   └── requirements.txt
│   ├── generate_customers_daily
│   │   ├── deploy.sh
│   │   ├── main.py
│   │   └── requirements.txt
│   ├── generate_products_daily
│   │   ├── deploy.sh
│   │   ├── main.py
│   │   └── requirements.txt
│   ├── generate_suppliers_daily
│   │   ├── deploy.sh
│   │   ├── main.py
│   │   └── requirements.txt
│   └── shared
│       ├── config.py
│       ├── requirements.txt
│       └── utils.py
├── Makefile
├── README.md
├── requirements.txt
├── scripts
│   ├── changelog.sh
│   └── version.sh
├── terraform
│   ├── main.tf
│   ├── outputs.tf
│   └── variables.tf
└── tests
    ├── conftest.py
    ├── test_data_generator.py
    ├── test_generate_products_daily.py
    └── test_generate_suppliers_daily.py

<!-- TREE_START -->
## 📁 Structure du projet

```
[2025-06-11 14:36:27] [0;34m[INFO][0m Génération de l'arborescence...
.
|-- airflow
|   `-- dags
|       `-- retail_data_dag.py
|-- cloud_functions
|   |-- consolidate_masters
|   |   |-- deploy.sh
|   |   |-- .gcloudignore
|   |   |-- main.py
|   |   `-- requirements.txt
|   |-- generate_customers_daily
|   |   |-- deploy.sh
|   |   |-- .gcloudignore
|   |   |-- main.py
|   |   `-- requirements.txt
|   |-- generate_products_daily
|   |   |-- deploy.sh
|   |   |-- .gcloudignore
|   |   |-- main.py
|   |   `-- requirements.txt
|   |-- generate_suppliers_daily
|   |   |-- deploy.sh
|   |   |-- .gcloudignore
|   |   |-- main.py
|   |   `-- requirements.txt
|   `-- shared
|       |-- config.py
|       |-- requirements.txt
|       `-- utils.py
|-- .github
|   `-- workflows
|       |-- deploy.yml
|       `-- python-tests.yml
|-- scripts
|   |-- changelog.sh
|   `-- version.sh
|-- terraform
|   |-- main.tf
|   |-- outputs.tf
|   `-- variables.tf
|-- tests
|   |-- conftest.py
|   |-- test_data_generator.py
|   |-- test_generate_products_daily.py
|   `-- test_generate_suppliers_daily.py
|-- .gitignore
|-- Makefile
|-- README.md
|-- README.md.backup
|-- requirements.txt
`-- update-readme.sh

14 directories, 37 files
```

*Arborescence générée automatiquement le 2025-06-11 à 14:36:27*
<!-- TREE_END -->
