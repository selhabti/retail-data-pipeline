import os

# Configuration commune
PROJECT_ID = "sound-machine-457008-i6"
BUCKET_NAME = "retail-data-landing-zone"
REGION = "us-central1"

# Entités et leurs configurations
ENTITIES_CONFIG = {
    "customers": {
        "key": "customer_id",
        "master_dir": "master/customers/",
        "history_dir": "master/customers/history/",
        "audit_dir": "master/customers/audit_logs/",
        "service_account": f"generate-customers-daily-sa@{PROJECT_ID}.iam.gserviceaccount.com"
    },
    "products": {
        "key": "product_id",
        "master_dir": "master/products/",
        "history_dir": "master/products/history/",
        "audit_dir": "master/products/audit_logs/",
        "service_account": f"generate-products-daily-sa@{PROJECT_ID}.iam.gserviceaccount.com"
    },
    "suppliers": {
        "key": "supplier_id",
        "master_dir": "master/suppliers/",
        "history_dir": "master/suppliers/history/",
        "audit_dir": "master/suppliers/audit_logs/",
        "service_account": f"generate-suppliers-daily-sa@{PROJECT_ID}.iam.gserviceaccount.com"
    }
}
