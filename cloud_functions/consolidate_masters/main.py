import pandas as pd
import re
from google.cloud import storage
from datetime import datetime
import time
import io
import logging
import json
import os

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
BUCKET = "retail-data-landing-zone"
CUSTOMERS_KEY = "customer_id"
PRODUCTS_KEY = "product_id"
CUSTOMERS_MASTER = "master/customers_master.csv"
PRODUCTS_MASTER = "master/products_master.csv"
AUDIT_LOG_PATH = "master/audit_log.jsonl"
REPORTS_PATH = "master/reports/"

def download_csv_from_gcs(bucket_name, blob_path):
    """Download a CSV file from GCS and return as DataFrame."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    data = blob.download_as_text()
    return pd.read_csv(io.StringIO(data))

def upload_csv_to_gcs(df, bucket_name, blob_path):
    """Upload a DataFrame as CSV to GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')
    logger.info(f"Uploaded to gs://{bucket_name}/{blob_path}")

def append_audit_log(bucket_name, audit_log):
    """Append a JSON line to the audit log file in GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(AUDIT_LOG_PATH)
    try:
        existing = blob.download_as_text()
    except Exception:
        existing = ""
    new_line = json.dumps(audit_log) + "\n"
    blob.upload_from_string(existing + new_line, content_type='application/json')
    logger.info(f"Audit log updated at gs://{bucket_name}/{AUDIT_LOG_PATH}")

def process_mastering(entity, today_file_path, master_path, key):
    """Compare today's file with master, update if needed, and generate report."""
    logger.info(f"Processing mastering for {entity}")
    today_df = download_csv_from_gcs(BUCKET, today_file_path)
    try:
        master_df = download_csv_from_gcs(BUCKET, master_path)
    except Exception:
        logger.warning(f"No existing master found for {entity}, will create a new one.")
        master_df = pd.DataFrame(columns=today_df.columns)

    today_ids = set(today_df[key])
    master_ids = set(master_df[key])

    new_ids = today_ids - master_ids
    obsolete_ids = master_ids - today_ids
    common_ids = today_ids & master_ids

    # Only update if there are changes
    if new_ids or obsolete_ids:
        logger.info(f"Changes detected for {entity}: {len(new_ids)} new, {len(obsolete_ids)} obsolete.")
        # Update master: keep all from today (replace master)
        updated_master = today_df.copy()
        # Save new master (overwrite)
        upload_csv_to_gcs(updated_master, BUCKET, master_path)
        # Also save a versioned copy for lineage
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        versioned_path = master_path.replace(".csv", f"_{timestamp}.csv")
        upload_csv_to_gcs(updated_master, BUCKET, versioned_path)
        # Generate report
        report_df = pd.DataFrame({
            "type": ["new", "obsolete", "existing"],
            "count": [len(new_ids), len(obsolete_ids), len(common_ids)]
        })
        report_name = f"{entity}_report_{timestamp}.csv"
        report_path = os.path.join(REPORTS_PATH, report_name)
        upload_csv_to_gcs(report_df, BUCKET, report_path)
        logger.info(f"Report generated at gs://{BUCKET}/{report_path}")
        return {
            "updated": True,
            "new": len(new_ids),
            "obsolete": len(obsolete_ids),
            "existing": len(common_ids),
            "versioned_path": versioned_path,
            "report_path": report_path
        }
    else:
        logger.info(f"No changes detected for {entity}. Master not updated.")
        return {
            "updated": False,
            "new": 0,
            "obsolete": 0,
            "existing": len(common_ids),
            "versioned_path": None,
            "report_path": None
        }

def main(event, context):
    """
    Cloud Function entry point for GCS trigger.
    Triggered when a new file is created in the bucket.
    """
    start_time = time.time()
    try:
        file_name = event['name']
        logger.info(f"New file detected: {file_name}")

        results = {}
        today = datetime.utcnow().strftime("%Y-%m-%d")

        # Check if the file is a customers or products file for today
        if file_name == f"customers/customers_{today}.csv":
            results["customers"] = process_mastering(
                "customers", file_name, CUSTOMERS_MASTER, CUSTOMERS_KEY
            )
        elif file_name == f"products/products_{today}.csv":
            results["products"] = process_mastering(
                "products", file_name, PRODUCTS_MASTER, PRODUCTS_KEY
            )
        else:
            logger.info("File is not a customers or products file for today. Skipping mastering.")
            return "No mastering needed for this file."

        elapsed = time.time() - start_time
        logger.info(f"Total execution time: {elapsed:.2f} seconds")

        # Audit log
        audit_log = {
            "event": "mastering",
            "timestamp": datetime.utcnow().isoformat(),
            "file": file_name,
            "results": results,
            "duration_seconds": elapsed
        }
        logger.info(f"AUDIT_LOG: {audit_log}")
        append_audit_log(BUCKET, audit_log)

        return "Mastering process completed!"

    except Exception as e:
        logger.error(f"Error during execution: {e}")
        return f"Error: {str(e)}", 500
