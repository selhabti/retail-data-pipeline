import pandas as pd
from google.cloud import storage
from datetime import datetime
import io
import logging
import json
import hashlib
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET = os.getenv("RETAIL_DATA_LANDING_ZONE_BUCKET")

if not BUCKET:
    logger.error("Environment variable RETAIL_DATA_LANDING_ZONE_BUCKET is not set.")
    raise EnvironmentError("RETAIL_DATA_LANDING_ZONE_BUCKET environment variable is required.")

def get_file_hash(bucket_name, file_path):
    """Calculate the MD5 hash of a CSV file's content sorted by columns."""
    client = storage.Client()
    try:
        blob = client.bucket(bucket_name).blob(file_path)
        if not blob.exists():
            logger.warning(f"File not found for hashing: gs://{bucket_name}/{file_path}")
            return None
        data = blob.download_as_text()
        df = pd.read_csv(io.StringIO(data))
        df_sorted = df.sort_values(by=list(df.columns))
        file_hash = hashlib.md5(df_sorted.to_csv(index=False).encode()).hexdigest()
        logger.info(f"Calculated hash for gs://{bucket_name}/{file_path}: {file_hash}")
        return file_hash
    except Exception as e:
        logger.error(f"Error calculating hash for {file_path}: {str(e)}")
        return None

def upload_csv(df, bucket, path):
    """Upload a DataFrame as CSV to Google Cloud Storage."""
    try:
        blob = bucket.blob(path)
        blob.upload_from_string(df.to_csv(index=False), 'text/csv')
        logger.info(f"File uploaded: gs://{BUCKET}/{path}")
    except Exception as e:
        logger.error(f"Error uploading file to gs://{BUCKET}/{path}: {str(e)}")
        raise

def move_to_history(bucket, current_path, entity):
    """Move the current master file to a history folder with a timestamp."""
    try:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = os.path.basename(current_path)
        filename_without_ext = os.path.splitext(filename)[0]
        history_filename = f"{filename_without_ext}_{timestamp}.csv"
        history_path = f"master/{entity}/history/{history_filename}"

        source_blob = bucket.blob(current_path)
        if not source_blob.exists():
            logger.warning(f"Current master file does not exist, cannot archive: {current_path}")
            return None

        bucket.copy_blob(source_blob, bucket, history_path)
        logger.info(f"Archived master file from {current_path} to {history_path}")
        return history_path
    except Exception as e:
        logger.error(f"Error archiving file {current_path}: {str(e)}")
        return None

def log_audit(bucket, entity, event_data):
    """Append an audit log entry for the given entity."""
    try:
        audit_path = f"master/{entity}/audit/audit_log.jsonl"
        blob = bucket.blob(audit_path)

        if not blob.exists():
            logger.info(f"Audit log file does not exist, creating new: {audit_path}")
            blob.upload_from_string("")

        current_content = blob.download_as_text()
        new_entry = json.dumps(event_data) + "\n"
        blob.upload_from_string(current_content + new_entry)
        logger.info(f"Audit log updated: {audit_path}")
    except Exception as e:
        logger.error(f"Error updating audit log {audit_path}: {str(e)}")

def process_mastering(entity, new_file, id_col):
    """Main mastering logic for a given entity."""
    client = storage.Client()
    bucket = client.bucket(BUCKET)

    master_dir = f"master/{entity}"
    master_path = f"{master_dir}/{entity}_master.csv"

    logger.info(f"Starting mastering process for entity '{entity}' with new file: {new_file}")

    new_hash = get_file_hash(BUCKET, new_file)
    current_hash = get_file_hash(BUCKET, master_path)

    if new_hash is None:
        logger.error(f"New file hash could not be calculated, aborting mastering for {new_file}")
        return {"action": "error", "reason": "new_file_hash_failed"}

    if new_hash == current_hash:
        logger.info(f"No changes detected between new file and current master for {entity}")
        return {"action": "unchanged", "reason": "identical_content"}

    try:
        data = bucket.blob(new_file).download_as_text()
        new_df = pd.read_csv(io.StringIO(data))
        logger.info(f"Downloaded and read new file {new_file} with {len(new_df)} rows")
    except Exception as e:
        logger.error(f"Error downloading or reading new file {new_file}: {str(e)}")
        return {"action": "error", "reason": "download_or_read_failed"}

    if current_hash is None:
        logger.info(f"No existing master found for {entity}, creating new master file")
        try:
            upload_csv(new_df, bucket, master_path)
            return {"action": "created", "rows": len(new_df)}
        except Exception as e:
            logger.error(f"Failed to upload new master file for {entity}: {str(e)}")
            return {"action": "error", "reason": "upload_failed"}

    history_path = move_to_history(bucket, master_path, entity)
    if history_path is None:
        logger.warning(f"History archiving failed or skipped for {entity}")

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    new_master_path = f"{master_dir}/{entity}_master_{timestamp}.csv"

    try:
        upload_csv(new_df, bucket, new_master_path)
    except Exception as e:
        logger.error(f"Failed to upload timestamped master file for {entity}: {str(e)}")
        return {"action": "error", "reason": "upload_failed"}

    try:
        bucket.copy_blob(bucket.blob(new_master_path), bucket, master_path)
        logger.info(f"Updated main master file for {entity} to {master_path}")
    except Exception as e:
        logger.error(f"Failed to update main master file for {entity}: {str(e)}")
        return {"action": "error", "reason": "copy_failed"}

    return {
        "action": "mastered",
        "rows": len(new_df),
        "current_master": master_path,
        "timestamped_version": new_master_path,
        "history": history_path
    }

def main(event, context):
    """Cloud Function entry point triggered by GCS events."""
    file_name = event.get('name', '')
    logger.info(f"Triggered by file: {file_name}")

    if file_name.startswith("customers/") and file_name.endswith(".csv"):
        entity, id_col = "customers", "customer_id"
    elif file_name.startswith("products/") and file_name.endswith(".csv"):
        entity, id_col = "products", "product_id"
    elif file_name.startswith("suppliers/") and file_name.endswith(".csv"):
        entity, id_col = "suppliers", "supplier_id"
    else:
        logger.info(f"Ignored file (not relevant): {file_name}")
        return "File not relevant"

    client = storage.Client()
    bucket = client.bucket(BUCKET)

    result = process_mastering(entity, file_name, id_col)

    log_audit(bucket, entity, {
        "timestamp": datetime.utcnow().isoformat(),
        "source_file": file_name,
        "entity": entity,
        "action": result.get("action"),
        "details": result
    })

    logger.info(f"Mastering {entity} completed with action: {result.get('action')}")
    return f"Mastering {entity}: {result.get('action')}"
