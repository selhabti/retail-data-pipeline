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

# Load bucket name from environment variable
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
            return None
        data = blob.download_as_text()
        df = pd.read_csv(io.StringIO(data))
        df_sorted = df.sort_values(by=list(df.columns))
        return hashlib.md5(df_sorted.to_csv(index=False).encode()).hexdigest()
    except Exception as e:
        logger.error(f"Error calculating hash: {str(e)}")
        return None

def upload_csv(df, bucket, path):
    """Upload a DataFrame as CSV to Google Cloud Storage."""
    blob = bucket.blob(path)
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')
    logger.info(f"File uploaded: gs://{BUCKET}/{path}")

def move_to_history(bucket, current_path, entity):
    """Move the current master file to a history folder with a timestamp."""
    try:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = os.path.basename(current_path)
        filename_without_ext = os.path.splitext(filename)[0]

        # New filename with timestamp
        history_filename = f"{filename_without_ext}_{timestamp}.csv"
        history_path = f"master/{entity}/history/{history_filename}"

        # Copy the blob to history path
        bucket.copy_blob(
            bucket.blob(current_path),
            bucket,
            history_path
        )
        logger.info(f"Archived: {current_path} -> {history_path}")
        return history_path
    except Exception as e:
        logger.error(f"Error archiving file: {str(e)}")
        return None

def log_audit(bucket, entity, event_data):
    """Append an audit log entry for the given entity."""
    try:
        audit_path = f"master/{entity}/audit/audit_log.jsonl"
        blob = bucket.blob(audit_path)

        # Create the file if it doesn't exist
        if not blob.exists():
            blob.upload_from_string("")

        # Append new entry
        current_content = blob.download_as_text()
        new_entry = json.dumps(event_data) + "\n"
        blob.upload_from_string(current_content + new_entry)
        logger.info(f"Audit log updated: {audit_path}")
    except Exception as e:
        logger.error(f"Error updating audit log: {str(e)}")

def process_mastering(entity, new_file, id_col):
    """Main mastering logic for a given entity."""
    client = storage.Client()
    bucket = client.bucket(BUCKET)

    # Paths specific to the entity
    master_dir = f"master/{entity}"
    master_path = f"{master_dir}/{entity}_master.csv"

    # Compare hashes of new file and current master
    new_hash = get_file_hash(BUCKET, new_file)
    current_hash = get_file_hash(BUCKET, master_path)

    if new_hash == current_hash:
        return {"action": "unchanged", "reason": "identical_content"}

    # Download new file data
    data = bucket.blob(new_file).download_as_text()
    new_df = pd.read_csv(io.StringIO(data))

    if current_hash is None:
        # No existing master, create new master file
        upload_csv(new_df, bucket, master_path)
        return {"action": "created", "rows": len(new_df)}

    # Archive current master before updating
    history_path = move_to_history(bucket, master_path, entity)

    # Upload new master with timestamp
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    new_master_path = f"{master_dir}/{entity}_master_{timestamp}.csv"
    upload_csv(new_df, bucket, new_master_path)

    # Update main master file (without timestamp)
    bucket.copy_blob(
        bucket.blob(new_master_path),
        bucket,
        master_path
    )

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

    # Determine entity and ID column based on file path prefix
    if file_name.startswith("customers/") and file_name.endswith(".csv"):
        entity, id_col = "customers", "customer_id"
    elif file_name.startswith("products/") and file_name.endswith(".csv"):
        entity, id_col = "products", "product_id"
    elif file_name.startswith("suppliers/") and file_name.endswith(".csv"):
        entity, id_col = "suppliers", "supplier_id"
    else:
        logger.info(f"Ignored file (not relevant): {file_name}")
        return "File not relevant"

    # Process mastering for the entity
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    result = process_mastering(entity, file_name, id_col)

    # Log audit entry
    log_audit(bucket, entity, {
        "timestamp": datetime.utcnow().isoformat(),
        "source_file": file_name,
        "entity": entity,
        "action": result.get("action"),
        "details": result
    })

    logger.info(f"Mastering {entity} completed: {result['action']}")
    return f"Mastering {entity}: {result['action']}"
