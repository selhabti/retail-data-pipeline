import pandas as pd
from google.cloud import storage, bigquery
from datetime import datetime
import io
import logging
import json
import hashlib
import os
import time
from io import StringIO
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

BUCKET = os.getenv("RETAIL_DATA_LANDING_ZONE_BUCKET")

if not BUCKET:
    logger.error("Environment variable RETAIL_DATA_LANDING_ZONE_BUCKET is not set.")
    raise EnvironmentError("RETAIL_DATA_LANDING_ZONE_BUCKET environment variable is required.")

# Buffer en mémoire pour stocker les logs d'étapes
step_logs_buffer = []

def append_step_log_buffer(entity, source_file, step, status, message="", rows=None, duration_sec=None):
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "entity": entity,
        "source_file": source_file,
        "step": step,
        "status": status,
        "message": message,
        "rows": rows if rows is not None else "",
        "duration_sec": duration_sec if duration_sec is not None else ""
    }
    step_logs_buffer.append(log_entry)

def flush_step_logs(bucket, entity):
    audit_path = f"master/{entity}/audit/step_log.csv"
    blob = bucket.blob(audit_path)

    if blob.exists():
        data = blob.download_as_text()
        df = pd.read_csv(StringIO(data))
        df = pd.concat([df, pd.DataFrame(step_logs_buffer)], ignore_index=True)
    else:
        df = pd.DataFrame(step_logs_buffer)

    blob.upload_from_string(df.to_csv(index=False), 'text/csv')
    logger.info(f"Flushed {len(step_logs_buffer)} step logs to {audit_path}")
    step_logs_buffer.clear()

def get_file_hash(bucket_name, file_path):
    client = storage.Client()
    start = time.time()
    try:
        blob = client.bucket(bucket_name).blob(file_path)
        if not blob.exists():
            logger.warning(f"File not found for hashing: gs://{bucket_name}/{file_path}")
            append_step_log_buffer("", file_path, "hash_calculation", "warning", "File not found for hashing")
            return None
        data = blob.download_as_text()
        df = pd.read_csv(io.StringIO(data))
        df_sorted = df.sort_values(by=list(df.columns))
        file_hash = hashlib.md5(df_sorted.to_csv(index=False).encode()).hexdigest()
        duration = time.time() - start
        logger.info(f"Calculated hash for gs://{bucket_name}/{file_path}: {file_hash}")
        append_step_log_buffer("", file_path, "hash_calculation", "success", f"Hash calculated: {file_hash}", duration_sec=duration)
        return file_hash
    except Exception as e:
        duration = time.time() - start
        logger.error(f"Error calculating hash for {file_path}: {str(e)}")
        append_step_log_buffer("", file_path, "hash_calculation", "failure", str(e), duration_sec=duration)
        return None

def upload_csv(df, bucket, path):
    start = time.time()
    try:
        blob = bucket.blob(path)
        blob.upload_from_string(df.to_csv(index=False), 'text/csv')
        duration = time.time() - start
        logger.info(f"File uploaded: gs://{BUCKET}/{path}")
        append_step_log_buffer("", path, "upload_csv", "success", f"Uploaded file", duration_sec=duration)
    except Exception as e:
        duration = time.time() - start
        logger.error(f"Error uploading file to gs://{BUCKET}/{path}: {str(e)}")
        append_step_log_buffer("", path, "upload_csv", "failure", str(e), duration_sec=duration)
        raise

def move_to_history(bucket, current_path, entity):
    start = time.time()
    try:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = os.path.basename(current_path)
        filename_without_ext = os.path.splitext(filename)[0]
        history_filename = f"{filename_without_ext}_{timestamp}.csv"
        history_path = f"master/{entity}/history/{history_filename}"

        source_blob = bucket.blob(current_path)
        if not source_blob.exists():
            logger.warning(f"Current master file does not exist, cannot archive: {current_path}")
            append_step_log_buffer(entity, current_path, "move_to_history", "warning", "Master file does not exist")
            return None

        # Copier dans history
        bucket.copy_blob(source_blob, bucket, history_path)
        # Supprimer l'ancien master (déplacement)
        source_blob.delete()

        duration = time.time() - start
        logger.info(f"Moved master file from {current_path} to {history_path}")
        append_step_log_buffer(entity, current_path, "move_to_history", "success", f"Moved to {history_path}", duration_sec=duration)
        return history_path
    except Exception as e:
        duration = time.time() - start
        logger.error(f"Error moving file {current_path} to history: {str(e)}")
        append_step_log_buffer(entity, current_path, "move_to_history", "failure", str(e), duration_sec=duration)
        return None

def clean_history(bucket, entity, max_versions=5):
    history_prefix = f"master/{entity}/history/"
    blobs = list(bucket.list_blobs(prefix=history_prefix))
    files = [blob for blob in blobs if not blob.name.endswith('/')]
    files.sort(key=lambda blob: blob.time_created)
    num_files_to_delete = len(files) - max_versions
    if num_files_to_delete > 0:
        logger.info(f"Deleting {num_files_to_delete} old history files for entity '{entity}'.")
        for i in range(num_files_to_delete):
            file_to_delete = files[i]
            logger.info(f"Deleting old history file: {file_to_delete.name}")
            bucket.delete_blob(file_to_delete.name)
        logger.info(f"History cleaned for entity '{entity}'.")
    else:
        logger.info(f"No history files to clean for entity '{entity}'.")

def log_audit(bucket, entity, event_data):
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

def load_csv_to_bigquery(dataset_id, table_id, gcs_uri, write_disposition="WRITE_TRUNCATE"):
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)
    start_time = time.time()

    append_step_log_buffer("", gcs_uri, "bigquery_table_creation", "info", f"Table {dataset_id}.{table_id} will be created if not exists")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=write_disposition,
    )

    try:
        load_job = client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config
        )
        load_job.result()
        
        destination_table = client.get_table(table_ref)
        duration = time.time() - start_time
        
        logger.info(f"Loaded data into BigQuery table {dataset_id}.{table_id} from {gcs_uri}")
        append_step_log_buffer(
            "", 
            gcs_uri, 
            "bigquery_load", 
            "success", 
            f"Loaded into {dataset_id}.{table_id} - {load_job.output_rows} rows",
            rows=load_job.output_rows,
            duration_sec=duration
        )
        return True
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"Failed to load data into BigQuery table {dataset_id}.{table_id}: {str(e)}")
        append_step_log_buffer(
            "", 
            gcs_uri, 
            "bigquery_load", 
            "failure", 
            str(e),
            duration_sec=duration
        )
        return False

def process_mastering(entity, new_file, id_col):
    client = storage.Client()
    bucket = client.bucket(BUCKET)

    master_dir = f"master/{entity}"
    master_path = f"{master_dir}/{entity}_master.csv"

    logger.info(f"Starting mastering process for entity '{entity}' with new file: {new_file}")
    append_step_log_buffer(entity, new_file, "start_mastering", "success", "Starting mastering process")

    start = time.time()
    new_hash = get_file_hash(BUCKET, new_file)
    if new_hash is None:
        append_step_log_buffer(entity, new_file, "hash_calculation", "failure", "Failed to calculate hash")
        flush_step_logs(bucket, entity)
        return {"action": "error", "reason": "new_file_hash_failed"}

    current_hash = get_file_hash(BUCKET, master_path)

    if new_hash == current_hash:
        append_step_log_buffer(entity, new_file, "compare_hash", "success", "No changes detected")
        flush_step_logs(bucket, entity)
        return {"action": "unchanged", "reason": "identical_content"}

    try:
        data = bucket.blob(new_file).download_as_text()
        new_df = pd.read_csv(io.StringIO(data))
        append_step_log_buffer(entity, new_file, "download_file", "success", rows=len(new_df))
    except Exception as e:
        append_step_log_buffer(entity, new_file, "download_file", "failure", str(e))
        flush_step_logs(bucket, entity)
        return {"action": "error", "reason": "download_or_read_failed"}

    if current_hash is None:
        append_step_log_buffer(entity, new_file, "create_master", "success", "No existing master found, creating new master")
        try:
            upload_csv(new_df, bucket, master_path)
            flush_step_logs(bucket, entity)
            return {"action": "created", "rows": len(new_df)}
        except Exception as e:
            append_step_log_buffer(entity, new_file, "upload_master", "failure", str(e))
            flush_step_logs(bucket, entity)
            return {"action": "error", "reason": "upload_failed"}

    history_path = move_to_history(bucket, master_path, entity)
    if history_path is None:
        append_step_log_buffer(entity, master_path, "move_to_history", "warning", "History archiving failed or skipped")

    clean_history(bucket, entity, max_versions=5)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    new_master_path = f"{master_dir}/{entity}_master_{timestamp}.csv"

    try:
        upload_csv(new_df, bucket, new_master_path)
    except Exception as e:
        append_step_log_buffer(entity, new_master_path, "upload_timestamped_master", "failure", str(e))
        flush_step_logs(bucket, entity)
        return {"action": "error", "reason": "upload_failed"}

    try:
        bucket.copy_blob(bucket.blob(new_master_path), bucket, master_path)
        append_step_log_buffer(entity, master_path, "update_master", "success", "Updated main master file")
    except Exception as e:
        append_step_log_buffer(entity, master_path, "update_master", "failure", str(e))
        flush_step_logs(bucket, entity)
        return {"action": "error", "reason": "copy_failed"}

    # Chargement dans BigQuery
    gcs_uri = f"gs://{BUCKET}/{new_master_path}"
    bq_table_map = {
        "customers": "customers_master",
        "products": "products_master",
        "suppliers": "suppliers_master"
    }
    
    bq_status = "not_executed"
    try:
        bq_success = load_csv_to_bigquery("retail", bq_table_map[entity], gcs_uri)
        if bq_success:
            append_step_log_buffer(entity, new_master_path, "bigquery_overall", "success", "BigQuery load completed successfully")
            bq_status = "success"
        else:
            append_step_log_buffer(entity, new_master_path, "bigquery_overall", "warning", "BigQuery load completed with issues")
            bq_status = "partial_failure"
    except Exception as e:
        append_step_log_buffer(entity, new_master_path, "bigquery_overall", "failure", str(e))
        bq_status = "failed"
        flush_step_logs(bucket, entity)
        return {"action": "error", "reason": "bigquery_load_failed"}

    flush_step_logs(bucket, entity)

    duration = time.time() - start
    append_step_log_buffer(entity, new_file, "total_mastering_time", "success", f"Total mastering duration: {duration:.2f} sec", duration_sec=duration)
    flush_step_logs(bucket, entity)

    return {
        "action": "mastered",
        "rows": len(new_df),
        "current_master": master_path,
        "timestamped_version": new_master_path,
        "history": history_path,
        "bigquery_status": bq_status
    }

def main(event, context):
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
