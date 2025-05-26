# cloud_functions/shared/utils.py
import pandas as pd
from google.cloud import storage
import io
import logging
import json
from datetime import datetime

def download_csv_from_gcs(bucket_name, blob_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    data = blob.download_as_text()
    return pd.read_csv(io.StringIO(data))

def upload_csv_to_gcs(df, bucket_name, blob_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')
    logging.info(f"Uploaded to gs://{bucket_name}/{blob_path}")

def move_blob(bucket_name, source_blob_name, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    source_blob = bucket.blob(source_blob_name)
    bucket.copy_blob(source_blob, bucket, destination_blob_name)
    source_blob.delete()
    logging.info(f"Moved {source_blob_name} to {destination_blob_name}")

def append_audit_log(bucket_name, audit_log, audit_log_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(audit_log_path)
    try:
        existing = blob.download_as_text()
    except Exception:
        existing = ""
    new_line = json.dumps(audit_log) + "\n"
    blob.upload_from_string(existing + new_line, content_type='application/json')
    logging.info(f"Audit log updated at gs://{bucket_name}/{audit_log_path}")
