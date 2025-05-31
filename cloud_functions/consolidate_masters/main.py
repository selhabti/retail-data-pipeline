import pandas as pd
from google.cloud import storage
from datetime import datetime
import io
import logging
import json
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET = "retail-data-landing-zone"

def get_file_hash(bucket_name, file_path):
    """Calcule le hash d'un fichier CSV."""
    client = storage.Client()
    try:
        blob = client.bucket(bucket_name).blob(file_path)
        if not blob.exists():
            return None
        data = blob.download_as_text()
        df = pd.read_csv(io.StringIO(data)).sort_values(by=list(pd.read_csv(io.StringIO(data)).columns))
        return hashlib.md5(df.to_csv(index=False).encode()).hexdigest()
    except:
        return None

def upload_csv(df, path):
    """Upload DataFrame vers GCS."""
    client = storage.Client()
    blob = client.bucket(BUCKET).blob(path)
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')

def move_to_history(current_path, entity):
    """Déplace vers historique."""
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    history_path = f"master/{entity}/history/{entity}_master_{timestamp}.csv"
    bucket.copy_blob(bucket.blob(current_path), bucket, history_path)
    return history_path

def log_audit(event_data):
    """Ajoute au log d'audit."""
    client = storage.Client()
    blob = client.bucket(BUCKET).blob("master/audit/audit_log.jsonl")
    try:
        existing = blob.download_as_text()
    except:
        existing = ""
    blob.upload_from_string(existing + json.dumps(event_data) + "\n")

def process_mastering(entity, new_file, id_col):
    """Logique principale de mastering."""
    master_path = f"master/{entity}/{entity}_master.csv"
    
    # Compare les hashs
    new_hash = get_file_hash(BUCKET, new_file)
    current_hash = get_file_hash(BUCKET, master_path)
    
    if new_hash == current_hash:
        return {"action": "unchanged", "reason": "identical_content"}
    
    # Charge le nouveau fichier
    client = storage.Client()
    data = client.bucket(BUCKET).blob(new_file).download_as_text()
    new_df = pd.read_csv(io.StringIO(data))
    
    if current_hash is None:
        # Premier master
        upload_csv(new_df, master_path)
        return {"action": "created", "rows": len(new_df)}
    
    # Mastering avec historisation
    history_path = move_to_history(master_path, entity)
    upload_csv(new_df, master_path)
    
    return {
        "action": "mastered", 
        "rows": len(new_df),
        "history": history_path
    }

def main(event, context):
    """Point d'entrée GCS."""
    file_name = event['name']
    
    # Détermine l'entité
    if file_name.startswith("customers/") and file_name.endswith(".csv"):
        entity, id_col = "customers", "customer_id"
    elif file_name.startswith("products/") and file_name.endswith(".csv"):
        entity, id_col = "products", "product_id"
    elif file_name.startswith("suppliers/") and file_name.endswith(".csv"):
        entity, id_col = "suppliers", "supplier_id"
    else:
        return "File not relevant"
    
    # Traite le mastering
    result = process_mastering(entity, file_name, id_col)
    
    # Audit
    log_audit({
        "timestamp": datetime.utcnow().isoformat(),
        "file": file_name,
        "entity": entity,
        "result": result
    })
    
    logger.info(f"Mastering {entity}: {result['action']}")
    return f"Mastering {entity}: {result['action']}"
