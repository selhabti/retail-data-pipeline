import pandas as pd
import re
from google.cloud import storage
from datetime import datetime
import io

# Configuration
BUCKET = "retail-data-landing-zone"
CUSTOMERS_PATTERN = r"customers/customers_(\d{4}-\d{2}-\d{2})\.csv"
PRODUCTS_PATTERN = r"products/products_(\d{4}-\d{2}-\d{2})\.csv"
CUSTOMERS_MASTER = "master/customers_master.csv"
PRODUCTS_MASTER = "master/products_master.csv"
CUSTOMERS_KEY = "customer_id"
PRODUCTS_KEY = "product_id"

def list_files_in_bucket(bucket_name, prefix):
    """Liste tous les fichiers dans un bucket avec un préfixe donné."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return [blob.name for blob in blobs]

def download_csv_from_gcs(bucket_name, blob_path):
    """Télécharge un fichier CSV depuis GCS et le retourne comme DataFrame."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    data = blob.download_as_text()
    return pd.read_csv(io.StringIO(data))

def upload_csv_to_gcs(df, bucket_name, blob_path):
    """Upload un DataFrame comme CSV vers GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')
    print(f"Uploaded to gs://{bucket_name}/{blob_path}")

def create_consolidated_master(bucket_name, file_pattern, primary_key, master_path):
    """Crée un master consolidé à partir de tous les fichiers historiques."""
    # Liste tous les fichiers correspondant au pattern
    all_files = list_files_in_bucket(bucket_name, file_pattern.split('/')[0])

    # Filtre les fichiers qui correspondent au pattern et extrait la date
    file_dates = []
    for file in all_files:
        match = re.match(file_pattern, file)
        if match:
            date_str = match.group(1)
            file_dates.append((file, datetime.strptime(date_str, "%Y-%m-%d")))

    # Trie les fichiers par date (du plus ancien au plus récent)
    file_dates.sort(key=lambda x: x[1])

    if not file_dates:
        print(f"No files found matching pattern {file_pattern}")
        return

    print(f"Found {len(file_dates)} files to process")

    # Initialise un DataFrame vide pour le master
    master_df = pd.DataFrame()

    # Traite chaque fichier dans l'ordre chronologique
    for file, date in file_dates:
        print(f"Processing {file} from {date.strftime('%Y-%m-%d')}")
        df = download_csv_from_gcs(bucket_name, file)

        # Ajoute une colonne pour la date du fichier
        df['file_date'] = date.strftime("%Y-%m-%d")

        if master_df.empty:
            # Premier fichier, initialise le master
            master_df = df
        else:
            # Fusionne avec le master existant
            # 1. Garde toutes les lignes du master qui n'ont pas de correspondance dans le nouveau fichier
            master_unique = master_df[~master_df[primary_key].isin(df[primary_key])]

            # 2. Ajoute toutes les lignes du nouveau fichier (elles remplacent les anciennes versions)
            master_df = pd.concat([master_unique, df], ignore_index=True)

    # Trie le master final par primary_key
    master_df = master_df.sort_values(by=primary_key)

    # Upload le master consolidé
    upload_csv_to_gcs(master_df, bucket_name, master_path)

    print(f"Master consolidé créé avec {len(master_df)} lignes")
    return master_df

def main():
    # Crée le master consolidé pour les clients
    print("=== Création du master clients ===")
    customers_master = create_consolidated_master(
        BUCKET,
        CUSTOMERS_PATTERN,
        CUSTOMERS_KEY,
        CUSTOMERS_MASTER
    )

    # Crée le master consolidé pour les produits
    print("=== Création du master produits ===")
    products_master = create_consolidated_master(
        BUCKET,
        PRODUCTS_PATTERN,
        PRODUCTS_KEY,
        PRODUCTS_MASTER
    )

    print("Terminé !")

if __name__ == "__main__":
    main()
