import pandas as pd
import random
import json
from faker import Faker
from google.cloud import storage
from io import StringIO
from datetime import datetime, timedelta
import os

fake = Faker()
Faker.seed(42)

SUPPLIER_SERVICES = [
    "Transport", "Equipment", "Cleaning", "Security", "Consulting", "IT",
    "Catering", "Maintenance", "Construction", "Telecom", "Marketing", "HR", "Logistics"
]

def upload_to_gcs(df, bucket_name, folder, filename):
    """Upload a DataFrame as CSV to a GCS bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"{folder}/{filename}")
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')
    print(f"Uploaded {filename} to gs://{bucket_name}/{folder}/")

def generate_supplier_id(i):
    """Generate a supplier ID."""
    return f"S{str(i).zfill(6)}"

def generate_suppliers(n=500, duplicate_rate=0.05, date=None):
    """Generate a DataFrame of suppliers, with duplicates on Tuesdays."""
    if date is None:
        date = (datetime.utcnow() - timedelta(days=1)).date()  # Use yesterday's date by default
    supplier_ids = [generate_supplier_id(i) for i in range(1, n+1)]
    data = {
        'supplier_id': supplier_ids,
        'company_name': [fake.company() for _ in range(n)],
        'service_type': [random.choice(SUPPLIER_SERVICES) for _ in range(n)],
        'address': [fake.street_address() for _ in range(n)],
        'postal_code': [fake.postcode() for _ in range(n)],
        'city': [fake.city() for _ in range(n)],
        'country': [fake.country() for _ in range(n)],
        'email': [fake.company_email() for _ in range(n)],
        'phone': [fake.phone_number() for _ in range(n)],
        'created_at': [date for _ in range(n)],
        'last_modified': [date for _ in range(n)],
        'is_active': [random.choice([True, False]) for _ in range(n)],
        'modification_history': [json.dumps([]) for _ in range(n)]
    }
    df = pd.DataFrame(data)

    # Add duplicates every Tuesday
    if date.weekday() == 1:  # 0=Monday, 1=Tuesday
        n_duplicates = int(n * duplicate_rate)
        duplicate_indices = random.sample(range(n), n_duplicates)
        duplicates = df.loc[duplicate_indices].copy()
        for idx in duplicates.index:
            hist = []
            now = datetime.now()
            # Simulate a modification
            if random.random() < 0.5:
                old_address = duplicates.at[idx, 'address']
                new_address = fake.street_address()
                duplicates.at[idx, 'address'] = new_address
                hist.append({'date': now.isoformat(), 'field': 'address', 'old': old_address, 'new': new_address})
            else:
                old_name = duplicates.at[idx, 'company_name']
                new_name = fake.company() + " " + random.choice(['SAS', 'SARL', 'SA', 'GmbH', 'Ltd'])
                duplicates.at[idx, 'company_name'] = new_name
                hist.append({'date': now.isoformat(), 'field': 'company_name', 'old': old_name, 'new': new_name})
            duplicates.at[idx, 'last_modified'] = now
            duplicates.at[idx, 'modification_history'] = json.dumps(hist)
            duplicates.at[idx, 'supplier_id'] = f"DUP{str(idx).zfill(6)}"
        df = pd.concat([df, duplicates], ignore_index=True)
    return df

def generate_and_upload_suppliers(bucket_name="retail-data-landing-zone", date=None):
    """Generate and upload the suppliers file for a given date (default: yesterday)."""
    if date is None:
        date = (datetime.utcnow() - timedelta(days=1)).date()  # Use yesterday's date by default
    date_str = date.strftime("%Y-%m-%d")
    folder = "suppliers"
    filename = f"suppliers_{date_str}.csv"
    suppliers_df = generate_suppliers(n=500, duplicate_rate=0.05, date=date)
    upload_to_gcs(suppliers_df, bucket_name, folder, filename)
    print(f"Suppliers generated and uploaded for {date_str} ({len(suppliers_df)} records)")

# Cloud Function entry point
def generate_suppliers_daily(request):
    """HTTP Cloud Function to generate yesterday's suppliers file."""
    bucket_name = os.environ.get("SUPPLIER_BUCKET", "retail-data-landing-zone")
    generate_and_upload_suppliers(bucket_name)
    return "Yesterday's suppliers generated.", 200
