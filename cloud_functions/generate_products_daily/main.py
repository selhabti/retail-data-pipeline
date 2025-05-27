import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from faker import Faker
from google.cloud import storage

fake = Faker()
Faker.seed(42)

def upload_to_gcs(df, bucket_name, folder, filename):
    # Uploads a DataFrame as a CSV file to Google Cloud Storage
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"{folder}/{filename}")
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')
    print(f"Uploaded {filename} to gs://{bucket_name}/{folder}/")

def generate_products(n=2000):
    # Generates a DataFrame of products with realistic data
    product_ids = [f"P{str(i).zfill(5)}" for i in range(1, n+1)]
    categories = ['Computers', 'Components', 'Accessories']
    yesterday = datetime.now() - timedelta(days=1)
    data = {
        'product_id': product_ids,
        'product_name': [fake.word().capitalize() + " " + random.choice(['Pro', 'Plus', 'Max', 'Lite', 'Go']) for _ in range(n)],
        'category': [random.choice(categories) for _ in range(n)],
        'price': [round(random.uniform(10, 1000), 2) for _ in range(n)],
        'cost': [round(random.uniform(5, 800), 2) for _ in range(n)],
        'weight_kg': [round(random.uniform(0.1, 20), 2) for _ in range(n)],
        'in_stock': [random.choice([True, False]) for _ in range(n)],
        'created_at': [yesterday for _ in range(n)]
    }
    df = pd.DataFrame(data)
    return df

def generate_products_daily(request):
    """
    Cloud Function entry point for generating the daily products file.
    This function generates a new products file for the previous day and uploads it to Google Cloud Storage.
    """
    bucket_name = "retail-data-landing-zone"
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")
    products_df = generate_products()
    upload_to_gcs(products_df, bucket_name, "products", f"products_{date_str}.csv")
    print(f"Daily products file generated for {date_str}")
    return f"Daily products file generated for {date_str}"
