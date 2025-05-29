import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import json
from faker import Faker
from google.cloud import storage

def get_excluded_countries():
    # List of countries to exclude from customer generation
    return ['Saudi Arabia', 'Israel', 'United Arab Emirates', 'India']

currency_map = {
    'France': 'EUR', 'Germany': 'EUR', 'Italy': 'EUR', 'Spain': 'EUR', 'Belgium': 'EUR',
    'Netherlands': 'EUR', 'United Kingdom': 'GBP', 'Switzerland': 'CHF', 'Austria': 'EUR',
    'Portugal': 'EUR', 'Poland': 'PLN', 'Sweden': 'SEK', 'Denmark': 'DKK', 'Finland': 'EUR',
    'Ireland': 'EUR', 'Norway': 'NOK', 'Luxembourg': 'EUR', 'Czech Republic': 'CZK',
    'Slovakia': 'EUR', 'Hungary': 'HUF', 'USA': 'USD', 'Canada': 'CAD', 'Australia': 'AUD',
    'New Zealand': 'NZD', 'Japan': 'JPY', 'South Korea': 'KRW', 'Brazil': 'BRL', 'Mexico': 'MXN'
}

def upload_to_gcs(df, bucket_name, folder, filename):
    # Uploads a DataFrame as a CSV file to Google Cloud Storage
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"{folder}/{filename}")
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')
    print(f"Uploaded {filename} to gs://{bucket_name}/{folder}/")

def generate_initial_b2b_customers(n=10000, fake=None):
    # Generates a DataFrame of B2B customers with realistic data
    excluded_countries = get_excluded_countries()
    valid_countries = []
    while len(valid_countries) < n:
        c = fake.country()
        if c not in excluded_countries:
            valid_countries.append(c)
    start_date = datetime.now() - timedelta(days=1)  # File for yesterday
    data = {
        'customer_id': [],
        'company_name': [],
        'vat_number': [],
        'address': [],
        'postal_code': [],
        'city': [],
        'country': [],
        'currency': [],
        'email': [],
        'phone': [],
        'industry': [],
        'created_at': [],
        'last_modified': [],
        'customer_segment': [],
        'is_active': [],
        'modification_history': []
    }
    for i, country in enumerate(valid_countries):
        cc = country[:2].upper()
        data['customer_id'].append(f"C{str(i+1).zfill(6)}")
        data['company_name'].append(fake.company())
        data['vat_number'].append(f"{cc}{random.randint(100000000,999999999)}")
        data['address'].append(fake.street_address())
        data['postal_code'].append(fake.postcode())
        data['city'].append(fake.city())
        data['country'].append(country)
        data['currency'].append(currency_map.get(country, 'EUR'))
        data['email'].append(fake.company_email())
        data['phone'].append(fake.phone_number())
        data['industry'].append(fake.job())
        data['created_at'].append(start_date)
        data['last_modified'].append(start_date)
        data['customer_segment'].append(random.choice(['SME', 'Mid-Market', 'Enterprise', 'Startup']))
        data['is_active'].append(random.choice([True, False]))
        data['modification_history'].append(json.dumps([]))
    df = pd.DataFrame(data)
    return df

def generate_customers_daily(request):
    """
    Cloud Function entry point for generating the daily customers file.
    This function generates a new customers file for the previous day and uploads it to Google Cloud Storage.
    """
    fake = Faker()
    Faker.seed(42)
    bucket_name = "retail-data-landing-zone"
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")
    customers_df = generate_initial_b2b_customers(n=10000, fake=fake)
    upload_to_gcs(customers_df, bucket_name, "customers", f"customers_{date_str}.csv")
    print(f"Daily customers file generated for {date_str}")
    return f"Daily customers file generated for {date_str}"
