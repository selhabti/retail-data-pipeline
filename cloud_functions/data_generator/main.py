import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import json
from faker import Faker
from google.cloud import storage

fake = Faker()
Faker.seed(42)

def upload_to_gcs(df, bucket_name, folder, filename):
    """
    Upload a DataFrame to Google Cloud Storage as a CSV file.

    Args:
        df: Pandas DataFrame to upload
        bucket_name: GCS bucket name
        folder: Destination folder in the bucket
        filename: Name of the file to create
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"{folder}/{filename}")
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')
    print(f"Uploaded {filename} to gs://{bucket_name}/{folder}/")

def download_from_gcs(bucket_name, folder, filename):
    """
    Download a CSV file from Google Cloud Storage and return as DataFrame.

    Args:
        bucket_name: GCS bucket name
        folder: Source folder in the bucket
        filename: Name of the file to download

    Returns:
        Pandas DataFrame or None if file doesn't exist
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"{folder}/{filename}")
    if not blob.exists():
        return None
    data = blob.download_as_string()
    return pd.read_csv(pd.compat.StringIO(data.decode('utf-8')))

def get_country_code(country):
    """
    Map country names to their ISO country codes.

    Args:
        country: Country name

    Returns:
        Two-letter country code (defaults to 'FR' if not found)
    """
    codes = {
        'France': 'FR', 'Germany': 'DE', 'Italy': 'IT', 'Spain': 'ES', 'Belgium': 'BE',
        'Netherlands': 'NL', 'United Kingdom': 'GB', 'Switzerland': 'CH', 'Austria': 'AT',
        'Portugal': 'PT', 'Poland': 'PL', 'Sweden': 'SE', 'Denmark': 'DK', 'Finland': 'FI',
        'Ireland': 'IE', 'Norway': 'NO', 'Luxembourg': 'LU', 'Czech Republic': 'CZ',
        'Slovakia': 'SK', 'Hungary': 'HU'
    }
    return codes.get(country, 'FR')

def generate_vat_number(country_code):
    """Generate a random VAT number with country prefix."""
    number = ''.join([str(random.randint(0, 9)) for _ in range(9)])
    return f"{country_code}{number}"

def generate_initial_b2b_customers(n=10000, duplicate_rate=0.02):
    """
    Generate initial B2B customer dataset with controlled duplicates.

    Args:
        n: Number of unique customers to generate
        duplicate_rate: Percentage of customers that will have duplicates

    Returns:
        Pandas DataFrame with customer data
    """
    company_names = [fake.company() for _ in range(n)]
    countries = [fake.country() for _ in range(n)]
    country_codes = [get_country_code(c) for c in countries]
    vat_numbers = [generate_vat_number(cc) for cc in country_codes]
    customer_ids = [f"C{str(i).zfill(6)}" for i in range(1, n+1)]
    now = datetime.now()

    # Create base customer data
    data = {
        'customer_id': customer_ids,
        'company_name': company_names,
        'vat_number': vat_numbers,
        'address': [fake.street_address() for _ in range(n)],
        'postal_code': [fake.postcode() for _ in range(n)],
        'city': [fake.city() for _ in range(n)],
        'country': countries,
        'email': [fake.company_email() for _ in range(n)],
        'phone': [fake.phone_number() for _ in range(n)],
        'industry': [fake.job() for _ in range(n)],
        'created_at': [fake.date_time_between(start_date='-10y', end_date='-1y') for _ in range(n)],
        'last_modified': [now for _ in range(n)],
        'customer_segment': [random.choice(['SME', 'Mid-Market', 'Enterprise', 'Startup']) for _ in range(n)],
        'is_active': [random.choice([True, False]) for _ in range(n)],
        'modification_history': [json.dumps([]) for _ in range(n)]
    }
    df = pd.DataFrame(data)

    # Generate duplicates with modification history
    n_duplicates = int(n * duplicate_rate)
    duplicate_indices = random.sample(range(n), n_duplicates)
    duplicates = df.loc[duplicate_indices].copy()

    for idx in duplicates.index:
        hist = []
        now = datetime.now()

        # Randomly modify either address or company name
        if random.random() < 0.5:
            old_address = duplicates.at[idx, 'address']
            new_address = fake.street_address()
            duplicates.at[idx, 'address'] = new_address
            hist.append({'date': now.isoformat(), 'field': 'address', 'old': old_address, 'new': new_address})
        else:
            old_name = duplicates.at[idx, 'company_name']
            new_name = fake.company() + " " + random.choice(['Inc', 'LLC', 'Ltd', 'GmbH', 'SAS'])
            duplicates.at[idx, 'company_name'] = new_name
            hist.append({'date': now.isoformat(), 'field': 'company_name', 'old': old_name, 'new': new_name})

        duplicates.at[idx, 'last_modified'] = now
        duplicates.at[idx, 'modification_history'] = json.dumps(hist)
        duplicates.at[idx, 'customer_id'] = f"DUP{str(idx).zfill(6)}"

    # Combine original and duplicate records
    df = pd.concat([df, duplicates], ignore_index=True)
    return df

def evolve_customers(df, day, add_new_rate=0.001, modif_rate=0.002, duplicate_rate=0.0005):
    """
    Evolve the customer dataset by adding new customers, modifying existing ones,
    and creating duplicates.

    Args:
        df: Existing customer DataFrame
        day: Current day string for tracking
        add_new_rate: Rate of new customers to add
        modif_rate: Rate of existing customers to modify
        duplicate_rate: Rate of new duplicates to create

    Returns:
        Updated DataFrame with evolved customer data
    """
    fake = Faker()
    now = datetime.now()

    # Add new customers
    n_new = int(len(df) * add_new_rate)
    new_clients = []

    for _ in range(n_new):
        country = fake.country()
        cc = get_country_code(country)
        vat = generate_vat_number(cc)
        cid = f"NEW{str(random.randint(100000,999999))}"

        new_clients.append({
            'customer_id': cid,
            'company_name': fake.company(),
            'vat_number': vat,
            'address': fake.street_address(),
            'postal_code': fake.postcode(),
            'city': fake.city(),
            'country': country,
            'email': fake.company_email(),
            'phone': fake.phone_number(),
            'industry': fake.job(),
            'created_at': now,
            'last_modified': now,
            'customer_segment': random.choice(['SME', 'Mid-Market', 'Enterprise', 'Startup']),
            'is_active': random.choice([True, False]),
            'modification_history': json.dumps([{'date': now.isoformat(), 'field': 'created', 'old': None, 'new': 'created'}])
        })

    df = pd.concat([df, pd.DataFrame(new_clients)], ignore_index=True)

    # Modify existing customers
    n_modif = int(len(df) * modif_rate)
    modif_indices = np.random.choice(df.index, n_modif, replace=False)

    for idx in modif_indices:
        now = datetime.now()
        field = random.choice(['address', 'company_name'])
        old = df.at[idx, field]

        if field == 'address':
            new = fake.street_address()
        else:
            new = fake.company() + " " + random.choice(['Inc', 'LLC', 'Ltd', 'GmbH', 'SAS'])

        df.at[idx, field] = new
        df.at[idx, 'last_modified'] = now

        # Update modification history
        hist = json.loads(df.at[idx, 'modification_history'])
        hist.append({'date': now.isoformat(), 'field': field, 'old': old, 'new': new})
        df.at[idx, 'modification_history'] = json.dumps(hist)

    # Add new duplicates
    n_dup = int(len(df) * duplicate_rate)
    dup_indices = np.random.choice(df.index, n_dup, replace=False)
    duplicates = df.loc[dup_indices].copy()

    for idx in duplicates.index:
        now = datetime.now()
        field = random.choice(['address', 'company_name'])
        old = duplicates.at[idx, field]

        if field == 'address':
            new = fake.street_address()
        else:
            new = fake.company() + " " + random.choice(['Inc', 'LLC', 'Ltd', 'GmbH', 'SAS'])

        duplicates.at[idx, field] = new
        duplicates.at[idx, 'last_modified'] = now

        hist = json.loads(duplicates.at[idx, 'modification_history'])
        hist.append({'date': now.isoformat(), 'field': field, 'old': old, 'new': new})
        duplicates.at[idx, 'modification_history'] = json.dumps(hist)
        duplicates.at[idx, 'customer_id'] = f"DUP{str(idx).zfill(6)}_{day}"

    df = pd.concat([df, duplicates], ignore_index=True)
    return df

def generate_products(n=2000):
    """
    Generate synthetic product data.

    Args:
        n: Number of products to generate

    Returns:
        DataFrame with product data
    """
    fake = Faker()
    Faker.seed(123)

    product_ids = [f"P{str(i).zfill(5)}" for i in range(1, n+1)]

    categories = ['Electronics', 'Clothing', 'Home', 'Garden', 'Sports', 'Food',
                  'Beauty', 'Toys', 'Books', 'Automotive']

    subcategories = {
        'Electronics': ['Smartphones', 'Computers', 'TV', 'Audio', 'Accessories'],
        'Clothing': ['Men', 'Women', 'Kids', 'Shoes', 'Accessories'],
        'Home': ['Furniture', 'Decoration', 'Kitchen', 'Appliances', 'Bedding'],
        'Garden': ['Tools', 'Plants', 'Outdoor Furniture', 'Pool', 'BBQ'],
        'Sports': ['Fitness', 'Cycling', 'Swimming', 'Running', 'Team Sports'],
        'Food': ['Grocery', 'Drinks', 'Fresh', 'Frozen', 'Organic'],
        'Beauty': ['Face Care', 'Body Care', 'Makeup', 'Perfume', 'Hair'],
        'Toys': ['Board Games', 'Stuffed Animals', 'Construction', 'Educational', 'Outdoor'],
        'Books': ['Novels', 'Comics', 'Children', 'Cooking', 'Self-Help'],
        'Automotive': ['Accessories', 'Maintenance', 'Spare Parts', 'Tires', 'Car Electronics']
    }

    category_list = []
    subcategory_list = []

    for _ in range(n):
        cat = random.choice(categories)
        category_list.append(cat)
        subcategory_list.append(random.choice(subcategories[cat]))

    data = {
        'product_id': product_ids,
        'product_name': [fake.word().capitalize() + " " + random.choice(['Pro', 'Plus', 'Max', 'Lite', 'Go']) for _ in range(n)],
        'category': category_list,
        'subcategory': subcategory_list,
        'price': [round(random.uniform(10, 1000), 2) for _ in range(n)],
        'cost': [round(random.uniform(5, 800), 2) for _ in range(n)],
        'weight_kg': [round(random.uniform(0.1, 20), 2) for _ in range(n)],
        'in_stock': [random.choice([True, False]) for _ in range(n)],
        'created_at': [fake.date_time_between(start_date='-2y', end_date='now') for _ in range(n)]
    }

    df = pd.DataFrame(data)
    return df

def generate_daily_orders(day_offset, n=5000, customers_df=None, products_df=None):
    """
    Generate synthetic order data for a specific day.

    Args:
        day_offset: Number of days in the past to generate orders for
        n: Number of orders to generate
        customers_df: DataFrame containing customer data
        products_df: DataFrame containing product data

    Returns:
        DataFrame with order data
    """
    target_date = datetime.now() - timedelta(days=day_offset)
    start_time = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0)
    end_time = start_time + timedelta(days=1)

    order_ids = [f"O{target_date.strftime('%Y%m%d')}{str(i).zfill(5)}" for i in range(1, n+1)]
    customer_ids = customers_df['customer_id'].tolist()
    product_ids = products_df['product_id'].tolist()

    payment_methods = ['Credit Card', 'PayPal', 'Bank Transfer', 'Cash on Delivery', 'Gift Card']
    shipping_methods = ['Standard', 'Express', 'Same Day', 'Pickup']

    order_dates = [start_time + (end_time - start_time) * random.random() for _ in range(n)]

    data = {
        'order_id': order_ids,
        'customer_id': [random.choice(customer_ids) for _ in range(n)],
        'product_id': [random.choice(product_ids) for _ in range(n)],
        'order_date': order_dates,
        'quantity': [random.randint(1, 10) for _ in range(n)],
        'status': [random.choice(['Completed', 'Shipped', 'Processing', 'Cancelled', 'Returned']) for _ in range(n)],
        'payment_method': [random.choice(payment_methods) for _ in range(n)],
        'shipping_method': [random.choice(shipping_methods) for _ in range(n)],
        'shipping_cost': [round(random.uniform(0, 50), 2) for _ in range(n)]
    }

    orders_df = pd.DataFrame(data)

    # Join with products to get prices
    orders_df = orders_df.merge(products_df[['product_id', 'price']], on='product_id', how='left')

    # Calculate amounts
    orders_df['amount'] = orders_df['quantity'] * orders_df['price']
    orders_df['total_amount'] = orders_df['amount'] + orders_df['shipping_cost']

    # Remove price column
    orders_df.drop('price', axis=1, inplace=True)

    return orders_df

def generate_and_upload_data(bucket_name="retail-data-landing-zone", force_create_master=False):
    """
    Main function to generate and upload all data to GCS.

    Args:
        bucket_name: Target GCS bucket
        force_create_master: Whether to force creation of master customer file
    """
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")

    # Force creation of master or check if it exists
    if force_create_master:
        print("Forcing creation of master customer database...")
        customers_df = generate_initial_b2b_customers()
        upload_to_gcs(customers_df, bucket_name, "master", "customers_master.csv")
        print(f"Master database created with {len(customers_df)} customers")
    else:
        # Normal behavior: load or create if missing
        customers_df = download_from_gcs(bucket_name, "master", "customers_master.csv")
        if customers_df is None:
            print("Master customer database missing, creating initial version...")
            customers_df = generate_initial_b2b_customers()
            upload_to_gcs(customers_df, bucket_name, "master", "customers_master.csv")
            print(f"Master database created with {len(customers_df)} customers")

    # Simulate evolution (e.g., every Monday)
    if yesterday.weekday() == 0:  # 0 = Monday
        customers_df = evolve_customers(customers_df, date_str)
        upload_to_gcs(customers_df, bucket_name, "master", "customers_master.csv")
        print(f"Master database updated (Monday) with {len(customers_df)} customers")

    # Generate products and orders
    products_df = generate_products(2000)
    upload_to_gcs(customers_df, bucket_name, "customers", f"customers_{date_str}.csv")
    upload_to_gcs(products_df, bucket_name, "products", f"products_{date_str}.csv")
    daily_orders = generate_daily_orders(1, 5000, customers_df, products_df)
    upload_to_gcs(daily_orders, bucket_name, "orders", f"orders_{date_str}.csv")

    print(f"Data generated and uploaded for {date_str}")
    print(f"- Customers: {len(customers_df)} records")
    print(f"- Products: {len(products_df)} records")
    print(f"- Orders: {len(daily_orders)} records")

# Cloud Function entry point
def cloud_function_entry(request):
    """
    HTTP Cloud Function entry point.

    Args:
        request: Flask request object

    Returns:
        Response text
    """
    # Extract request data if needed
    request_json = request.get_json(silent=True)
    force_create = request_json.get('force_create_master', False) if request_json else False

    # Generate and upload data
    generate_and_upload_data(force_create_master=force_create)

    return "Data generation completed successfully"
