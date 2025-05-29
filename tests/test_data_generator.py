import sys
import os
from faker import Faker

sys.path.insert(0, os.path.abspath('cloud_functions/generate_customers_daily'))

from main import generate_initial_b2b_customers

def test_generate_initial_b2b_customers():
    fake = Faker()
    Faker.seed(42)
    df = generate_initial_b2b_customers(n=10, fake=fake)
    assert len(df) == 10
    for col in [
        'customer_id', 'company_name', 'vat_number', 'address', 'postal_code',
        'city', 'country', 'currency', 'email', 'phone', 'industry', 'created_at',
        'last_modified', 'customer_segment', 'is_active', 'modification_history'
    ]:
        assert col in df.columns
