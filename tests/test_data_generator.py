import pytest
import pandas as pd
from cloud_functions.data_generator.main import generate_initial_b2b_customers, generate_products

def test_generate_initial_b2b_customers():
    """Test that customer generation produces expected data structure."""
    # Generate a small test dataset
    df = generate_initial_b2b_customers(n=10)

    # Check that we have the right number of columns
    expected_columns = [
        'customer_id', 'company_name', 'vat_number', 'address', 'postal_code',
        'city', 'country', 'email', 'phone', 'industry', 'created_at',
        'last_modified', 'customer_segment', 'is_active', 'modification_history'
    ]
    assert all(col in df.columns for col in expected_columns)

    # Check that we have at least 10 rows (could be more due to duplicates)
    assert len(df) >= 10

    # Check data types
    assert df['customer_id'].dtype == 'object'
    assert df['is_active'].dtype == 'bool'

def test_generate_products():
    """Test that product generation produces expected data structure."""
    # Generate a small test dataset
    df = generate_products(n=10)

    # Check that we have the right columns
    expected_columns = [
        'product_id', 'product_name', 'category', 'subcategory', 'price',
        'cost', 'weight_kg', 'in_stock', 'created_at'
    ]
    assert all(col in df.columns for col in expected_columns)

    # Check that we have exactly 10 rows
    assert len(df) == 10

    # Check data types
    assert df['product_id'].dtype == 'object'
    assert df['price'].dtype == 'float64'
    assert df['in_stock'].dtype == 'bool'
