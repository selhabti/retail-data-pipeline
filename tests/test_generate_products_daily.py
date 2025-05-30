import sys
import os

sys.path.insert(0, os.path.abspath('cloud_functions/generate_products_daily'))

from main import generate_products

def test_generate_products():
    """Test that product generation produces expected data structure."""
    df = generate_products(n=10)

    # Vérifie qu'on a exactement 10 lignes
    assert len(df) == 10

    # Vérifie la présence de toutes les colonnes attendues
    expected_columns = [
        'product_id', 'product_name', 'category', 'price', 'cost',
        'weight_kg', 'in_stock', 'created_at'
    ]
    for col in expected_columns:
        assert col in df.columns

    # Vérifie les types de données
    assert df['product_id'].dtype == 'object'
    assert df['price'].dtype == 'float64'
    assert df['cost'].dtype == 'float64'
    assert df['weight_kg'].dtype == 'float64'
    assert df['in_stock'].dtype == 'bool'

    # Vérifie que les IDs sont bien formatés
    assert all(df['product_id'].str.startswith('P'))
    assert all(df['product_id'].str.len() == 6)  # P + 5 chiffres

    # Vérifie que les prix sont positifs
    assert all(df['price'] > 0)
    assert all(df['cost'] > 0)
    assert all(df['weight_kg'] > 0)
