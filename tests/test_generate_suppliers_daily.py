import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath('cloud_functions/generate_suppliers_daily'))

from main import generate_suppliers

def test_generate_suppliers():
    """Test that supplier generation produces expected data structure."""
    df = generate_suppliers(n=10)

    # Vérifie qu'on a au moins 10 lignes (peut être plus avec les doublons)
    assert len(df) >= 10

    # Vérifie la présence de toutes les colonnes attendues
    expected_columns = [
        'supplier_id', 'company_name', 'service_type', 'address', 'postal_code',
        'city', 'country', 'email', 'phone', 'created_at', 'last_modified',
        'is_active', 'modification_history'
    ]
    for col in expected_columns:
        assert col in df.columns

    # Vérifie les types de données
    assert df['supplier_id'].dtype == 'object'
    assert df['is_active'].dtype == 'bool'

    # Vérifie que les IDs sont bien formatés (S + 6 chiffres ou DUP + 6 chiffres)
    assert all(df['supplier_id'].str.match(r'^(S|DUP)\d{6}$'))

def test_generate_suppliers_tuesday_duplicates():
    """Test that suppliers generate duplicates on Tuesday."""
    # Simule un mardi (weekday = 1)
    tuesday_date = datetime(2024, 1, 2).date()  # Un mardi
    df = generate_suppliers(n=20, duplicate_rate=0.1, date=tuesday_date)

    # Avec 10% de doublons sur 20 éléments, on devrait avoir au moins 22 lignes
    assert len(df) >= 22

    # Vérifie qu'il y a des IDs qui commencent par "DUP"
    dup_ids = df[df['supplier_id'].str.startswith('DUP')]
    assert len(dup_ids) > 0
