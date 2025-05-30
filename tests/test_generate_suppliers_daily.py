import os
import importlib.util

def test_generate_suppliers():
    """Test that supplier generation produces expected data structure."""
    # Charge directement le fichier main.py des suppliers
    main_path = os.path.join(os.getcwd(), 'cloud_functions', 'generate_suppliers_daily', 'main.py')
    
    spec = importlib.util.spec_from_file_location("suppliers_main", main_path)
    suppliers_main = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(suppliers_main)
    
    # Test
    df = suppliers_main.generate_suppliers(n=10)
    assert len(df) >= 10
    expected_columns = [
        'supplier_id', 'company_name', 'service_type', 'address', 'postal_code',
        'city', 'country', 'email', 'phone', 'created_at', 'last_modified',
        'is_active', 'modification_history'
    ]
    for col in expected_columns:
        assert col in df.columns
    
    print(f"✅ Suppliers test passed: {len(df)} rows generated")
