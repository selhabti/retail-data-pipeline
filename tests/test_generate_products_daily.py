import os
import importlib.util

def test_generate_products():
    """Test that product generation produces expected data structure."""
    # Charge directement le fichier main.py des products
    main_path = os.path.join(os.getcwd(), 'cloud_functions', 'generate_products_daily', 'main.py')
    
    spec = importlib.util.spec_from_file_location("products_main", main_path)
    products_main = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(products_main)
    
    # Test
    df = products_main.generate_products(n=10)
    assert len(df) == 10
    expected_columns = [
        'product_id', 'product_name', 'category', 'price', 'cost',
        'weight_kg', 'in_stock', 'created_at'
    ]
    for col in expected_columns:
        assert col in df.columns
    
    print(f"✅ Products test passed: {len(df)} rows generated")
