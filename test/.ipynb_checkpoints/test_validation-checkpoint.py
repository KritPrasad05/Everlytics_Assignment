# quickshop_etl/tests/test_validation.py

import pandas as pd
from datetime import datetime

from quickshop_etl.validation import (
    validate_dataframe,
    OrderSchema,
    ProductSchema,
    InventorySchema,
)

# OrderSchema validation tests

def test_order_validation_success():
    df = pd.DataFrame([
        {
            'order_id': 50001,
            'order_date': pd.to_datetime('2025-10-25'),
            'product_id': 1001,
            'qty': 2,
            'unit_price': 19.99,
            'user_id': 9001,
            'order_status': 'completed'
        }
    ])
    
    good, bad = validate_dataframe(df, OrderSchema)
    
    assert len(good) == 1
    assert len(bad) == 0

def test_order_validation_negative_qty():
    df = pd.DataFrame([
        {
            'order_id': 50002,
            'order_date': pd.to_datetime('2025-10-25'),
            'product_id': 1001,
            'qty': -1,
            'unit_price': 19.99,
            'user_id': 9001,
            'order_status': 'completed'
        }
    ])
    
    good, bad = validate_dataframe(df, OrderSchema)
    
    assert len(good) == 0
    assert len(bad) == 1
    assert '_error' in bad.columns

# ProductSchema validation tests

def test_product_validation_success():
    df = pd.DataFrame([
        {
            'product_id': 1001,
            'product_name': 'Classic Tee',
            'category': 'Apparel',
            'price': 19.99
        }
    ])
    
    good, bad = validate_dataframe(df, ProductSchema)
    
    assert len(good) == 1
    assert len(bad) == 0

def test_product_validation_negative_price():
    df = pd.DataFrame([
        {
            'product_id': 1001,
            'product_name': 'Classic Tee',
            'category': 'Apparel',
            'price': -10.00
        }
    ])
    
    good, bad = validate_dataframe(df, ProductSchema)
    
    assert len(good) == 0
    assert len(bad) == 1
    assert '_error' in bad.columns

# InventorySchema validation tests

def test_inventory_validation_success():
    df = pd.DataFrame([
        {
            'product_id': 1001,
            'warehouse_id': 'W1',
            'stock_on_hand': 120,
            'last_restock_date': pd.to_datetime('2025-10-18')
        }
    ])
    
    good, bad = validate_dataframe(df, InventorySchema)
    
    assert len(good) == 1
    assert len(bad) == 0


def test_inventory_validation_bad_stock():
    df = pd.DataFrame([
        {
            'product_id': 1001,
            'warehouse_id': 'W1',
            'stock_on_hand': -5,
            'last_restock_date': pd.to_datetime('2025-10-18')
        }
    ])
    
    good, bad = validate_dataframe(df, InventorySchema)
    
    assert len(good) == 0
    assert len(bad) == 1
    assert '_error' in bad.columns
