import pandas as pd
from quickshop_etl.transforms import add_order_total, enrich_with_products, add_order_date_iso

def test_add_order_total():
    df = pd.DataFrame([{'order_id':1,'product_id':1001,'qty':2,'unit_price':10.0}])
    out = add_order_total(df)
    assert 'order_total' in out.columns
    assert out.loc[0,'order_total'] == 20.0

def test_enrich_and_date_iso():
    orders = pd.DataFrame([{'order_id':1,'product_id':1001,'qty':1,'unit_price':2.0,'order_date':pd.to_datetime('2025-10-25')}])
    products = pd.DataFrame([{'product_id':1001,'product_name':'X','category':'cat','price':2.0}])
    enriched = enrich_with_products(orders, products)
    enriched = add_order_date_iso(enriched)
    assert 'product_name' in enriched.columns
    assert enriched.loc[0,'order_date_iso'] == '2025-10-25'
