# quickshop_etl/transforms.py
from __future__ import annotations
import pandas as pd
from typing import Tuple

def add_order_total(df_orders: pd.DataFrame) -> pd.DataFrame:
    """
    Compute order_total = qty * unit_price. Coerce qty & unit_price to numeric safely.
    Returns a new DataFrame with 'order_total' column (rounded to 2 decimals).
    """
    df = df_orders.copy()
    # normalize column names (common casing issues)
    # keep original names if already correct
    if 'qty' not in df.columns:
        for c in df.columns:
            if c.lower() == 'qty':
                df = df.rename(columns={c: 'qty'})
    if 'unit_price' not in df.columns:
        for c in df.columns:
            if c.lower() in ('unitprice', 'unit_price', 'price'):
                df = df.rename(columns={c: 'unit_price'})

    # Coerce and fill invalid values defensively
    df['qty'] = pd.to_numeric(df.get('qty', 0), errors='coerce').fillna(0).astype(int)
    df['unit_price'] = pd.to_numeric(df.get('unit_price', 0.0), errors='coerce').fillna(0.0).astype(float)

    # Compute
    df['order_total'] = (df['qty'] * df['unit_price']).round(2)

    return df

def enrich_with_products(df_orders: pd.DataFrame, df_products: pd.DataFrame, how: str = 'left') -> pd.DataFrame:
    """
    Left-join orders with product metadata on 'product_id'.
    Fills missing product_name/category with 'unknown'.
    Returns enriched dataframe.
    """
    # Ensure product_id column exists in both
    left = df_orders.copy()
    right = df_products.copy()

    # Ensure consistent column name for join
    # If product_id typed as float due to NaNs, coerce to int where safe
    left['product_id'] = pd.to_numeric(left['product_id'], errors='coerce')
    right['product_id'] = pd.to_numeric(right['product_id'], errors='coerce')

    enriched = left.merge(
        right[['product_id', 'product_name', 'category', 'price']].rename(columns={'price': 'product_price'}),
        on='product_id',
        how=how,
        validate='m:1'  # expects many orders -> one product
    )

    # Fill missing product metadata
    if 'product_name' in enriched.columns:
        enriched['product_name'] = enriched['product_name'].fillna('unknown_product')
    if 'category' in enriched.columns:
        enriched['category'] = enriched['category'].fillna('unknown_category')

    return enriched

def add_order_date_iso(df_orders: pd.DataFrame, date_col: str = 'order_date', out_col: str = 'order_date_iso') -> pd.DataFrame:
    """
    Add a string column with ISO date (YYYY-MM-DD) derived from a datetime column.
    Useful for partitioning.
    """
    df = df_orders.copy()
    if date_col not in df.columns:
        raise KeyError(f"{date_col} not found in dataframe for add_order_date_iso")
    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    df[out_col] = df[date_col].dt.strftime('%Y-%m-%d')
    return df

def compute_daily_category_revenue(df_enriched: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates revenue by order_date_iso and category.
    Returns dataframe with columns: order_date_iso, category, total_revenue, total_units.
    """
    df = df_enriched.copy()
    if 'order_total' not in df.columns:
        df = add_order_total(df)
    if 'order_date_iso' not in df.columns:
        df = add_order_date_iso(df)

    agg = df.groupby(['order_date_iso', 'category'], dropna=False).agg(
        total_revenue=('order_total', 'sum'),
        total_units=('qty', 'sum'),
    ).reset_index()
    agg['total_revenue'] = agg['total_revenue'].round(2)
    return agg

def top_n_products_by_revenue(df_enriched: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """
    Return top-n products by revenue across all data in df_enriched.
    Columns: product_id, product_name, category, total_revenue, total_units
    """
    df = df_enriched.copy()
    if 'order_total' not in df.columns:
        df = add_order_total(df)
    agg = df.groupby(['product_id', 'product_name', 'category'], dropna=False).agg(
        total_revenue=('order_total', 'sum'),
        total_units=('qty', 'sum')
    ).reset_index()
    agg = agg.sort_values('total_revenue', ascending=False).head(n)
    agg['total_revenue'] = agg['total_revenue'].round(2)
    return agg
