"""
ETL CLI: read -> validate -> transform -> write (per date).

Functions:
 - run_for_date(date_str, data_dir=None, output_dir=None, dry_run=False)
 - main() for CLI usage

run_for_date returns a summary dict:
{
  'date': '20251025',
  'rows': <int processed>,
  'revenue': <float total revenue>,
  'bad_rows': <int bad rows>,
  'parquet_path': <str or None>
}
"""
from argparse import ArgumentParser
from datetime import datetime, timedelta
from pathlib import Path
import logging
from typing import Optional, Dict, Any, Tuple

from .readers import resolve_data_dir, read_orders_for_date, read_products, read_inventory
from .validation import validate_dataframe, OrderSchema, ProductSchema, InventorySchema
from .transforms import add_order_total, enrich_with_products, add_order_date_iso
from .writers import write_parquet_partition, write_summary_json, write_bad_rows_csv

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def run_for_date(
    date_str: str,
    data_dir: Optional[str] = None,
    output_dir: Optional[str] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Run ETL pipeline for a single date. Returns summary dict.

    - date_str: 'YYYYMMDD' (e.g., '20251025')
    - data_dir: optional override path to data folder
    - output_dir: optional override for outputs (defaults to project 'output' folder)
    - dry_run: if True, do not write parquet/summary/bad_rows, but still return summary
    """
    # Resolve directories
    data_path = Path(data_dir) if data_dir else resolve_data_dir()
    out_base = Path(output_dir) if output_dir else Path.cwd() / "output"

    logger.info("Starting ETL for date %s (data_dir=%s, output_dir=%s, dry_run=%s)",
                date_str, str(data_path), str(out_base), dry_run)

    # 1) Read products (metadata)
    products_df, products_bad = read_products(str(data_path))
    if products_bad is not None and not products_bad.empty:
        logger.warning("Found %d bad product rows", len(products_bad))
        # Write bad rows for inspection (even in dry-run we may skip writing — here we log only)
        if not dry_run:
            write_bad_rows_csv(products_bad, name="products-bad", output_dir=out_base / "bad_rows")

    # 2) Read orders for the date (date parsing done in reader)
    orders_parsed_good, orders_parsed_bad = read_orders_for_date(date_str, data_dir=str(data_path))

    # 3) Validate parsed orders (schema checks)
    orders_valid, orders_valid_bad = validate_dataframe(orders_parsed_good, OrderSchema)

    # Combine bad rows from parsing and validation
    bad_rows_combined = []
    if orders_parsed_bad is not None and not orders_parsed_bad.empty:
        bad_rows_combined.append(orders_parsed_bad)
    if orders_valid_bad is not None and not orders_valid_bad.empty:
        bad_rows_combined.append(orders_valid_bad)
    if bad_rows_combined:
        import pandas as pd
        bad_orders_df = pd.concat(bad_rows_combined, ignore_index=True)
    else:
        bad_orders_df = None

    # 4) If no valid orders, still return summary and write bad rows
    if orders_valid is None or orders_valid.empty:
        summary = {
            "date": date_str,
            "rows": 0,
            "revenue": 0.0,
            "bad_rows": len(bad_orders_df) if bad_orders_df is not None else 0,
            "parquet_path": None,
        }
        if bad_orders_df is not None and not bad_orders_df.empty and not dry_run:
            write_bad_rows_csv(bad_orders_df, name=f"orders-{date_str}-bad", output_dir=out_base / "bad_rows")
        logger.info("No valid orders for %s; returning summary: %s", date_str, summary)
        return summary

    # 5) Transform: add order_total
    enriched = add_order_total(orders_valid)

    # 6) Join with product metadata
    enriched = enrich_with_products(enriched, products_df)

    # 7) Add partition column order_date_iso for partitioning
    enriched = add_order_date_iso(enriched, date_col="order_date", out_col="order_date_iso")

    # 8) Compute summary metrics
    total_revenue = float(enriched['order_total'].sum().round(2)) if 'order_total' in enriched.columns else 0.0
    rows_processed = len(enriched)

    # 9) Writers (respect dry_run)
    parquet_path = None
    if not dry_run:
        # Use the order_date_iso of the first row (all rows should have same date since read per date)
        partition_value = enriched['order_date_iso'].iloc[0]
        parquet_path = write_parquet_partition(enriched, partition_value=partition_value, output_dir=out_base / "processed")
        # write summary JSON
        summary_obj = {
            "date": partition_value,
            "rows": rows_processed,
            "revenue": total_revenue,
        }
        write_summary_json(summary_obj, output_dir=out_base / "summaries")
        # write bad rows if exist
        if bad_orders_df is not None and not bad_orders_df.empty:
            write_bad_rows_csv(bad_orders_df, name=f"orders-{date_str}-bad", output_dir=out_base / "bad_rows")

    # 10) Return summary
    summary_out = {
        "date": date_str,
        "rows": rows_processed,
        "revenue": total_revenue,
        "bad_rows": int(len(bad_orders_df)) if bad_orders_df is not None else 0,
        "parquet_path": str(parquet_path) if parquet_path else None,
    }

    logger.info("ETL finished for %s: %s", date_str, summary_out)
    return summary_out


def parse_args() -> ArgumentParser:
    p = ArgumentParser(description="QuickShop ETL runner")
    p.add_argument("--start-date", required=True, help="YYYYMMDD or YYYY-MM-DD")
    p.add_argument("--end-date", required=False, help="YYYYMMDD or YYYY-MM-DD")
    p.add_argument("--data-dir", required=False, help="Override data directory (path)")
    p.add_argument("--output-dir", required=False, help="Override output directory (path)")
    p.add_argument("--dry-run", action="store_true", help="Do not write outputs; run the pipeline and return summary")
    return p


def _normalize_date_str(s: str) -> str:
    if "-" in s:
        return s.replace("-", "")
    return s


def main():
    args = parse_args().parse_args()
    start = _normalize_date_str(args.start_date)
    end = _normalize_date_str(args.end_date) if args.end_date else start

    start_dt = datetime.strptime(start, "%Y%m%d")
    end_dt = datetime.strptime(end, "%Y%m%d")

    cur = start_dt
    results = {}
    while cur <= end_dt:
        ds = cur.strftime("%Y%m%d")
        try:
            res = run_for_date(ds, data_dir=args.data_dir, output_dir=args.output_dir, dry_run=args.dry_run)
            results[ds] = res
        except Exception as e:
            logger.exception("ETL failed for %s: %s", ds, e)
            results[ds] = {"error": str(e)}
        cur = cur + timedelta(days=1)

    # Print results
    print(results)


if __name__ == "__main__":
    main()
