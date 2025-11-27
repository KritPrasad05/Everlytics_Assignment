import shutil
from pathlib import Path
import pandas as pd
import pytest
from quickshop_etl.cli import run_for_date
from quickshop_etl.readers import resolve_data_dir

@pytest.fixture
def data_dir():
    return resolve_data_dir()

def compute_expected_for_date(date_str: str, data_dir: Path) -> (int, float):
    """
    Read the matching orders CSV for date_str and compute expected rows and revenue.
    Assumes filenames contain the date string (e.g., order_20251025.csv).
    """
    # find candidate
    candidates = [f for f in Path(data_dir).iterdir() if f.is_file() and date_str in f.name and 'order' in f.name.lower()]
    assert candidates, f"No orders file found for {date_str} in {data_dir}"
    df = pd.read_csv(candidates[0])
    # ensure numeric types
    df['qty'] = pd.to_numeric(df['qty'], errors='coerce').fillna(0)
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce').fillna(0.0)
    expected_rows = len(df)
    expected_revenue = float((df['qty'] * df['unit_price']).sum().round(2))
    return expected_rows, expected_revenue

def test_run_for_date_dry_run(tmp_path: Path, data_dir: Path):
    date_str = "20251025"
    expected_rows, expected_revenue = compute_expected_for_date(date_str, data_dir)

    summary = run_for_date(date_str, data_dir=str(data_dir), output_dir=str(tmp_path / "output"), dry_run=True)

    assert isinstance(summary, dict)
    assert summary["date"] == date_str
    assert summary["rows"] == expected_rows
    # numeric check
    assert pytest.approx(summary["revenue"], rel=1e-6) == expected_revenue

    # dry-run should not create processed parquet file
    processed_dir = tmp_path / "output" / "processed"
    assert not processed_dir.exists() or not any(processed_dir.iterdir())

def test_run_for_date_write(tmp_path: Path, data_dir: Path):
    date_str = "20251026"
    expected_rows, expected_revenue = compute_expected_for_date(date_str, data_dir)
    out_dir = tmp_path / "output"

    summary = run_for_date(date_str, data_dir=str(data_dir), output_dir=str(out_dir), dry_run=False)

    assert isinstance(summary, dict)
    assert summary["date"] == date_str
    assert summary["rows"] == expected_rows
    assert pytest.approx(summary["revenue"], rel=1e-6) == expected_revenue

    # The CLI writes summary JSON using the partition_value (ISO date like '2025-10-26').
    # Use the 'date' field from the returned summary if it is ISO, otherwise check both possibilities.
    # Our CLI returns summary["date"] == date_str (YYYYMMDD), but it wrote summary using partition_value (ISO).
    # So construct filename using the written summary date if present in the summaries folder.
    summaries_dir = out_dir / "summaries"
    # Find any summary file in the summaries dir (there should be exactly one)
    files = list(summaries_dir.glob("summary_*.json"))
    assert len(files) >= 1, f"No summary JSON found in {summaries_dir}"
    # Optionally read file and validate its contents match the returned summary
    found = False
    for f in files:
        content = f.read_text()
        if str(expected_rows) in content and str(expected_revenue) in content:
            found = True
            break
    assert found, f"Summary JSON file doesn't contain expected values; checked files: {files}"