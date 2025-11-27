# quickshop_etl/writers.py
from __future__ import annotations
from pathlib import Path
import pandas as pd
import json
import tempfile
import os
import shutil
from typing import Optional, Dict

def _atomic_write_file(src: Path, dst: Path):
    """
    Atomically move src -> dst (on same filesystem if possible).
    Uses os.replace for atomic rename.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    # use os.replace which overwrites if dst exists atomically on most OSes
    os.replace(str(src), str(dst))

def write_parquet_partition(df: pd.DataFrame, partition_value: str, output_dir: Path | str = "output/processed") -> Path:
    """
    Write df to Parquet file under output_dir/date=partition_value/data.parquet atomically.
    Returns final Path to file.
    """
    outdir = Path(output_dir) / f"date={partition_value}"
    outdir_tmp = Path(tempfile.mkdtemp(prefix="tmp_parquet_"))
    try:
        tmp_file = outdir_tmp / "data.parquet"
        # Use pandas to_parquet which delegates to pyarrow
        df.to_parquet(tmp_file, engine='pyarrow', index=False)
        final_file = outdir / "data.parquet"
        # ensure outdir exists
        outdir.mkdir(parents=True, exist_ok=True)
        _atomic_write_file(tmp_file, final_file)
    finally:
        # cleanup temp dir if exists
        try:
            shutil.rmtree(outdir_tmp)
        except Exception:
            pass
    return final_file

def write_summary_json(summary: Dict, output_dir: Path | str = "output/summaries") -> Path:
    """
    Write a JSON summary file. Example summary keys: date, rows_processed, revenue.
    Creates unique filename summary_{date}.json if 'date' key present.
    """
    outdir = Path(output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    filename = f"summary_{summary.get('date','run')}.json"
    tmp = outdir / (filename + ".tmp")
    final = outdir / filename
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)
    os.replace(str(tmp), str(final))
    return final

def write_bad_rows_csv(bad_df: pd.DataFrame, name: str, output_dir: Path | str = "output/bad_rows") -> Optional[Path]:
    """
    Write bad_rows DataFrame (could be empty). Returns path or None if empty.
    name: friendly name like 'orders-20251025'
    """
    outdir = Path(output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    if bad_df is None or bad_df.empty:
        return None
    target = outdir / f"{name}.csv"
    tmp = outdir / f".{name}.csv.tmp"
    bad_df.to_csv(tmp, index=False)
    os.replace(str(tmp), str(target))
    return target
