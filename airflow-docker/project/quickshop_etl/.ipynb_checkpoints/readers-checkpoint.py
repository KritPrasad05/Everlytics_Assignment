# quickshop_etl/readers.py
from pathlib import Path
from typing import Tuple
import pandas as pd
import os

# Try to import config.resolve_data_dir if you later centralize it; otherwise the function below works standalone.
def find_project_root(start: Path) -> Path | None:
    PROJECT_MARKERS = {"pyproject.toml", "setup.py", "requirements.txt", ".git"}
    cur = start.resolve()
    for _ in range(10):
        if (cur / "data").exists():
            return cur
        if any((cur / m).exists() for m in PROJECT_MARKERS):
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return None

def resolve_data_dir(explicit: str | None = None) -> Path:
    """
    Resolve data directory with fallback logic:
      1. explicit argument
      2. project-relative (based on __file__ when module)
      3. search upward from cwd for a 'data' folder or project marker
      4. /mnt/data (common in hosted sandboxes)
      5. cwd()
    Works in both Jupyter and module contexts.
    """
    if explicit:
        p = Path(explicit)
        if p.exists():
            return p.resolve()

    # 2: if running as a module (this file), compute project root
    try:
        this_file = Path(__file__).resolve()
        project_root = this_file.parent.parent
        candidate = project_root / "data"
        if candidate.exists():
            return candidate
    except NameError:
        # __file__ not defined (e.g., Jupyter). fall through.
        pass

    # 3: search upward from cwd
    cwd = Path.cwd()
    root = find_project_root(cwd)
    if root:
        candidate = root / "data"
        if candidate.exists():
            return candidate.resolve()

    # 4: fallback to /mnt/data
    mnt = Path("/mnt/data")
    if mnt.exists() and any(mnt.iterdir()):
        return mnt

    # 5: final fallback
    return cwd.resolve()

# -------------------------
# Flexible date parser (copy of the notebook-tested function)
# -------------------------
def parse_date_column(df: pd.DataFrame, column_name: str, date_format: str | None = "%Y-%m-%d") -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Parse a date column safely.
    Returns (good_df, bad_df). Good rows have column_name as datetime64[ns].
    bad_df contains original columns + '_parse_error'.
    date_format: if None -> let pandas infer (slower).
    """
    df = df.copy()
    if column_name not in df.columns:
        raise KeyError(f"Column '{column_name}' not in dataframe")

    # 1) strict parse using format (if provided)
    if date_format:
        parsed = pd.to_datetime(df[column_name], format=date_format, errors="coerce")
    else:
        parsed = pd.to_datetime(df[column_name], errors="coerce")

    bad_mask = parsed.isna()
    bad_df = df[bad_mask].copy()
    if not bad_df.empty:
        bad_df["_parse_error"] = f"Failed to parse '{column_name}' with format='{date_format}'"

    good_df = df[~bad_mask].copy()
    good_df[column_name] = parsed[~bad_mask]

    # 2) fallback: if some rows failed, try flexible parse on those rows
    if not bad_df.empty:
        fallback_parsed = pd.to_datetime(bad_df[column_name], errors="coerce")
        still_bad_mask = fallback_parsed.isna()
        fallback_good = bad_df[~still_bad_mask].copy()
        fallback_good[column_name] = fallback_parsed[~still_bad_mask]
        final_bad = bad_df[still_bad_mask].copy()
        final_bad["_parse_error"] = "Flexible parsing also failed"
        # combine good sets
        good_df = pd.concat([good_df, fallback_good], ignore_index=True)
        bad_df = final_bad.reset_index(drop=True)

    return good_df.reset_index(drop=True), bad_df.reset_index(drop=True)

# -------------------------
# Readers (return good_df, bad_df)
# -------------------------
def read_products(data_dir: str | None = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    base = Path(data_dir) if data_dir else resolve_data_dir()
    p = base / "products.csv"
    if not p.exists():
        raise FileNotFoundError(f"products.csv not found in {base}")
    df = pd.read_csv(p)
    return df, pd.DataFrame()  # no date parsing needed; validation happens elsewhere

def read_inventory(data_dir: str | None = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    base = Path(data_dir) if data_dir else resolve_data_dir()
    p = base / "inventory.csv"
    if not p.exists():
        raise FileNotFoundError(f"inventory.csv not found in {base}")
    df = pd.read_csv(p)
    # parse last_restock_date -> datetime
    good, bad = parse_date_column(df, "last_restock_date", date_format="%Y-%m-%d")
    return good, bad

def read_orders_for_date(date_str: str, data_dir: str | None = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Finds order file that contains date_str in its filename (e.g., '20251025') and 'order' in the name.
    Returns (good_orders_df, bad_orders_df) where 'order_date' is datetime in good_df.
    """
    base = Path(data_dir) if data_dir else resolve_data_dir()
    candidates = [f for f in base.iterdir() if f.is_file() and date_str in f.name and 'order' in f.name.lower()]
    if not candidates:
        raise FileNotFoundError(f"No order file for date {date_str} in {base}")
    path = candidates[0]
    df = pd.read_csv(path)
    good, bad = parse_date_column(df, "order_date", date_format="%Y-%m-%d")
    return good, bad
