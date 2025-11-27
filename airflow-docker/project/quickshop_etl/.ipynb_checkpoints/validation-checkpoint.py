# quickshop_etl/validation.py
from pydantic import BaseModel, Field, conint, confloat, validator
from typing import Optional, Any
from datetime import datetime
import pandas as pd

# --- Schemas ---
class ProductSchema(BaseModel):
    product_id: int
    product_name: str
    category: Optional[str] = "unknown"
    price: confloat(ge=0)

class InventorySchema(BaseModel):
    product_id: int
    warehouse_id: str
    stock_on_hand: conint(ge=0)
    last_restock_date: datetime

class OrderSchema(BaseModel):
    order_id: int
    order_date: datetime
    product_id: int
    qty: conint(ge=0)
    unit_price: confloat(ge=0)
    user_id: Optional[int] = None
    order_status: Optional[str] = "completed"

# --- Helper: row-by-row validation for a DataFrame ---
def validate_dataframe(df: pd.DataFrame, schema: BaseModel) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate rows of df against pydantic schema class `schema` (a subclass of BaseModel).
    Returns (valid_df, bad_rows_df). Each bad row includes original columns + '_error'.
    Note: df must already have any date columns parsed to datetime objects.
    """
    good_rows = []
    bad_rows = []
    SchemaClass = schema

    for idx, row in df.reset_index(drop=True).iterrows():
        try:
            # convert pandas types to plain python types where needed (e.g., numpy ints)
            obj = row.to_dict()
            # For pandas Timestamp -> convert to python datetime
            for k, v in obj.items():
                if isinstance(v, (pd.Timestamp,)):
                    obj[k] = v.to_pydatetime()
            SchemaClass.parse_obj(obj)
            good_rows.append(row)
        except Exception as e:
            err = str(e)
            bad = row.to_dict()
            bad['_error'] = err
            bad_rows.append(bad)

    import pandas as _pd
    return _pd.DataFrame(good_rows).reset_index(drop=True), _pd.DataFrame(bad_rows).reset_index(drop=True)
