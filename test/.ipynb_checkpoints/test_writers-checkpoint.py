import pandas as pd
import tempfile
from quickshop_etl.writers import write_parquet_partition, write_summary_json, write_bad_rows_csv

def test_write_parquet_partition(tmp_path):
    df = pd.DataFrame([{'a':1,'b':'x'}])
    out = write_parquet_partition(df, partition_value='2025-10-25', output_dir=tmp_path/'processed')
    assert out.exists()

def test_write_summary_and_bad_rows(tmp_path):
    summary = {'date':'2025-10-25','rows':2,'revenue':100.0}
    s = write_summary_json(summary, output_dir=tmp_path/'summaries')
    assert s.exists()
    bad = pd.DataFrame([{'col':1,'_error':'x'}])
    b = write_bad_rows_csv(bad, name='orders-2025-10-25', output_dir=tmp_path/'bad_rows')
    assert b.exists()
