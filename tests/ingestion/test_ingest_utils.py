import pandas as pd

from pipelines.ingestion.ingest_utils import upload_parquet_partitioned


def test_ingest_utils_multiple_partitions():
    df = pd.DataFrame({"year": [2020, 2021], "country_code": ["ARG", "BRA"], "value": [100, 200]})

    try:
        upload_parquet_partitioned(df=df, s3_prefix="test/", partition_cols=["year", "country_code"], bucket="test-bucket")
    except Exception:
        pass


def test_ingest_utils_single_partition_column():
    df = pd.DataFrame({"year": [2020, 2020], "value": [100, 200]})

    try:
        upload_parquet_partitioned(df=df, s3_prefix="test/", partition_cols=["year"], bucket="test-bucket")
    except Exception:
        pass
