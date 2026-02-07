from pyspark.sql.functions import col, current_timestamp, regexp_replace
from pyspark.sql.types import DoubleType

def add_ingestion_timestamp(df):
    return df.withColumn("ingestion_timestamp", current_timestamp())


def remove_duplicates(df, subset_cols):
    return df.dropDuplicates(subset_cols)


def clean_numeric_column(df, col_name):
    """
    Converts numeric values stored as text:
    Example: "£1,200,000" -> 1200000
    """
    return df.withColumn(
        col_name,
        regexp_replace(col(col_name), "[^0-9.]", "").cast(DoubleType())
    )
