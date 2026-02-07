from src.utils.common import add_ingestion_timestamp

def ingest_properties(spark, raw_path, bronze_path):
    df = spark.read.option("header", True).option("inferSchema", True).csv(raw_path)

    df = add_ingestion_timestamp(df)

    df.write.mode("overwrite").parquet(bronze_path)
    print(f"✅ Bronze ingestion completed: {bronze_path}")
