from pyspark.sql.functions import col, trim, lower, to_date, regexp_replace, when
from pyspark.sql.types import DoubleType, IntegerType
from src.utils.common import remove_duplicates, clean_numeric_column

def clean_properties(spark, bronze_path, silver_path):
    df = spark.read.parquet(bronze_path)

    # Standard cleanup
    df = df.withColumn("property_url", trim(col("property_url"))) \
           .withColumn("company_name", trim(col("company_name"))) \
           .withColumn("property_title", trim(col("property_title"))) \
           .withColumn("property_address", trim(col("property_address"))) \
           .withColumn("city", trim(lower(col("city")))) \
           .withColumn("postcode", trim(lower(col("postcode")))) \
           .withColumn("property_type", trim(lower(col("property_type")))) \
           .withColumn("property_status", trim(lower(col("property_status"))))

    # Convert date columns if present
    if "scraped_date" in df.columns:
        df = df.withColumn("scraped_date", to_date(col("scraped_date"), "yyyy-MM-dd"))

    if "lease_expiry_date" in df.columns:
        df = df.withColumn("lease_expiry_date", to_date(col("lease_expiry_date"), "yyyy-MM-dd"))

    # Clean numeric columns if present
    numeric_cols = [
        "property_price",
        "property_rent",
        "property_size",
        "property_latitude",
        "property_longitude"
    ]

    for nc in numeric_cols:
        if nc in df.columns:
            df = clean_numeric_column(df, nc)

    # If property_size is in sqft text like "12,000 sq ft"
    if "property_size" in df.columns:
        df = df.withColumn("property_size", col("property_size").cast(DoubleType()))

    # Standardize vacancy
    if "tenant_name" in df.columns:
        df = df.withColumn(
            "tenant_name",
            when(trim(col("tenant_name")) == "", None).otherwise(trim(col("tenant_name")))
        )

    # Remove duplicate properties based on URL
    if "property_url" in df.columns:
        df = remove_duplicates(df, ["property_url"])

    # Filter invalid rows
    if "property_title" in df.columns:
        df = df.filter(col("property_title").isNotNull())

    df.write.mode("overwrite").parquet(silver_path)

    print(f"✅ Silver cleaning completed: {silver_path}")
