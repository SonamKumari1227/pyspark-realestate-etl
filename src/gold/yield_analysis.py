from pyspark.sql.functions import col, when

def generate_yield_analysis(spark, silver_path, gold_path):
    df = spark.read.parquet(silver_path)

    # Yield formula:
    # annual_rent = monthly_rent * 12 (assuming rent is monthly)
    # yield = (annual_rent / property_price) * 100

    df = df.withColumn(
        "annual_rent",
        col("property_rent") * 12
    )

    df = df.withColumn(
        "rental_yield_percent",
        when(
            (col("property_price").isNotNull()) & (col("property_price") > 0),
            (col("annual_rent") / col("property_price")) * 100
        ).otherwise(None)
    )

    yield_table = df.select(
        "property_title",
        "property_address",
        "city",
        "postcode",
        "property_price",
        "property_rent",
        "annual_rent",
        "rental_yield_percent",
        "company_name",
        "property_url"
    ).orderBy(col("rental_yield_percent").desc())

    yield_table.write.mode("overwrite").parquet(gold_path + "/yield_analysis")
    print("✅ Gold yield analysis generated")
