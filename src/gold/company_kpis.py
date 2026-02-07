from pyspark.sql.functions import count, avg, sum, col, when

def generate_company_kpis(spark, silver_path, gold_path):
    df = spark.read.parquet(silver_path)

    company_kpis = df.groupBy("company_name").agg(
        count("*").alias("total_properties"),
        avg("property_rent").alias("avg_rent"),
        avg("property_price").alias("avg_price"),
        avg("property_size").alias("avg_size"),
        count(when(col("tenant_name").isNotNull(), True)).alias("leased_properties"),
        count(when(col("tenant_name").isNull(), True)).alias("vacant_properties")
    )

    company_kpis.write.mode("overwrite").parquet(gold_path + "/company_kpis")
    print("✅ Gold company KPIs generated")
