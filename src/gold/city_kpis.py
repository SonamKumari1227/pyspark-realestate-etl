from pyspark.sql.functions import count, avg, sum, col

def generate_city_kpis(spark, silver_path, gold_path):
    df = spark.read.parquet(silver_path)

    city_kpis = df.groupBy("city").agg(
        count("*").alias("total_properties"),
        avg("property_rent").alias("avg_rent"),
        avg("property_price").alias("avg_price"),
        avg("property_size").alias("avg_property_size")
    )

    city_kpis.write.mode("overwrite").parquet(gold_path + "/city_kpis")
    print("✅ Gold city KPIs generated")
