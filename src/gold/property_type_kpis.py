from pyspark.sql.functions import count, avg

def generate_property_type_kpis(spark, silver_path, gold_path):
    df = spark.read.parquet(silver_path)

    property_type_kpis = df.groupBy("property_type").agg(
        count("*").alias("total_properties"),
        avg("property_rent").alias("avg_rent"),
        avg("property_price").alias("avg_price")
    )

    property_type_kpis.write.mode("overwrite").parquet(gold_path + "/property_type_kpis")
    print("✅ Gold property type KPIs generated")
