import os
from src.config.spark_session import get_spark
from src.bronze.ingest_raw import ingest_properties
from src.silver.clean_properties import clean_properties

from src.gold.city_kpis import generate_city_kpis
from src.gold.company_kpis import generate_company_kpis
from src.gold.property_type_kpis import generate_property_type_kpis
from src.gold.yield_analysis import generate_yield_analysis


def run_pipeline():
    spark = get_spark()

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # base_dir will point to project root folder

    raw_path = os.path.join(base_dir, "data", "raw", "2025-07-28bidwell_spider_items.csv")
    bronze_path = os.path.join(base_dir, "data", "bronze", "properties")
    silver_path = os.path.join(base_dir, "data", "silver", "properties")
    gold_path = os.path.join(base_dir, "data", "gold")

    ingest_properties(spark, raw_path, bronze_path)
    clean_properties(spark, bronze_path, silver_path)

    generate_city_kpis(spark, silver_path, gold_path)
    generate_company_kpis(spark, silver_path, gold_path)
    generate_property_type_kpis(spark, silver_path, gold_path)
    generate_yield_analysis(spark, silver_path, gold_path)

    print("✅ Real Estate Pipeline Executed Successfully!")


if __name__ == "__main__":
    run_pipeline()
