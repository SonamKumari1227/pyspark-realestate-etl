from pyspark.sql import SparkSession

def get_spark(app_name="RealEstatePipeline"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark
