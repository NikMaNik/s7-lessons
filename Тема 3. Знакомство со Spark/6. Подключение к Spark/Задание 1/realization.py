from pyspark.sql import SparkSession
spark = (
    SparkSession
    .builder
    .config("spark.executor.memory", "1g")
    .config("spark.executor.cores", 2)
    .appName("My first session")
    .getOrCreate()
)