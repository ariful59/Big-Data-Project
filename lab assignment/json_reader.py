from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("json_reader").config("spark.executor.memory", "2g").getOrCreate()

