from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SocketStreamTest").getOrCreate()

df = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()


query = df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("checkpointLocation", "/tmp/spark-checkpoint/test1") \
    .start()

query.awaitTermination()
