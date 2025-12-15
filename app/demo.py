from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, current_timestamp

spark = SparkSession.builder.appName("SocketWordCount").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# IMPORTANT: use the Docker service name "nc" as the host
lines = (spark.readStream
         .format("socket")
         .option("host", "host.docker.internal")
         .option("port", 8888)
         .load())

words = lines.select(explode(split(col("value"), r"\s+")).alias("word")) \
    .where(col("word") != "")

counts = words.groupBy("word").count()

query = (counts.writeStream
         .outputMode("complete")  # shows only updated counts
         .format("console")
         .option("truncate", "false")
         .start())

query.awaitTermination()
