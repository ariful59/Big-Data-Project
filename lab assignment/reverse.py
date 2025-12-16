

from pyspark.sql import SparkSession
from pyspark.sql.functions import reverse

spark = SparkSession.builder.appName("reverse").config("spark.executor.memory", "2g").getOrCreate()

input = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

reverse_line = input.select(reverse(input.value).alias("reversed_data"))

output = reverse_line.writeStream.outputMode("update").format("console").start()

output.awaitTermination()