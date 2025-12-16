from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, concat_ws
from pyspark.sql.types import StringType, ArrayType

spark = SparkSession.builder.appName("task-1").getOrCreate()

first_name = "Ariful"
last_name = "Amin"

def add_name(value):
    result = []
    for word in value.split(' '):
        temp = first_name + word + last_name
        result.append(temp)
    return ','.join(result)

add_name_udf = udf(add_name, StringType())

# input_df = spark.read \
#     .format("text") \
#     .option("path", "files/hello.txt") \
#     .load()

input_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

modified_input = input_df.select(
    add_name_udf(col("value")).alias("modified_name")
)

# modified_input.show(truncate=False)

query = modified_input.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
