from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, concat_ws
from pyspark.sql.types import StringType, ArrayType

spark = SparkSession.builder.appName("task-1").getOrCreate()

# input_df = spark.read \
#     .format("text") \
#     .option("path", "files/hello.txt") \
#     .load()

input_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()


# casted_df = input_df.selectExpr("CAST(value AS INT) as number")
# even_numbers = casted_df.filter(col("number") % 2 == 0)
#
# even_numbers.show()

input_df.createOrReplaceTempView("numbers")
even_numbers = spark.sql("""
    SELECT CAST(value AS INT) as even_number
    FROM numbers
    WHERE CAST(value AS INT) % 2 = 0
""")

# even_numbers.show()

query = even_numbers.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
