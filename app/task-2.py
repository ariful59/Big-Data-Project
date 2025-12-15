from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, concat_ws, explode, split
from pyspark.sql.types import StringType, ArrayType

spark = SparkSession.builder.appName("task-2").getOrCreate()

# input_df = spark.read \
#     .format("text") \
#     .option("path", "files/hello.txt") \
#     .load()

input_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

 #without sql
# word_list = input_df.select(split(col("value"), ' ').alias('words'))
#
# word_list_explode = word_list.select(explode(col("words")).alias('word_individual'))
#
# average_len = word_list_explode.selectExpr("AVG(LENGTH(word_individual)) as avg_leng")
# average_len.show()

input_df.createOrReplaceTempView("input_data")

average_len_sql = spark.sql("""
    SELECT AVG(LENGTH(word)) as avg_leng
    FROM (
        SELECT explode(split(value, " ")) as word
        FROM input_data where value is not null
    )""")

# average_len_sql.show()

query = average_len_sql.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
