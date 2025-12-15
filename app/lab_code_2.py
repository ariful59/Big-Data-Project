from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, explode
from pyspark.sql.types import IntegerType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("SingleFileProductSales") \
    .getOrCreate()

# BATCH read single file (not streaming)
# inputDF = spark.read \
#     .format("text") \
#     .option("path", "files/hello.txt") \
#     .load()

inputDF = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 8888) \
    .load()

# inputDF.show()


split_df = inputDF.withColumn("split_table", split(col("value"), " ")).drop("value")

# split_df.show()


explode_df = split_df.withColumn("after_explode", explode("split_table"))
# explode_df.show()

# Split and cast quantity to integer
after_spliting = explode_df.select(
    split(col("after_explode"), ",").getItem(0).alias("product"),
    split(col("after_explode"), ",").getItem(1).cast(IntegerType()).alias("quantity")
)
# after_spliting.show()

sales_per_product = after_spliting.groupBy('product').sum('quantity').withColumnRenamed("sum(quantity)", "total sales")
# sales_per_product.show()

output = sales_per_product.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "checkpoints/") \
    .start()

output.awaitTermination()


