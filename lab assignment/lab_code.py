from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when, length
from pyspark.sql.types import StringType, IntegerType

# Initialize a more configured Spark session
spark = SparkSession.builder \
    .appName("EnhancedUDFExample") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Define a complex UDF that does text processing
def complex_text_processing(input_str):
    # Simple text processing: reversing and converting to upper case
    return "Processed: " + input_str[::-1].upper()


# Register the UDF with Spark
complex_text_udf = udf(complex_text_processing, StringType())


# Another UDF to calculate the length of a string
def string_length(input_str):
    return len(input_str)


length_udf = udf(string_length, IntegerType())
# Define the streaming input DataFrame to read data from a socket


inputStream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()


# Use the UDFs to transform the input stream
transformedStream = inputStream.select(
    complex_text_udf(inputStream.value).alias("processed_text"),
    length_udf(inputStream.value).alias("original_length")
)


# Further transformation: marking if original length is > 10
transformedStream = transformedStream.withColumn(
    "long_text",
    when(col("original_length") > 10, "Yes").otherwise("No")
)

# Output the transformed data to the console
query = transformedStream.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
# Await termination of the query
query.awaitTermination()
