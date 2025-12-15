from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("NumberDoubler") \
    .getOrCreate()


# Define a simple UDF to double a number
def double_number(num):
    try:
        return float(num) * 2
    except:
        return None  # Return None if the conversion to float fails


# Register the UDF with Spark
double_udf = udf(double_number, DoubleType())
# Define the streaming input DataFrame to read data from a socket
inputStream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()
# Use the UDF to transform the input stream
doubledStream = inputStream.select(double_udf(inputStream.value).alias("doubled_value"))
# Output the transformed data to the console
query = doubledStream.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()
# Await termination of the query
query.awaitTermination()
