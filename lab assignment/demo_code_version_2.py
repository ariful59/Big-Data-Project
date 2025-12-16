from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("SimpleStructuredStreaming") \
    .getOrCreate()
# Define the Input Source (Socket)
streamingInputDF = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()
# Define the Output Sink (Console) with " append " output mode
query = streamingInputDF \
    .writeStream \
    .outputMode("complete") \
    .format("update") \
    .start()
# Start and Await Termination
query.awaitTermination()
