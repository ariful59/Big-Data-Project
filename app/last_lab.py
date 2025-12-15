from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("StreamingSQLCompleteWordCount") \
    .getOrCreate()
# Connect to the socket source
# Expecting lines of text from the input
streamingInputDF = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()
# Tokenize the input lines into words
words = streamingInputDF.select(explode(split(streamingInputDF.value, " ")).alias("word"))
# Register the DataFrame of words as a temporary view
words.createOrReplaceTempView("words_table")
# Use SQL to count the total number of words received
wordCount = spark.sql("SELECT 'Total Words' as Description, COUNT(word) as Count FROM words_table")
# Output the results to console using the "complete" mode
query = wordCount.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
# Keep the application running until terminated
query.awaitTermination()
