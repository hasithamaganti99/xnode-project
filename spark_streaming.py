from pyspark.sql import SparkSession
from pyspark.sql.functions import *

print("Starting Spark Session")
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

print("Spark Session started")

# Read from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "Bank_Finance_Data") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the Kafka message value (it's in bytes, so we cast)
parsed_df = df.selectExpr("CAST(value AS STRING) as message")

# Optional: parse JSON
json_df = parsed_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", "transaction_id STRING, security_type STRING, region STRING, timestamp STRING").alias("data")) \
    .select("data.*")

# Show the output in console
query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

print("Stream started, waiting for termination...")
query.awaitTermination()
