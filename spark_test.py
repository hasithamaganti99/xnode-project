import os
os.environ['SPARK_LOG_DIR'] = "/tmp/spark-logs"
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MinimalSparkTest") \
    .master("local[*]") \
    .getOrCreate()

print("Spark Session started!")
df=spark.range(10)
df.show()
spark.stop()
