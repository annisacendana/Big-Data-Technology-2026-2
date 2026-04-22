from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder \
    .appName("FraudDetection") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schema Kafka
schema = StructType() \
    .add("transaction_id", IntegerType()) \
    .add("amount", IntegerType()) \
    .add("type", StringType()) \
    .add("status", StringType())

# Read Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bank_topic") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# DEBUG (lihat data masuk)
console_query = parsed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

# Simpan ke parquet
file_query = parsed_df.writeStream \
    .format("parquet") \
    .option("path", "stream_data/realtime_output") \
    .option("checkpointLocation", "stream_data/checkpoint") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()