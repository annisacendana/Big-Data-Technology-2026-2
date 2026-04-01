from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os

# =============================
# INIT SPARK
# =============================
spark = SparkSession.builder \
    .appName("TransportationStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =============================
# BUAT FOLDER (AUTO)
# =============================
os.makedirs("data/checkpoint/transportation", exist_ok=True)
os.makedirs("data/serving/transportation", exist_ok=True)

# =============================
# SCHEMA DATA
# =============================
schema = StructType([
    StructField("trip_id", StringType()),
    StructField("vehicle_type", StringType()),
    StructField("location", StringType()),
    StructField("distance", DoubleType()),
    StructField("fare", DoubleType()),
    StructField("timestamp", StringType())
])

# =============================
# READ STREAM
# =============================
df = spark.readStream \
    .schema(schema) \
    .json("stream_data/transportation")

# =============================
# TRANSFORMASI
# =============================
df = df.withColumn("timestamp", to_timestamp("timestamp"))

# =============================
# WRITE STREAM
# =============================
query = df.writeStream \
    .format("parquet") \
    .option("path", "data/serving/transportation") \
    .option("checkpointLocation", "data/checkpoint/transportation") \
    .outputMode("append") \
    .start()

# =============================
# RUN STREAMING
# =============================
print("🚀 Streaming berjalan... Tekan CTRL+C untuk stop")
query.awaitTermination()