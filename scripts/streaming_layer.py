from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Membuat Spark Session
spark = SparkSession.builder \
    .appName("StreamingPipeline") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schema data streaming
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# Membaca data streaming dari folder
stream_df = spark.readStream \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .json("stream_data")

# Menyimpan hasil streaming ke Parquet
query = stream_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "data/serving/stream") \
    .option("checkpointLocation", "logs/stream_checkpoint") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()