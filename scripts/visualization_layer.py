# =====================================
# VISUALIZATION LAYER
# =====================================

from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import os

# Membuat Spark Session
spark = (
    SparkSession.builder
    .appName("VisualizationLayer")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Membaca data parquet
df = spark.read.parquet("data/clean/parquet/")

# Revenue per Category
category_df = (
    df.groupBy("category")
    .sum("total_amount")
    .toPandas()
)

# Urutkan dari revenue terbesar
category_df = category_df.sort_values("sum(total_amount)", ascending=False)

# Pastikan folder reports ada
os.makedirs("reports", exist_ok=True)

# Membuat grafik
plt.figure(figsize=(8,5))
plt.bar(category_df["category"], category_df["sum(total_amount)"])
plt.xticks(rotation=45)
plt.title("Revenue per Category")
plt.ylabel("Total Revenue")
plt.tight_layout()

# Simpan grafik
plt.savefig("reports/category_revenue.png")

print("Visualization saved to reports/category_revenue.png")

spark.stop()