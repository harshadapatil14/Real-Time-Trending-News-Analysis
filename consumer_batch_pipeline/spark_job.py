from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create SparkSession with Hive support

spark = SparkSession.builder \
    .appName("Top Trending News Loader") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083")\
    .enableHiveSupport() \
    .getOrCreate()

# Define base path (HDFS)
base_path = "hdfs://localhost:9000/user/BigDataProject/news_data/"

# Read JSON files from all date-partitioned directories
# df_raw = spark.read.option("multiLine", True).json(base_path + "date=*")

df_raw = spark.read \
    .option("multiLine", True) \
    .option("basePath", base_path) \
    .json(base_path + "date=*")

# Extract relevant columns + flatten source.name
df_cleaned = df_raw.select(
    col("title"),
    col("url"),
    col("source.name").alias("source_name"),
    col("date")  # extracted from partition automatically by Spark
)

# Optional: show preview
df_cleaned.select("title", "url", "source_name", "date").show(5, truncate=False)

# Write to Hive table with partitioning
df_cleaned.write.mode("append").partitionBy("date").format("parquet").saveAsTable("bdproject.top_trending_news_with_date")
# spark.sql("SHOW DATABASES").show()

print("✅ Data saved to Hive table: top_trending_news_with_date")

spark.stop()
