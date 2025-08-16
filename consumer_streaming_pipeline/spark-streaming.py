from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_date
from pyspark.sql.types import StructType, StringType

# Define schema for incoming JSON
schema = StructType() \
    .add("title", StringType()) \
    .add("url", StringType()) \
    .add("source", StructType().add("name", StringType()))

# SparkSession with Hive + Kafka support
spark = SparkSession.builder \
    .appName("Top Trending News Loader - Streaming") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .enableHiveSupport() \
    .getOrCreate()

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "news") \
    .load()

# Parse JSON from Kafka messages
df_cleaned = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(
        col("data.title"),
        col("data.url"),
        col("data.source.name").alias("source_name")
    ) \
    .withColumn("date", current_date())  # ⬅️ Add system date here

# Write to Hive-compatible Parquet table with partitioning
query = df_cleaned.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://localhost:9000/user/warehouse/bdproject.db/top_trending_news_streaming/") \
    .option("checkpointLocation", "hdfs://localhost:9000/user/checkpoints/news_streaming/") \
    .partitionBy("date") \
    .start()

query.awaitTermination()
