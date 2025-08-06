from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max, to_date

# Create Spark session
spark = SparkSession.builder \
    .appName("TopTrendingNewsWithSource") \
    .enableHiveSupport() \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .getOrCreate()

# Read news data from HDFS
df = spark.read.json("hdfs://localhost:9000/user/BigDataProject/news_data.json")

# Extract date from publishedAt
df = df.withColumn("news_date", to_date(col("publishedAt")))

# Get latest date
latest_date = df.select(spark_max("news_date")).collect()[0][0]
print(f"Processing latest date: {latest_date}")

# Filter only latest date
latest_df = df.filter(col("news_date") == latest_date)

# Get top trending by title, url, and source.name
top_news_df = latest_df.groupBy(
    col("title"), 
    col("url"), 
    col("source.name").alias("source_name")
).agg(count("*").alias("count")) \
.orderBy(col("count").desc()) \
.limit(20)

# Save to Hive table (overwrite daily)
top_news_df.write.mode("overwrite").saveAsTable("top_trending_news_with_source")

spark.stop()
