from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max

spark = SparkSession.builder.appName("LatestTrendingNews").getOrCreate()

# Read all partitions
df = spark.read.option("basePath", "hdfs://localhost:9000/user/BigDataProject/top_trending_news")
# # Get latest date
# latest_date = df.select(spark_max("news_date")).collect()[0][0]

# # Filter only latest date
# latest_df = df.filter(col("news_date") == latest_date)

# # Trending analysis
# trending_df = latest_df.groupBy("title", "source") \
#     .agg(count("*").alias("count")) \
#     .orderBy(col("count").desc())

# trending_df.show(10, truncate=False)

df.show(5, truncate=False)
spark.stop()
