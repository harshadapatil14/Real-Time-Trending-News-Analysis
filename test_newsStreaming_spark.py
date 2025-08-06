from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("NewsFromHDFS") \
    .getOrCreate()

# Path to HDFS file
hdfs_path = "hdfs://localhost:9000/user/BigDataProject/news_data.json"

# Read JSON data from HDFS
df = spark.read.json(hdfs_path)

# Show schema
df.printSchema()

# Show a sample of news titles and dates
df.show(10, truncate=False)

# Stop Spark session
spark.stop()
