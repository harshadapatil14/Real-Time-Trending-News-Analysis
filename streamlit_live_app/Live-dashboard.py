import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession

# ---------- Streamlit UI ----------
st.set_page_config(page_title="Trending News Dashboard", layout="wide")
st.title("📈 Top Trending News - Date-wise Dashboard (Live Streamed Data)")

# ---------- Spark Session (Cached) ----------
@st.cache_resource
def create_spark_session():
    return SparkSession.builder \
        .appName("TrendingNewsDashboard") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("hive.metastore.uris", "thrift://localhost:9083")\
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

spark = create_spark_session()

# ---------- Repair Table to Detect New Partitions ----------
try:
    spark.sql("MSCK REPAIR TABLE bdproject.top_trending_news_streaming")
except Exception as e:
    st.warning("Partition repair failed: " + str(e))

# ---------- Fetch Available Dates ----------
try:
    dates_df = spark.sql("SELECT DISTINCT date FROM bdproject.top_trending_news_streaming ORDER BY date DESC")
    dates = [row["date"] for row in dates_df.collect()]
except Exception as e:
    st.error(f"Error fetching available dates: {e}")
    st.stop()

# ---------- Sidebar Filters ----------
st.sidebar.header("Filters")
selected_date = st.sidebar.selectbox("📅 Select Date", dates)
keyword = st.sidebar.text_input("🔍 Search keyword in title")

# ---------- News Retrieval ----------
if selected_date:
    try:
        if keyword:
            query = f"""
                SELECT DISTINCT title, source_name, url
                FROM bdproject.top_trending_news_streaming
                WHERE date = '{selected_date}'
                AND LOWER(title) LIKE '%{keyword.lower()}%'
                LIMIT 10
            """
            st.subheader(f"🔎 News with '{keyword}' on {selected_date}")
        else:
            query = f"""
                SELECT title, source_name, url, COUNT(*) as occurrences
                FROM bdproject.top_trending_news_streaming
                WHERE date = '{selected_date}'
                GROUP BY title, source_name, url
                ORDER BY occurrences DESC
                LIMIT 20
            """
            st.subheader(f"🔥 Top Trending News on {selected_date}")

        df = spark.sql(query).toPandas()
        if df.empty:
            st.warning("No news found for selected filters.")
        else:
            df["url"] = df["url"].apply(lambda x: f"[🔗 Link]({x})")
            st.write(df.to_markdown(index=False), unsafe_allow_html=True)

    except Exception as e:
        st.error(f"Error querying Hive table: {e}")
