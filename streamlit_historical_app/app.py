from pyspark.sql import SparkSession
import streamlit as st
import pandas as pd

# ---------- Streamlit UI ----------
st.set_page_config(page_title="Trending News Dashboard", layout="wide")
st.title("📈 Top Trending News - Date-wise Dashboard")

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

# ---------- Fetch Distinct Dates ----------
try:
    dates_df = spark.sql("SELECT DISTINCT date FROM bdproject.top_trending_news_with_date ORDER BY date DESC")
    dates_list = [row["date"] for row in dates_df.collect()]
except Exception as e:
    st.error(f"Error fetching dates: {e}")
    st.stop()

# ---------- Sidebar UI ----------
st.sidebar.header("Filters")

selected_date = st.sidebar.selectbox("📅 Select Date", dates_list)
keyword = st.sidebar.text_input("🔍 Search keyword in title")


# ---------- Fetch & Show News ----------
if selected_date:
    with st.spinner("Fetching news..."):
        try:
            if keyword:
                # Search keyword in title (case-insensitive)
                search_query = f"""
                    SELECT 
                        Distinct
                        title, 
                        source_name, 
                        url
                    FROM bdproject.top_trending_news_with_date
                    WHERE date = '{selected_date}'
                      AND LOWER(title) LIKE '%{keyword.lower()}%'
                    LIMIT 10
                """
                news_df = spark.sql(search_query)
                pandas_df = news_df.toPandas()

                if not pandas_df.empty:
                    pandas_df["url"] = pandas_df["url"].apply(lambda x: f"[🔗 Link]({x})")
                    st.success(f"News with '{keyword}' in title on {selected_date}")
                    st.write(pandas_df.to_markdown(index=False), unsafe_allow_html=True)
                else:
                    st.warning("No news found with that keyword in the title.")

            else:
                # Show top 50 trending news (by occurrences)
                base_query = f"""
                    SELECT 
                        title, 
                        source_name, 
                        url, 
                        COUNT(*) AS occurrences
                    FROM bdproject.top_trending_news_with_date
                    WHERE date = '{selected_date}'
                    GROUP BY title, source_name, url
                    ORDER BY occurrences DESC
                    LIMIT 20
                """
                news_df = spark.sql(base_query)
                pandas_df = news_df.toPandas()
                pandas_df["url"] = pandas_df["url"].apply(lambda x: f"[🔗 Link]({x})")
                st.success(f"Top trending news on {selected_date}")
                st.write(pandas_df.to_markdown(index=False), unsafe_allow_html=True)

        except Exception as e:
            st.error(f"Error running query: {e}")
