import requests
import json
from kafka import KafkaProducer
import time

# Replace with your NewsAPI key
NEWSAPI_KEY = "8a0879605a5149139750442b744ed98b"

KAFKA_TOPIC = "news"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_trending_news():
    url = f"https://newsapi.org/v2/top-headlines?language=en&pageSize=10&apiKey={NEWSAPI_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json().get("articles", [])
    else:
        print(f"Failed to fetch news: {response.status_code}")
        return []

def send_to_kafka(article):
    producer.send(KAFKA_TOPIC, value=article)
    producer.flush()
    print(f"Sent article to Kafka: {article['title']}")

def main():
    while True:
        articles = fetch_trending_news()
        for article in articles:
            send_to_kafka(article)
        print("Sleeping for 60 seconds...")
        time.sleep(60)  # fetch every 1 minute

if __name__ == "__main__":
    main()
