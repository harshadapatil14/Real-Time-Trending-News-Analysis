from kafka import KafkaConsumer
import json
from hdfs import InsecureClient
from datetime import datetime

TOPIC_NAME = "news"
KAFKA_SERVER = "localhost:9092"
HDFS_URL = "http://localhost:9870"
HDFS_USER = "sunbeam"

def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    client = InsecureClient(HDFS_URL, user=HDFS_USER)

    for message in consumer:
        today = datetime.now().strftime("%Y-%m-%d")
        folder_path = f"/user/BigDataProject/news_data/date={today}/"

        # Ensure today's folder exists
        client.makedirs(folder_path)

        # Create unique file for each message
        file_path = folder_path + f"news_{datetime.now().strftime('%H%M%S%f')}.json"

        # Write the single news item into its own file
        with client.write(file_path, encoding='utf-8') as writer:
            writer.write(json.dumps(message.value) + "\n")

        print(f"📦 Saved to HDFS ({today}): {message.value.get('title', 'No Title')}")

if __name__ == "__main__":
    main()
