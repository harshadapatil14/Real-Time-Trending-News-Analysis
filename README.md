
# Real-Time trending News Analysis


A project that collects, processes, and analyzes **real-time trending news** to uncover insights, detect trends, and visualize results in an interactive way.  
The system supports both **streaming** and **batch processing**, making it suitable for live dashboards as well as historical trend analysis.

---

## Features
-  **Real-time news ingestion** from APIs and feeds  
-  **Trending topic detection** based on frequency and relevance  
-  **Sentiment analysis** of news articles  
-  **Batch and streaming pipelines** for analysis  
-  **Interactive dashboards** for visualization  

---

## Tech Stack
- **Programming Language**: Python  
- **Data Processing**: Apache Spark / Pandas  
- **Streaming**: Apache Kafka  
- **APIs**: News API (or similar)  
- **Visualization**: Streamlit / Matplotlib / Plotly  
- **Database**: (Optional: MongoDB / PostgreSQL)  

---

## Project Structure
.
├── producer/ # Fetches news and pushes to Kafka
├── consumer_streaming/ # Real-time stream processing
├── batch_pipeline/ # Batch processing scripts
├── streamlit_live_app/ # Live dashboard for real-time news
├── streamlit_historical_app/ # Dashboard for historical analysis
├── requirements.txt # Python dependencies
└── README.md # Project documentation

yaml
Copy
Edit

---

## Installation & Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/harshadapatil14/Real-Time-Trending-News-Analysis.git
   cd Real-Time-Trending-News-Analysis
Create a virtual environment (recommended)

bash
Copy
Edit
python -m venv venv
source venv/bin/activate   # On Linux/Mac
venv\Scripts\activate      # On Windows
Install dependencies

bash
Copy
Edit
pip install -r requirements.txt
Set up API keys

Get an API key from NewsAPI (or any other source)

Store it in an .env file or config file

Usage
Start the Kafka producer
bash
Copy
Edit
python producer/producer.py
Run the streaming consumer
bash
Copy
Edit
python consumer_streaming/consumer.py
Start the dashboard (Streamlit)
bash
Copy
Edit
streamlit run streamlit_live_app/app.py
Example Output
Real-time trending headlines

Word clouds of frequently used terms

Sentiment distribution (positive, negative, neutral)

Interactive dashboard with filters

Future Improvements
Add support for multiple languages

Deploy as a cloud-based microservice

Enhance NLP with transformer-based models (BERT, GPT, etc.)

Store processed data in a scalable database

Contributing
Contributions are welcome!

Fork the repo

Create a new branch

Submit a pull request

License
This project is licensed under the MIT License.

Author
Developed by Harshada Patil
Developed by Pallavi Dudhalkar

