### Olist real-time analysis system

Read e-commerce data in stream and produce real-time top-rated and top-sold data.


### Tools 
- Kafka
- Spark Sturctured Streaming
- Python

### Description
- producer.ipynb: read from data folder and produce streaming data to Kafka
- consumer_review.py: train a sentiment model and use that model to inference streaming data from Kafka then feed the result back to Kafka
- consumer_sent: produce top-rated list
- consumer_order: produce top-sold list