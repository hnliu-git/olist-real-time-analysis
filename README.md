# Olist E-Commerce Data Real-time Analysis
This is a real-time analyzer for an e-commerce system based on Kafka and Spark. 

The analyzer can output a real-time top-10 rated(based on sentiment analysis) and purchased products with streaming data input. The overall structure is shown in the figure below.

![image](overview.png)

## Tools
- Language: Python, Scala
- Streaming Processing: Spark Sturctured Streaming
- Messaging System: Kafka
- Sentimnet Analyzer: SparkML
