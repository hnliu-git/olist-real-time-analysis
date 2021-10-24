#!/usr/bin/env python
# coding: utf-8

# Import packages


import re
import pandas as pd

import nltk
nltk.download('stopwords')
nltk.download('rslp')

from nltk.corpus import stopwords
from nltk.stem import RSLPStemmer

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import split

from sklearn.model_selection import train_test_split

# Load data

df_reviews = pd.read_csv('data/olist_order_reviews_dataset.csv',
                usecols=['order_id', 'review_id', 'review_score', 'review_comment_message', 'review_creation_date']).dropna()

df_reviews['label'] = df_reviews['review_score'].map(lambda x: 0 if x<=3 else 1)
df_train, df_test = train_test_split(df_reviews, test_size=0.2)



spark = SparkSession.builder \
                    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0") \
                    .appName("pyspark_structured_streaming_kafka") \
                    .getOrCreate()

spark_df_train = spark.createDataFrame(df_train)
spark_df_test = spark.createDataFrame(df_test)


# Data Preprocess


@udf
def text_preprocess(text):
    
    site = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    date = '([0-2][0-9]|(3)[0-1])(\/|\.)(((0)[0-9])|((1)[0-2]))(\/|\.)\d{2,4}'
    money = '[R]{0,1}\$[ ]{0,}\d+(,|\.)\d+'

    text = re.sub('[\n\r]', '', text)
    text = re.sub(site, 'link', text)
    text = re.sub(date, 'data', text)
    text = re.sub(money, 'dinheiro', text)
    text = re.sub('[0-9]+', 'numero', text)
    text = re.sub('([nN][ãÃaA][oO]|[ñÑ]| [nN] )', 'negação', text)
    text = re.sub('\W', ' ', text)
    text = re.sub('\s+', ' ', text)
    text = re.sub('[ \t]+$', ' ', text)
    
    return text.lower().strip()

trainset = spark_df_train.select(split(text_preprocess("review_comment_message"), ' ').alias("words"), "label")
testset = spark_df_test.select(split(text_preprocess("review_comment_message"), ' ').alias("words"), "label")


# Sentiment model Training

from pyspark.ml.feature import StopWordsRemover, CountVectorizer, IDF, Tokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

# Remove stopwords
pt_stopwords = [w.lower() for w in stopwords.words('portuguese')]
remover = StopWordsRemover(stopWords=pt_stopwords, inputCol='words', outputCol='rm')
remover.transform(trainset)
remover.transform(testset)
# Countvectorizer
cv = CountVectorizer(vocabSize=2**16, inputCol="rm", outputCol='cv')
# IDF
idf = IDF(inputCol='cv', outputCol='features', minDocFreq=3)
# LR model
lr = LogisticRegression(maxIter=100)
# Pipeline
pipeline = Pipeline(stages=[remover, cv, idf, lr])

# Train the model
model = pipeline.fit(trainset)
predictions = model.transform(testset)
acc = predictions.filter(predictions.label == predictions.prediction).count() / float(testset.count())

print("Acc of the model: %.3f"%acc)


## Streaming Processing
# - Read from Kafka
# - Apply the model
# - Save the results

# Read from Kafka
df = spark.readStream \
          .format("kafka") \
          .option("kafka.bootstrap.servers", "localhost:9092") \
          .option("subscribe", "review") \
          .load()


kv_pairs = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Get the columns
split_v = split(kv_pairs.value, "\[SEP\]")

# Name them
df = kv_pairs.withColumn("product_id", split_v.getItem(0)) \
             .withColumn("text", split_v.getItem(1)) \
             .drop("value") \
             .drop("key") \
             .toDF("product_id", "text")

# Apply the ML pipeline
textset = df.select(split(text_preprocess("text"), ' ').alias("words"), "product_id", "text").dropna()
predictions = model.transform(textset)

# label index to str
label2str = udf(lambda x: ['pos', 'neg'][int(x)])
pred_df = predictions.withColumnRenamed("product_id", "key") \
                     .select("key",label2str("prediction").alias('value'))

# Write to kafka in order to do Stateful operations
query = pred_df.writeStream \
               .format("kafka") \
               .option("kafka.bootstrap.servers", "localhost:9092") \
               .option("topic", "sentiment") \
               .option("checkpointLocation", "./ckpt") \
               .start()

query.awaitTermination()

