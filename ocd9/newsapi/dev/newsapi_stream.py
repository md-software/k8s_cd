#!/usr/bin/env python3
#
# This script is used for POC pipeline
#
# Reads messages from Kafka topic and store them without transformation into postgresql table
#

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Créer la session Spark
spark = SparkSession.builder \
    .appName("newsapi_stream") \
    .getOrCreate()

# Set Kafka log level to WARN
spark.sparkContext.setLogLevel("WARN")
spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger("org.apache.kafka").setLevel(spark.sparkContext._jvm.org.apache.log4j.Level.WARN)

# Configurer les propriétés Kafka
kafka_bootstrap_servers = "kafka-controller-0.kafka-controller-headless.kafka.svc.cluster.local:9092,kafka-controller-1.kafka-controller-headless.kafka.svc.cluster.local:9092,kafka-controller-2.kafka-controller-headless.kafka.svc.cluster.local:9092"
kafka_topic = "newsapi"

# Lire les données à partir de Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Sélectionner uniquement la colonne "value" du DataFrame
df = df.selectExpr("CAST(value AS STRING)")

# Define an output directory to write the messages to
output_directory = "/tmp"

# Define a function to process each batch of messages
def process_batch(batch_df, batch_id):
    # for col in batch_df.dtypes:
    #     print(col[0]+" , "+col[1])

    # Convert the batch of messages to a list of strings
    messages = batch_df.select("value").rdd.map(lambda r: r.value).collect()

    # Process the messages as needed
    # For example, you can write them to a file
    with open(output_directory+f"/batch_{batch_id}.txt", "w") as file:
        for message in messages:
            #print(batch_id, message)
            file.write(message.replace("None", "null") + "\n")

# def process_batch(batch_df, batch_id):
#     # Perform any necessary transformations or aggregations on batch_df
#     processed_df = batch_df.cache()

#     # Write the processed DataFrame to the PostgreSQL table
#     processed_df.write \
#         .json(output_directory+f"/batch.txt")

# Write the messages using foreachBatch
df.writeStream \
    .foreachBatch(lambda batch_df, batch_id: process_batch(batch_df, batch_id)) \
    .start() \
    .awaitTermination()
