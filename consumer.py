import json
import time
import random
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'

def read_config():
    config = {}
    with open("./working.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                config[parameter] = value.strip()
    return config

if __name__ == "__main__":
    
    spark = SparkSession\
            .builder\
            .appName("TD5")\
            .getOrCreate()

    sensor_schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", FloatType(), True),
        StructField("humidity", FloatType(), True)
    ])

    config = read_config()
    bootstrap_servers = config.get("bootstrap.servers")
    sasl_username = config.get("sasl.username")
    sasl_password = config.get("sasl.password")
    jaas_config = f'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{sasl_username}\" password=\"{sasl_password}\";'
    
    spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

    kafka_options = {
        "kafka.bootstrap.servers": bootstrap_servers,
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.jaas.config": jaas_config,
        "startingOffsets": "earliest",
        "auto.offset.reset": "earliest"
    }

    sensors_loaded_stream = spark.readStream\
        .format("kafka")\
        .option("subscribe", "IOT_sensor")\
        .options(**kafka_options)\
        .load()
    
    sensors_stream = sensors_loaded_stream.select(
        F.col("value").cast(StringType()).alias("json_value")
    ).select(
        F.from_json(F.col("json_value"), sensor_schema).alias("data")
    ).select(F.col("data.*"))

    sensors_state = sensors_stream.groupBy("sensor_id").agg(
        F.max("timestamp").alias("timestamp"),
        F.last("temperature").alias("temperature"),
        F.last("humidity").alias("humidity")
    )

    sensors_state.createOrReplaceTempView("sensor_table")

    querry = """
    SELECT * FROM sensor_table
    """

    query_stream = spark.sql(querry).writeStream\
        .format("console")\
        .outputMode("complete")\
        .queryName("name")\
        .option("truncate", "false")\
        .trigger(processingTime="5 seconds")\
        .start()

    try:
        query_stream.awaitTermination()
    except KeyboardInterrupt:
        print("Arrêt du streaming demandé.")