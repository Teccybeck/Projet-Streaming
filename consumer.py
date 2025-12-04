import json
import time
import random
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

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

    config = read_config()
    bootstrap_servers = config.get("bootstrap.servers")
    sasl_username = config.get("sasl.username")
    sasl_password = config.get("sasl.password")
    jaas_config = f'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{sasl_username}\" password=\"{sasl_password}\";'
    topic = 'IOT_sensor'

    spark = (
        SparkSession.builder
        .appName("IoT_Sensor_Alerts")
        .getOrCreate()
    )

    spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas_config)
        .load()
    )

    value_df = kafka_df.select(F.col("value").cast("string").alias("json_str"))

    schema = T.StructType([
        T.StructField("sensor_id", T.StringType(), True),
        T.StructField("timestamp", T.StringType(), True),
        T.StructField("temperature", T.DoubleType(), True),
        T.StructField("humidity", T.DoubleType(), True),
    ])

    parsed_df = (
        value_df
        .select(F.from_json("json_str", schema).alias("data"))
        .select("data.*")
    )

    TEMP_THRESHOLD = 40.0
    TEMP_THRESHOLD_LOW = 15.0
    HUM_THRESHOLD = 35.0
    HUM_THRESHOLD_HIGH = 75.0

    alerts_df = (
    parsed_df
    .withColumn("alert_type", F.when(F.col("temperature") > TEMP_THRESHOLD, F.lit("High_temperature"))
         .when(F.col("temperature") < TEMP_THRESHOLD_LOW, F.lit("Low_temperature"))
         .when(F.col("humidity") < HUM_THRESHOLD, F.lit("Low_humidity"))
         .when(F.col("humidity") > HUM_THRESHOLD_HIGH, F.lit("High_humidity"))
      )
      .where(F.col("alert_type").isNotNull())
    )

    query = (
        alerts_df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 50)
        .start()
    )

    query.awaitTermination()
