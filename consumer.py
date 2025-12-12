import os
import json
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import types as T
from kafka import KafkaProducer
from producer import AdapterParam 

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'

def read_config():
    config = {}
    if not os.path.exists("./working.properties"):
        return {}
    with open("./working.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                config[parameter] = value.strip()
    return config

if __name__ == "__main__":

    config = read_config()
    bootstrap_servers = config.get("bootstrap.servers", "localhost:9092")
    sasl_username = config.get("sasl.username")
    sasl_password = config.get("sasl.password")
    
    sasl_mechanism = "PLAIN"
    security_protocol = "SASL_SSL" if sasl_username and sasl_password else "PLAINTEXT"
    
    jaas_config = None
    if sasl_username and sasl_password:
        jaas_config = f'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{sasl_username}\" password=\"{sasl_password}\";'

    INPUT_TOPIC = 'IOT_sensor'
    OUTPUT_TOPIC = 'IOT_archive'

    spark = SparkSession.builder.appName("IoT_Archiver_TableConsole").getOrCreate()
    spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
    spark.sparkContext.setLogLevel("WARN")

    kafka_options = {
        "kafka.bootstrap.servers": bootstrap_servers,
        "subscribe": INPUT_TOPIC,
        "startingOffsets": "latest"
    }
    if jaas_config:
        kafka_options["kafka.security.protocol"] = security_protocol
        kafka_options["kafka.sasl.mechanism"] = sasl_mechanism
        kafka_options["kafka.sasl.jaas.config"] = jaas_config

    kafka_df = spark.readStream.format("kafka").options(**kafka_options).load()

    value_df = kafka_df.select(F.col("value").cast("string").alias("json_str"))

    schema = T.StructType([
        T.StructField("sensor_id", T.StringType(), True),
        T.StructField("timestamp", T.StringType(), True),
        T.StructField("temperature", T.DoubleType(), True),
        T.StructField("humidity", T.DoubleType(), True),
        T.StructField("waterQuality", T.DoubleType(), True),
        T.StructField("insectPresence", T.DoubleType(), True),
        T.StructField("motion", T.DoubleType(), True),
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
    WAT_THRESHOLD_LOW = 0.6

    enriched_df = (
    parsed_df
    .withColumn("alert_type", F.when(F.col("temperature") > TEMP_THRESHOLD, F.lit("High_temperature"))
         .when(F.col("temperature") < TEMP_THRESHOLD_LOW, F.lit("Low_temperature"))
         .when(F.col("humidity") < HUM_THRESHOLD, F.lit("Low_humidity"))
         .when(F.col("humidity") > HUM_THRESHOLD_HIGH, F.lit("High_humidity"))
         .when(F.col("waterQuality") < WAT_THRESHOLD_LOW, F.lit("Low_Water_Quality"))
         .when(F.col("insectPresence") == 1, F.lit("Insect_Detected"))
         .when(F.col("motion") == 1, F.lit("Motion_Detected"))
         .otherwise(None) 
      )
    .withColumn("alert_value", 
        F.when(F.col("temperature") > TEMP_THRESHOLD, F.col("temperature").cast("string"))
         .when(F.col("temperature") < TEMP_THRESHOLD_LOW, F.col("temperature").cast("string"))
         .when(F.col("humidity") < HUM_THRESHOLD, F.col("humidity").cast("string"))
         .when(F.col("humidity") > HUM_THRESHOLD_HIGH, F.col("humidity").cast("string"))
         .when(F.col("waterQuality") < WAT_THRESHOLD_LOW, F.col("waterQuality").cast("string"))
         .when(F.col("insectPresence") == 1, F.col("insectPresence").cast("string"))
         .when(F.col("motion") == 1, F.col("motion").cast("string"))
         .otherwise(None)
    )
    .select("sensor_id", "timestamp", "temperature", "humidity", "waterQuality", "insectPresence", "motion", "alert_type", "alert_value")
    )

    def process_batch(df, epoch_id):
        df.persist()
        
        count = df.count()
        if count > 0:
            rows = df.collect()
            
            try:
                producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    security_protocol=security_protocol,
                    sasl_mechanism=sasl_mechanism,
                    sasl_plain_username=sasl_username,
                    sasl_plain_password=sasl_password,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                
                alerts_for_table = []

                for row in rows:
                    data_to_send = row.asDict()
                    producer.send(OUTPUT_TOPIC, data_to_send)
                    if row["alert_type"] is not None:
                        alerts_for_table.append({
                            "Timestamp": row["timestamp"].split('T')[1][:8],
                            "Sensor": row["sensor_id"],
                            "Alert Type": row["alert_type"],
                            "Value": row["alert_value"]
                        })
                        AdapterParam(row["alert_type"], row["sensor_id"])
                
                producer.flush()
                producer.close()
                
                if len(alerts_for_table) > 0:
                    print("="*60)
                    df_display = pd.DataFrame(alerts_for_table)
                    print(df_display.to_string(index=False)) 
                    print("="*60 + "\n")
                else:
                    print(f"Aucune alerte détectée.")

            except Exception as e:
                print(f"Erreur : {e}")

        df.unpersist()

    query = (
        enriched_df.writeStream
        .outputMode("append")
        .foreachBatch(process_batch)
        .start()
    )

    query.awaitTermination()