import os
import time
import json
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import types as T
from kafka import KafkaProducer

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

    spark = SparkSession.builder.appName("IoT_Dashboard_And_Archiver").getOrCreate()
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

    schema = T.StructType([
        T.StructField("sensor_id", T.StringType(), True),
        T.StructField("timestamp", T.StringType(), True),
        T.StructField("temperature", T.DoubleType(), True),
        T.StructField("humidity", T.DoubleType(), True),
        T.StructField("waterQuality", T.DoubleType(), True),
        T.StructField("insectPresence", T.DoubleType(), True),
        T.StructField("motion", T.DoubleType(), True),
    ])
    
    parsed_df = kafka_df.select(F.from_json(F.col("value").cast("string"), schema).alias("data")).select("data.*")

    TEMP_THRESHOLD = 40.0
    TEMP_THRESHOLD_LOW = 15.0
    HUM_THRESHOLD = 35.0
    HUM_THRESHOLD_LOW = 35.0
    HUM_THRESHOLD_HIGH = 75.0
    WAT_THRESHOLD_LOW = 0.6
    VALID_SENSORS = ['sensor_1', 'sensor_2', 'sensor_3', 'sensor_4', 'motion_sensor']

    query_memory = (
        parsed_df.writeStream
        .format("memory")
        .queryName("iot_raw_data")
        .outputMode("append")
        .start()
    )

    enriched_df = (
        parsed_df
        .withColumn("alert_type", F.when(F.col("temperature") > TEMP_THRESHOLD, F.lit("High_temperature"))
             .when(F.col("temperature") < TEMP_THRESHOLD_LOW, F.lit("Low_temperature"))
             .when(F.col("humidity") < HUM_THRESHOLD_LOW, F.lit("Low_humidity"))
             .when(F.col("humidity") > HUM_THRESHOLD_HIGH, F.lit("High_humidity"))
             .when(F.col("waterQuality") < WAT_THRESHOLD_LOW, F.lit("Low_Water_Quality"))
             .when(F.col("insectPresence") == 1, F.lit("Insect_Detected"))
             .when(F.col("motion") == 1, F.lit("Motion_Detected"))
             .otherwise(None)
          )
        .withColumn("alert_value", 
            F.when(F.col("temperature") > TEMP_THRESHOLD, F.col("temperature").cast("string"))
             .when(F.col("temperature") < TEMP_THRESHOLD_LOW, F.col("temperature").cast("string"))
             .when(F.col("humidity") < HUM_THRESHOLD_LOW, F.col("humidity").cast("string"))
             .when(F.col("humidity") > HUM_THRESHOLD_HIGH, F.col("humidity").cast("string"))
             .when(F.col("waterQuality") < WAT_THRESHOLD_LOW, F.col("waterQuality").cast("string"))
             .when(F.col("insectPresence") == 1, F.col("insectPresence").cast("string"))
             .when(F.col("motion") == 1, F.col("motion").cast("string"))
             .otherwise(None)
        )
        .select("sensor_id", "timestamp", "temperature", "humidity", "waterQuality", "insectPresence", "motion", "alert_type", "alert_value")
    )

    def process_archiving_batch(df, epoch_id):
        df.persist()
        count = df.count()
        if count > 0:
            print(f"📦 [Archiver] Envoi de {count} données vers '{OUTPUT_TOPIC}'...")
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
                
                for row in rows:
                    data_to_send = row.asDict()
                    producer.send(OUTPUT_TOPIC, data_to_send)
                
                producer.flush()
                producer.close()
                print("✅ [Archiver] Succès.")
            except Exception as e:
                print(f"❌ [Archiver] Erreur : {e}")
        df.unpersist()

    query_archive = (
        enriched_df.writeStream
        .outputMode("append")
        .foreachBatch(process_archiving_batch)
        .start()
    )

    plt.ion()
    fig, axs = plt.subplots(2, 3, figsize=(16, 10)) 
    fig.suptitle('IoT Dashboard - Monitoring Temps Réel & Incidents', fontsize=16)
    ax_flat = axs.flatten()

    print("📈 Dashboard & Archivage lancés. En attente de données...")

    try:
        while True:
            raw_pdf = spark.sql("SELECT * FROM iot_raw_data").toPandas()

            if not raw_pdf.empty:
                raw_pdf = raw_pdf[raw_pdf['sensor_id'].isin(VALID_SENSORS)]
                raw_pdf['timestamp'] = pd.to_datetime(raw_pdf['timestamp'])
                
                current_state = raw_pdf.sort_values('timestamp').groupby('sensor_id').tail(1)
                current_state = current_state.sort_values('sensor_id')

                sensors_only_pdf = current_state[current_state['sensor_id'] != 'motion_sensor']
                motion_only_pdf = current_state[current_state['sensor_id'] == 'motion_sensor']

                now = datetime.now()
                start_window = now - timedelta(hours=12)
                recent_data = raw_pdf[raw_pdf['timestamp'] >= start_window].copy()
                
                recent_alerts = pd.DataFrame()
                if not recent_data.empty:
                    def get_alert_list(row):
                        alerts = []
                        if row['temperature'] > TEMP_THRESHOLD: alerts.append("Temp. Haute")
                        if row['temperature'] < TEMP_THRESHOLD_LOW: alerts.append("Temp. Basse")
                        if row['humidity'] < HUM_THRESHOLD_LOW: alerts.append("Humidité Basse")
                        if row['humidity'] > HUM_THRESHOLD_HIGH: alerts.append("Humidité Haute")
                        if row['waterQuality'] < WAT_THRESHOLD_LOW: alerts.append("Eau Mauvaise")
                        if row['insectPresence'] == 1: alerts.append("Insectes")
                        if row['motion'] == 1: alerts.append("Mouvement")
                        return alerts if len(alerts) > 0 else None

                    recent_data['alert_list'] = recent_data.apply(get_alert_list, axis=1)
                    recent_alerts_raw = recent_data[recent_data['alert_list'].notnull()].copy()
                    recent_alerts = recent_alerts_raw.explode('alert_list')
                    recent_alerts['alert_type'] = recent_alerts['alert_list']

                # 1. Température
                ax1 = ax_flat[0]
                ax1.clear()
                if not sensors_only_pdf.empty:
                    sensors = sensors_only_pdf['sensor_id']
                    temps = sensors_only_pdf['temperature']
                    colors_t = ['#ff4d4d' if (t > TEMP_THRESHOLD or t < TEMP_THRESHOLD_LOW) else '#3498db' for t in temps]
                    bars1 = ax1.bar(sensors, temps, color=colors_t)
                    ax1.set_title("Température (°C)")
                    ax1.set_ylim(0, 50)
                    ax1.axhline(TEMP_THRESHOLD, color='r', linestyle='--', alpha=0.5)
                    for bar in bars1:
                        ax1.annotate(f'{bar.get_height()}°', (bar.get_x() + bar.get_width()/2, bar.get_height()), ha='center', va='bottom')

                # 2. Humidité
                ax2 = ax_flat[1]
                ax2.clear()
                if not sensors_only_pdf.empty:
                    hums = sensors_only_pdf['humidity']
                    colors_h = ['#e67e22' if (h < HUM_THRESHOLD_LOW or h > HUM_THRESHOLD_HIGH) else '#2ecc71' for h in hums]
                    bars2 = ax2.bar(sensors_only_pdf['sensor_id'], hums, color=colors_h)
                    ax2.set_title("Humidité (%)")
                    ax2.set_ylim(0, 100)
                    ax2.axhline(HUM_THRESHOLD_HIGH, color='orange', linestyle='--', alpha=0.5)
                    ax2.axhline(HUM_THRESHOLD_LOW, color='orange', linestyle='--', alpha=0.5)
                    for bar in bars2:
                        ax2.annotate(f'{bar.get_height()}%', (bar.get_x() + bar.get_width()/2, bar.get_height()), ha='center', va='bottom')

                # 3. Eau
                ax3 = ax_flat[2]
                ax3.clear()
                if not sensors_only_pdf.empty:
                    water = sensors_only_pdf['waterQuality']
                    colors_w = ['#e74c3c' if w < WAT_THRESHOLD_LOW else '#3498db' for w in water]
                    ax3.bar(sensors_only_pdf['sensor_id'], water, color=colors_w)
                    ax3.set_title("Qualité Eau (0-1)")
                    ax3.set_ylim(0, 1.2)
                    ax3.axhline(WAT_THRESHOLD_LOW, color='r', linestyle='--', alpha=0.5)

                # 4. Mouvement
                ax4 = ax_flat[3]
                ax4.clear()
                if not motion_only_pdf.empty:
                    motion_vis = [0.05 if m == 0 else 1.0 for m in motion_only_pdf['motion']]
                    colors_m = ['#c0392b' if m == 1 else '#bdc3c7' for m in motion_only_pdf['motion']] 
                    ax4.bar(motion_only_pdf['sensor_id'], motion_vis, color=colors_m)
                    ax4.set_title("Mouvement Détecté")
                    ax4.set_ylim(0, 1.2) 
                    ax4.set_yticks([0, 1])
                    ax4.set_yticklabels(['Non', 'Oui'])
                else:
                    ax4.set_title("Mouvement Détecté")
                    ax4.set_yticks([])
                    ax4.set_xticks([])

                # 5. Insectes
                ax5 = ax_flat[4]
                ax5.clear()
                if not sensors_only_pdf.empty:
                    insect_vis = [0.05 if i == 0 else 1.0 for i in sensors_only_pdf['insectPresence']]
                    colors_i = ['#8e44ad' if i == 1 else '#bdc3c7' for i in sensors_only_pdf['insectPresence']]
                    ax5.bar(sensors_only_pdf['sensor_id'], insect_vis, color=colors_i)
                    ax5.set_title("Présence Insectes")
                    ax5.set_ylim(0, 1.2)
                    ax5.set_yticks([0, 1])
                    ax5.set_yticklabels(['Non', 'Oui'])
                    ax5.tick_params(axis='x', rotation=15)

                # 6. Statistiques Alertes
                ax6 = ax_flat[5]
                ax6.clear()
                
                if not recent_alerts.empty:
                    recent_alerts = recent_alerts.sort_values(by=['sensor_id', 'alert_type', 'timestamp'])
                    recent_alerts['prev_ts'] = recent_alerts.groupby(['sensor_id', 'alert_type'])['timestamp'].shift(1)
                    recent_alerts['time_diff'] = (recent_alerts['timestamp'] - recent_alerts['prev_ts']).dt.total_seconds()
                    
                    GAP_THRESHOLD = 60 
                    unique_incidents = recent_alerts[ (recent_alerts['time_diff'].isnull()) | (recent_alerts['time_diff'] > GAP_THRESHOLD) ]
                    
                    if not unique_incidents.empty:
                        counts = unique_incidents['alert_type'].value_counts()
                        alert_types = counts.index
                        alert_values = counts.values
                        
                        bars6 = ax6.bar(alert_types, alert_values, color='#e74c3c')
                        ax6.set_title(f"Incidents Uniques (12h) - Total: {len(unique_incidents)}")
                        ax6.set_xticklabels(alert_types, rotation=30, ha='right', fontsize=9)
                        
                        for bar in bars6:
                            height = bar.get_height()
                            ax6.annotate(f'{height}',
                                         xy=(bar.get_x() + bar.get_width() / 2, height),
                                         xytext=(0, 3), 
                                         textcoords="offset points",
                                         ha='center', va='bottom')
                    else:
                         ax6.text(0.5, 0.5, "Pas de nouveaux incidents", ha='center', va='center')
                         ax6.set_title("Incidents (12h)")
                else:
                     ax6.text(0.5, 0.5, "Aucun incident", ha='center', va='center')
                     ax6.set_title("Incidents (12h)")

            plt.tight_layout()
            plt.subplots_adjust(top=0.90, bottom=0.15, hspace=0.4)
            plt.draw()
            plt.pause(2)

    except KeyboardInterrupt:
        print("Arrêt du dashboard.")
    finally:
        query_memory.stop()
        query_archive.stop()
        print("Spark Streams arrêtés.")