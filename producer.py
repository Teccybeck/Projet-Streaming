import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

def read_config():
    config = {}
    with open("./working.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                config[parameter] = value.strip()
    return config

def generate_sensor_data():
    sensor_ids = ['sensor_1', 'sensor_2', 'sensor_3', 'sensor_4']
    datas = []
    for sensor in sensor_ids:
        data = {
        "sensor_id": sensor,
        "timestamp": datetime.now().isoformat(),
        "temperature": round(random.uniform(10.0, 50.0), 2),
        "humidity": round(random.uniform(30.0, 70.0), 2)
        }
        datas.append(data)
    return datas

if __name__ == "__main__":     
    config = read_config()
    bootstrap_servers = config.get("bootstrap.servers")
    sasl_username = config.get("sasl.username")
    sasl_password = config.get("sasl.password")
    topic = "IOT_sensor"
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_plain_username=sasl_username,
            sasl_plain_password=sasl_password,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        while True:
            iot_data = generate_sensor_data()
            for data in iot_data:
                producer.send(topic, data)
            print("Données envoyées")
            time.sleep(5) 

    except KeyboardInterrupt:
        print("\nArrêt du producer.")
        producer.close()

def AdapterParam(data, val):
    # A modif
    if val == "up":
        return True
    elif val == "down":
        return True