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
    sensor_ids = ['sensor_1', 'sensor_2', 'sensor_3', 'sensor_4', 'motion_sensor']
    datas = []

    for sensor in sensor_ids:
        if sensor == 'motion_sensor':
            if sensor not in last_values:
                initial_time = datetime.now().isoformat()
                initial_motion = random.choices([0, 1], weights=[90, 10], k=1)[0]

                last_values[sensor] = {"timestamp": initial_time, "motion": initial_motion}

            new_time = datetime.now().isoformat()
            new_motion = random.choices([0, 1], weights=[90, 10], k=1)[0]
            
            last_values[sensor]["timestamp"] = new_time
            last_values[sensor]["motion"] = new_motion
            
            data = {
                "sensor_id": sensor,
                "timestamp": datetime.now().isoformat(),
                "motion": last_values[sensor]["motion"]
            }

            datas.append(data)
        else:
            if sensor not in last_values:
                initial_time = datetime.now().isoformat()
                initial_temp = round(random.uniform(10.0, 50.0), 2)
                initial_hum  = round(random.uniform(30.0, 70.0), 2)
                initial_water_quality = round(random.uniform(0, 1.0), 2)
                initial_insect_presence = random.choices([0, 1], weights=[90, 10], k=1)[0]

                last_values[sensor] = {"timestamp" : initial_time ,"temperature" : initial_temp, "humidity" : initial_hum, "waterQuality" : initial_water_quality, "insectPresence" : initial_insect_presence}

            prev_temp = last_values[sensor]["temperature"]
            prev_hum  = last_values[sensor]["humidity"]
            prev_water_quality = last_values[sensor]["waterQuality"]

            new_time = datetime.now().isoformat()
            new_temp = prev_temp + random.uniform(-1.5, 1.5)
            new_hum  = prev_hum + random.uniform(-2.0, 2.0)
            new_water_quality = prev_water_quality + random.uniform(-0.1, 0.1)
            new_insect_presence = random.choices([0, 1], weights=[90, 10], k=1)[0]

            new_temp = max(10.0, min(50.0, new_temp))
            new_hum = max(30.0, min(70.0, new_hum))
            new_water_quality = max(0.0, min(1.0, new_water_quality))

            last_values[sensor]["timestamp"] = new_time
            last_values[sensor]["temperature"] = round(new_temp, 2)
            last_values[sensor]["humidity"] = round(new_hum, 2)
            last_values[sensor]["waterQuality"] = round(new_water_quality, 2)
            last_values[sensor]["insectPresence"] = new_insect_presence

            data = {
                "sensor_id": sensor,
                "timestamp": datetime.now().isoformat(),
                "temperature": last_values[sensor]["temperature"],
                "humidity": last_values[sensor]["humidity"],
                "waterQuality": last_values[sensor]["waterQuality"],
                "insectPresence": last_values[sensor]["insectPresence"],
            }

            datas.append(data)

    return datas

if __name__ == "__main__":     
    last_values = {}
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

def AdapterParam(alerte = "", sensor = ""):
    # A modifier avec de vrai actions pour de vrai détecteurs.
    if alerte == "High_temperature":
        print(f"Alerte ! Le détecteur '{sensor}' détecte une température trop haute.")
    elif alerte == "Low_temperature":
        print(f"Alerte ! Le détecteur '{sensor}' détecte une température trop faible.")
    elif alerte == "Low_humidity":
        print(f"Alerte ! Le détecteur '{sensor}' détecte une humidité trop faible.")
    elif alerte == "High_humidity":
        print(f"Alerte ! Le détecteur '{sensor}' détecte une humidité trop haute.")
    elif alerte == "Low_Water_Quality":
        print(f"Alerte ! Le détecteur '{sensor}' détecte une qualité de l'eau trop faible.")
    elif alerte == "Insect_Detected":
        print(f"Alerte ! Le détecteur '{sensor}' détecte la présence d'un insecte.")
    elif alerte == "Motion_Detected":
        print(f"Alerte ! Le détecteur '{sensor}' détecte du mouvement autour de lui.")