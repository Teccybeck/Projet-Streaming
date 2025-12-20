import json
import time
import random
from datetime import datetime, timedelta
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

last_values = {}

TEMP_THRESHOLD = 40.0
COOLING_DURATION_SEC = 10
COOLING_TOTAL_DROP = 10.0 
TEMP_STOP_COOLING = 30.0

HEATING_THRESHOLD = 15.0
HEATING_DURATION_SEC = 10
HEATING_TOTAL_RISE = 15.0
HEATING_TARGET = 30.0

HUM_THRESHOLD_LOW = 35.0
HUM_THRESHOLD_HIGH = 75.0
HUM_TARGET = 55.0

HUM_ADJUST_DURATION_SEC = 10
HUM_TOTAL_ADJUST = 20.0 

WATER_QUALITY_THRESHOLD_LOW = 0.6
WATER_QUALITY_TARGET = 0.8
WATER_PURIFY_DURATION_SEC = 10
WATER_TOTAL_INCREASE = 0.3

def generate_sensor_data():
    sensor_ids = ['sensor_1', 'sensor_2', 'sensor_3', 'sensor_4', 'motion_sensor']
    datas = []
    now = datetime.now()

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
                initial_hum  = round(random.uniform(10.0, 90.0), 2)
                initial_water_quality = round(random.uniform(0, 1.0), 2)
                initial_insect_presence = random.choices([0, 1], weights=[90, 10], k=1)[0]

                last_values[sensor] = {
                    "timestamp": initial_time,
                    "temperature": initial_temp,
                    "humidity": initial_hum,
                    "waterQuality": initial_water_quality,
                    "insectPresence": initial_insect_presence,
                    "cooling": False,
                    "cooling_end_time": None,
                    "heating": False,
                    "heating_end_time": None,
                    "humidifying": False,
                    "humidifying_end_time": None,
                    "dehumidifying": False,
                    "dehumidifying_end_time": None,
                    "purifying": False,
                    "purifying_end_time": None
                }               

            prev_temp = last_values[sensor]["temperature"]
            prev_hum  = last_values[sensor]["humidity"]
            prev_water_quality = last_values[sensor]["waterQuality"]

            new_temp = prev_temp + random.uniform(-1.5, 1.5)
            new_hum  = prev_hum + random.uniform(-2.0, 2.0)
            new_water_quality = prev_water_quality + random.uniform(-0.1, 0.1)

            if prev_temp > TEMP_THRESHOLD and not last_values[sensor]["cooling"]:
                last_values[sensor]["cooling"] = True
                last_values[sensor]["cooling_end_time"] = now + timedelta(seconds=COOLING_DURATION_SEC)
                last_values[sensor]["heating"] = False
                last_values[sensor]["heating_end_time"] = None

            if prev_temp < HEATING_THRESHOLD and not last_values[sensor]["heating"]:
                last_values[sensor]["heating"] = True
                last_values[sensor]["heating_end_time"] = now + timedelta(seconds=HEATING_DURATION_SEC)
                last_values[sensor]["cooling"] = False
                last_values[sensor]["cooling_end_time"] = None

            if prev_hum < HUM_THRESHOLD_LOW and not last_values[sensor]["humidifying"]:
                last_values[sensor]["humidifying"] = True
                last_values[sensor]["humidifying_end_time"] = now + timedelta(seconds=HUM_ADJUST_DURATION_SEC)
                last_values[sensor]["dehumidifying"] = False
                last_values[sensor]["dehumidifying_end_time"] = None

            if prev_hum > HUM_THRESHOLD_HIGH and not last_values[sensor]["dehumidifying"]:
                last_values[sensor]["dehumidifying"] = True
                last_values[sensor]["dehumidifying_end_time"] = now + timedelta(seconds=HUM_ADJUST_DURATION_SEC)
                last_values[sensor]["humidifying"] = False
                last_values[sensor]["humidifying_end_time"] = None

            if prev_water_quality < WATER_QUALITY_THRESHOLD_LOW and not last_values[sensor]["purifying"]:
                last_values[sensor]["purifying"] = True
                last_values[sensor]["purifying_end_time"] = now + timedelta(seconds=WATER_PURIFY_DURATION_SEC)

            if last_values[sensor]["cooling"]:
                if prev_temp <= TEMP_STOP_COOLING:
                    last_values[sensor]["cooling"] = False
                    last_values[sensor]["cooling_end_time"] = None
                elif now < last_values[sensor]["cooling_end_time"]:
                    steps = max(1, int(COOLING_DURATION_SEC / 5))
                    cooling_step = COOLING_TOTAL_DROP / steps
                    new_temp = prev_temp - cooling_step + random.uniform(-0.3, 0.3)
                else:
                    last_values[sensor]["cooling"] = False
                    last_values[sensor]["cooling_end_time"] = None

            elif last_values[sensor]["heating"]:
                if prev_temp >= HEATING_TARGET:
                    last_values[sensor]["heating"] = False
                    last_values[sensor]["heating_end_time"] = None
                elif now < last_values[sensor]["heating_end_time"]:
                    steps = max(1, int(HEATING_DURATION_SEC / 5))
                    heating_step = HEATING_TOTAL_RISE / steps
                    new_temp = prev_temp + heating_step + random.uniform(-0.3, 0.3)
                else:
                    last_values[sensor]["heating"] = False
                    last_values[sensor]["heating_end_time"] = None

            if last_values[sensor]["humidifying"]:
                if prev_hum >= HUM_TARGET:
                    last_values[sensor]["humidifying"] = False
                    last_values[sensor]["humidifying_end_time"] = None
                elif now < last_values[sensor]["humidifying_end_time"]:
                    steps = max(1, int(HUM_ADJUST_DURATION_SEC / 5))
                    adjust_step = HUM_TOTAL_ADJUST / steps
                    new_hum = prev_hum + adjust_step + random.uniform(-0.5, 0.5)
                else:
                    last_values[sensor]["humidifying"] = False
                    last_values[sensor]["humidifying_end_time"] = None

            elif last_values[sensor]["dehumidifying"]:
                if prev_hum <= HUM_TARGET:
                    last_values[sensor]["dehumidifying"] = False
                    last_values[sensor]["dehumidifying_end_time"] = None
                elif now < last_values[sensor]["dehumidifying_end_time"]:
                    steps = max(1, int(HUM_ADJUST_DURATION_SEC / 5))
                    adjust_step = HUM_TOTAL_ADJUST / steps
                    new_hum = prev_hum - adjust_step + random.uniform(-0.5, 0.5)
                else:
                    last_values[sensor]["dehumidifying"] = False
                    last_values[sensor]["dehumidifying_end_time"] = None

            if last_values[sensor]["purifying"]:
                if prev_water_quality >= WATER_QUALITY_TARGET:
                    last_values[sensor]["purifying"] = False
                    last_values[sensor]["purifying_end_time"] = None
                elif now < last_values[sensor]["purifying_end_time"]:
                    steps = max(1, int(WATER_PURIFY_DURATION_SEC / 5))
                    purify_step = WATER_TOTAL_INCREASE / steps
                    new_water_quality = prev_water_quality + purify_step + random.uniform(-0.02, 0.02)
                else:
                    last_values[sensor]["purifying"] = False
                    last_values[sensor]["purifying_end_time"] = None

            new_temp = max(10.0, min(50.0, new_temp))
            new_hum = max(10.0, min(90.0, new_hum))
            new_water_quality = max(0.0, min(1.0, new_water_quality))

            last_values[sensor]["timestamp"] = now.isoformat()
            last_values[sensor]["temperature"] = round(new_temp, 2)
            last_values[sensor]["humidity"] = round(new_hum, 2)
            last_values[sensor]["waterQuality"] = round(new_water_quality, 2)
            last_values[sensor]["insectPresence"] = random.choices([0, 1], weights=[90, 10], k=1)[0]

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