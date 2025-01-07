import random
import time

sensorTypes = ["airQuality", "humidity", "light", "motion", "temperature"]
models = ["AQ-Model-X", "H-Model-A", "LX-Model-Z", "MotionPro-2", "T-Model-Q"]
locations = [
    "101 Main St, Springfield, ST",
    "202 Move Rd, ActiveTown, ST",
    "303 Heat Rd, HotCity, ST",
    "201 Light Ave, BrightCity, ST"
]
connectivities = ["online", "offline", "intermittent"]
manufacturers = ["Acme Corp", "LightCorp", "MotionMaker", ""]

print('"id","serialNumber","sensorType","model","userId","location","unitModel","status","lastUpdated","batteryLevel","connectivity","manufacturer","firmwareVersion","createdAt","updatedAt"')

for i in range(1, 201):  # 200 rows
    sid = f"sensor-{i}"
    sn = f"SN-{i:04d}"
    stype = random.choice(sensorTypes)
    model = random.choice(models)
    uid = f"user-{100 + i}"
    loc = random.choice(locations)
    if stype == "airQuality":
        co2 = round(random.uniform(390, 410), 1)
        voc = round(random.uniform(0.1, 0.3), 2)
        unitModel = f'"{{\\"AirQuality\\":{{\\"co2Level\\":{co2},\\"vocLevel\\":{voc},\\"unit\\":\\"ppm\\"}}}}"'
    elif stype == "humidity":
        hval = round(random.uniform(40, 70), 1)
        unitModel = f'"{{\\"Humidity\\":{{\\"value\\":{hval},\\"unit\\":\\"%\\"}}}}"'
    elif stype == "light":
        lum = round(random.uniform(800, 2000), 1)
        unitModel = f'"{{\\"LightIntensity\\":{{\\"value\\":{lum},\\"unit\\":\\"lumens\\"}}}}"'
    elif stype == "motion":
        det = random.choice(["true", "false"])
        ts_motion = int(time.time() * 1000) - random.randint(0, 10000000)
        unitModel = f'"{{\\"Motion\\":{{\\"detected\\":{det},\\"timestamp\\":{ts_motion}}}}}"'
    else:  # temperature
        temp = round(random.uniform(15, 30), 1)
        unitModel = f'"{{\\"Temperature\\":{{\\"value\\":{temp},\\"unit\\":\\"C\\"}}}}"'

    status = "active"
    now_ms = int(time.time() * 1000)
    lastUpdated = now_ms - random.randint(0, 5000000)
    battery = round(random.uniform(0, 100), 1) if random.random() > 0.3 else ""  # 30% chance null
    conn = random.choice(connectivities)
    manuf = random.choice(manufacturers)
    fw = f"{random.randint(1,3)}.{random.randint(0,9)}" if random.random() > 0.3 else ""
    createdAt = now_ms - random.randint(5000000, 10000000)
    updatedAt = now_ms - random.randint(0, 1000000)

    print(f'"{sid}","{sn}","{stype}","{model}","user-{i}","{loc}",{unitModel},"{status}",'
          f'"{lastUpdated}","{battery}","{conn}","{manuf}","{fw}","{createdAt}","{updatedAt}"')
