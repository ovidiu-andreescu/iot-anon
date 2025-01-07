#!/usr/bin/env python3

import csv
import json
import io
from kafka import KafkaProducer
import avro.schema
from avro.io import DatumWriter, BinaryEncoder
import time

AVRO_SCHEMA_PATH = "SensorReading.avsc"
CSV_FILE_PATH = "sensors.csv"
KAFKA_TOPIC = "sensors"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

def main():
    schema = avro.schema.parse(open(AVRO_SCHEMA_PATH, "r").read())

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: v
    )

    with open(CSV_FILE_PATH, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                record_dict = build_record_dict(row)
                avro_bytes = avro_encode(record_dict, schema)
                key_str = record_dict["id"]
                producer.send(KAFKA_TOPIC, key=key_str, value=avro_bytes)
            except Exception as e:
                print(f"Error processing row {row}: {e}")

    producer.flush()
    producer.close()

def build_record_dict(row):
    def safe_long(val, default=0):
        return int(val) if val.strip() else default
    def safe_float(val):
        return float(val) if val.strip() else None

    sensor_id       = row["id"].strip('"')
    serial_number   = row["serialNumber"].strip('"')
    sensor_type     = row["sensorType"].strip('"')
    model           = row["model"].strip('"')
    user_id         = row["userId"].strip('"')
    location        = row["location"].strip('"')
    status          = row.get("status", "active").strip('"')
    last_updated    = safe_long(row.get("lastUpdated", "0"))
    battery_val_str = row.get("batteryLevel", "").strip('"')
    battery_level   = safe_float(battery_val_str)
    connectivity    = row.get("connectivity", "online").strip('"')
    manuf_str       = row.get("manufacturer", "").strip('"')
    firmware_str    = row.get("firmwareVersion", "").strip('"')
    created_at      = safe_long(row.get("createdAt", "0"))
    updated_at      = safe_long(row.get("updatedAt", "0"))

    if battery_level is None:
        battery_level = None

    manufacturer     = manuf_str if manuf_str else None
    firmware_version = firmware_str if firmware_str else None

    unit_model_str = row["unitModel"].strip()
    unit_model_str = unit_model_str.strip('"')
    unit_model_json = json.loads(unit_model_str)

    if sensor_type == "airQuality":
        actual = unit_model_json.get("AirQuality", {})
    elif sensor_type == "humidity":
        actual = unit_model_json.get("Humidity", {})
    elif sensor_type == "light":
        actual = unit_model_json.get("LightIntensity", {})
    elif sensor_type == "motion":
        actual = unit_model_json.get("Motion", {})
    elif sensor_type == "temperature":
        actual = unit_model_json.get("Temperature", {})
    else:
        actual = {}

    record_dict = {
        "id": sensor_id,
        "serialNumber": serial_number,
        "sensorType": sensor_type,
        "model": model,
        "userId": user_id,
        "location": location,
        "status": status,
        "lastUpdated": last_updated,
        "batteryLevel": battery_level,
        "connectivity": connectivity,
        "manufacturer": manufacturer,
        "firmwareVersion": firmware_version,
        "createdAt": created_at,
        "updatedAt": updated_at
    }

    if sensor_type == "airQuality":
        record_dict["unitModel"] = {
            "AirQuality": {
                "co2Level": actual.get("co2Level", 0.0),
                "vocLevel": actual.get("vocLevel", 0.0),
                "unit": actual.get("unit", "ppm")
            }
        }
    elif sensor_type == "humidity":
        record_dict["unitModel"] = {
            "Humidity": {
                "value": actual.get("value", 0.0),
                "unit": actual.get("unit", "%")
            }
        }
    elif sensor_type == "light":
        record_dict["unitModel"] = {
            "LightIntensity": {
                "value": actual.get("value", 0.0),
                "unit": actual.get("unit", "lux")
            }
        }
    elif sensor_type == "motion":
        record_dict["unitModel"] = {
            "Motion": {
                "detected": actual.get("detected", False),
                "timestamp": actual.get("timestamp", 0)
            }
        }
    elif sensor_type == "temperature":
        record_dict["unitModel"] = {
            "Temperature": {
                "value": actual.get("value", 0.0),
                "unit": actual.get("unit", "C")
            }
        }
    else:
        record_dict["unitModel"] = None

    return record_dict

def avro_encode(record_dict, schema):
    out = io.BytesIO()
    encoder = BinaryEncoder(out)
    writer = DatumWriter(schema)
    writer.write(record_dict, encoder)
    return out.getvalue()


if __name__ == "__main__":
    main()
