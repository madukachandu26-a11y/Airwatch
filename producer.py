import requests
import json
import time
from pathlib import Path
from datetime import datetime, timezone
from kafka import KafkaProducer

API_URL = "https://data.sensor.community/static/v1/data.json"
KAFKA_SERVER = "localhost:9092"
TOPIC = "air-quality"

FETCH_INTERVAL = 60

BASE_DIR = Path(__file__).resolve().parent
SELECTED_SENSOR_FILE = BASE_DIR / "data" / "selected_live_sensor_ids.json"

with open(SELECTED_SENSOR_FILE, "r") as file:
    SELECTED_SENSOR_IDS = set(json.load(file))

if len(SELECTED_SENSOR_IDS) != 30:
    raise ValueError(
        f"Expected 30 selected sensors, found {len(SELECTED_SENSOR_IDS)}"
    )

HEADERS = {
    "User-Agent": "AirWatch-Student-Project/1.0"
}

PARAMETER_NAMES = {
    "P1": "PM10",
    "P2": "PM2.5",
    "temperature": "temperature",
    "humidity": "humidity"
}


def fetch_sensor_data():
    response = requests.get(API_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: str(key).encode("utf-8"),
        acks="all",
        retries=5
    )


def send_new_measurements(records, producer, seen_event_ids):

    futures = []
    sent_ids = []

    for record in records:

        location = record.get("location", {})

        if location.get("country") != "DE":
            continue

        sensor = record.get("sensor", {})
        sensor_id = str(sensor.get("id", "unknown"))

        if sensor_id not in SELECTED_SENSOR_IDS:
            continue

        for measurement in record.get("sensordatavalues", []):

            measurement_id = measurement.get("id")

            if measurement_id is None:
                continue

            event_id = str(measurement_id)

            if event_id in seen_event_ids:
                continue

            value_type = measurement.get("value_type")

            if value_type not in PARAMETER_NAMES:
                continue

            try:
                value = float(
                    str(measurement.get("value")).replace(",", ".")
                )
            except (ValueError, TypeError):
                print("Skipping invalid value:", measurement.get("value"))
                continue

            # Capture one consistent timestamp immediately before sending
            sent_at_ms = time.time_ns() / 1_000_000

            sent_at_iso = datetime.fromtimestamp(
                sent_at_ms / 1000,
                tz=timezone.utc
            ).isoformat()

            event = {
                "event_id": event_id,
                "sensor_id": sensor_id,
                "timestamp": record.get("timestamp"),

                # Producer publication timestamps
                "producer_sent_at_ms": sent_at_ms,
                "producer_sent_at": sent_at_iso,

                "country": location.get("country"),
                "latitude": location.get("latitude"),
                "longitude": location.get("longitude"),
                "parameter": PARAMETER_NAMES[value_type],
                "value": value,
                "sensor_type": sensor.get(
                    "sensor_type", {}
                ).get("name")
            }

            try:
                future = producer.send(
                    TOPIC,
                    key=sensor_id,
                    value=event
                )

                futures.append(future)
                sent_ids.append(event_id)

            except Exception as error:
                print("Message sending error:", error)

    producer.flush(timeout=30)

    successful = 0

    for future, event_id in zip(futures, sent_ids):
        try:
            future.get(timeout=10)
            seen_event_ids.add(event_id)
            successful += 1

        except Exception as error:
            print("Kafka delivery error:", error)

    return successful


def main():

    producer = None
    seen_event_ids = set()

    try:
        producer = create_producer()

        print("AirWatch continuous producer started.")
        print("Kafka:", KAFKA_SERVER)
        print("Topic:", TOPIC)
        print("Fetch interval:", FETCH_INTERVAL, "seconds")
        print("Selected sensors:", len(SELECTED_SENSOR_IDS))
        print("Only German selected sensors will be sent.")

        while True:

            cycle_start = time.monotonic()

            try:
                print("\nFetching Sensor.Community data...")

                records = fetch_sensor_data()

                print("Records received:", len(records))

                sent = send_new_measurements(
                    records,
                    producer,
                    seen_event_ids
                )

                print("New selected measurements sent:", sent)
                print(
                    "Total unique events sent this run:",
                    len(seen_event_ids)
                )

            except requests.RequestException as error:
                print("API connection error:", error)
                print("Will retry in the next cycle.")

            except Exception as error:
                print("Producer cycle error:", error)

            elapsed = time.monotonic() - cycle_start
            wait_time = max(0, FETCH_INTERVAL - elapsed)

            print(f"Next collection in {wait_time:.1f} seconds.")
            time.sleep(wait_time)

    except KeyboardInterrupt:
        print("\nAirWatch producer stopped by user.")

    finally:
        if producer is not None:
            producer.flush()
            producer.close()

        print("Producer connection closed.")


if __name__ == "__main__":
    main()
producer file