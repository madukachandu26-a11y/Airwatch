
import json
import time
from pathlib import Path
from collections import deque

from kafka import KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndMetadata

from database import (
    initialize_db,
    save_measurement,
    save_alert,
    save_quality_check
)

from detector import detect_anomaly
from data_quality import validate_measurement

from performance import (
    initialize_performance_db,
    record_performance
)


# --------------------------------------------------
# KAFKA CONFIGURATION
# --------------------------------------------------

KAFKA_SERVER = "localhost:9092"
TOPIC = "air-quality"
GROUP_ID = "airwatch-consumer-realtime"

COMMIT_BATCH_SIZE = 1000

# Messages received within 5 minutes are classified
# as live delivery messages.
LIVE_MESSAGE_WINDOW_MS = 5 * 60 * 1000


# --------------------------------------------------
# LOAD SELECTED SENSOR IDS
# --------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent

SELECTED_SENSOR_FILE = (
    BASE_DIR / "data" / "selected_live_sensor_ids.json"
)

with open(SELECTED_SENSOR_FILE, "r") as file:
    SELECTED_SENSOR_IDS = set(json.load(file))


if len(SELECTED_SENSOR_IDS) != 30:
    raise ValueError(
        f"Expected 30 selected sensors, "
        f"found {len(SELECTED_SENSOR_IDS)}"
    )


# --------------------------------------------------
# START CONSUMER
# --------------------------------------------------

def start_consumer():

    initialize_db()
    initialize_performance_db()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda message: json.loads(
            message.decode("utf-8")
        )
    )

    print("\nAirWatch consumer started.")
    print("Kafka:", KAFKA_SERVER)
    print("Topic:", TOPIC)
    print("Consumer group:", GROUP_ID)
    print("Selected sensors:", len(SELECTED_SENSOR_IDS))
    print("Skipped-message commit batch:", COMMIT_BATCH_SIZE)
    print("Live message window:", LIVE_MESSAGE_WINDOW_MS / 1000, "seconds")
    print("Waiting for Kafka messages...\n")


    # --------------------------------------------------
    # PERFORMANCE COUNTERS
    # --------------------------------------------------

    processed = 0
    inserted = 0
    duplicates = 0

    skipped_sensors = 0
    skipped_other = 0

    valid_count = 0
    suspicious_count = 0
    invalid_count = 0

    session_start = time.perf_counter()

    recent_completions = deque()

    # Store completed offsets separately for each partition
    pending_offsets = {}

    messages_since_commit = 0


    # --------------------------------------------------
    # KAFKA OFFSET COMMIT FUNCTIONS
    # --------------------------------------------------

    def commit_pending():

        nonlocal messages_since_commit

        if pending_offsets:

            consumer.commit(
                offsets=pending_offsets.copy()
            )

            pending_offsets.clear()
            messages_since_commit = 0


    def mark_message_complete(message):

        nonlocal messages_since_commit

        partition = TopicPartition(
            message.topic,
            message.partition
        )

        pending_offsets[partition] = OffsetAndMetadata(
            message.offset + 1,
            ""
        )

        messages_since_commit += 1


    # --------------------------------------------------
    # MAIN CONSUMER LOOP
    # --------------------------------------------------

    try:

        for message in consumer:

            message_start = time.perf_counter()

            # Actual Kafka message receive timestamp
            consumer_received_at_ms = (
                time.time_ns() / 1_000_000
            )

            event = message.value


            # --------------------------------------------------
            # MESSAGE STRUCTURE VALIDATION
            # --------------------------------------------------

            required_fields = [
                "event_id",
                "sensor_id",
                "timestamp",
                "country",
                "latitude",
                "longitude",
                "parameter",
                "value",
                "sensor_type"
            ]

            if not isinstance(event, dict) or not all(
                field in event for field in required_fields
            ):

                skipped_other += 1

                mark_message_complete(message)

                if messages_since_commit >= COMMIT_BATCH_SIZE:
                    commit_pending()

                continue


            # --------------------------------------------------
            # SELECTED SENSOR FILTER
            # --------------------------------------------------

            if str(event["sensor_id"]) not in SELECTED_SENSOR_IDS:

                skipped_sensors += 1

                mark_message_complete(message)

                if messages_since_commit >= COMMIT_BATCH_SIZE:
                    commit_pending()

                continue


            # --------------------------------------------------
            # PARAMETER VALIDATION
            # --------------------------------------------------

            if event["parameter"] not in [
                "PM10",
                "PM2.5",
                "temperature",
                "humidity"
            ]:

                skipped_other += 1

                mark_message_complete(message)

                if messages_since_commit >= COMMIT_BATCH_SIZE:
                    commit_pending()

                continue


            # --------------------------------------------------
            # DATA QUALITY VALIDATION
            # --------------------------------------------------

            quality_result = validate_measurement(event)


            # --------------------------------------------------
            # SAVE MEASUREMENT
            # --------------------------------------------------

            was_inserted = save_measurement(event)

            save_quality_check(
                event,
                quality_result
            )

            processed += 1

            if was_inserted:
                inserted += 1
            else:
                duplicates += 1


            # --------------------------------------------------
            # POLLUTION ANOMALY DETECTION
            # --------------------------------------------------

            status = quality_result["status"]

            if status == "VALID":

                valid_count += 1

                result = detect_anomaly(event)

                if result["is_anomaly"]:

                    alert_inserted = save_alert(
                        event,
                        result
                    )

                    if alert_inserted:

                        print(
                            "POLLUTION ALERT:",
                            result["alert"],
                            "| Sensor:",
                            event["sensor_id"],
                            "|",
                            result["message"]
                        )

            elif status == "SUSPICIOUS":

                suspicious_count += 1

            else:

                invalid_count += 1


            # --------------------------------------------------
            # KAFKA OFFSET COMMIT
            # --------------------------------------------------

            # Selected message is marked complete only after
            # its database operations finish.
            mark_message_complete(message)

            # Commit selected message and preceding completed
            # skipped messages.
            commit_pending()


            # --------------------------------------------------
            # PROCESSING LATENCY
            # --------------------------------------------------

            completed_at = time.perf_counter()

            processing_latency_ms = (
                completed_at - message_start
            ) * 1000


            # --------------------------------------------------
            # LIVE DELIVERY LATENCY AND REPLAY DELAY
            # --------------------------------------------------

            producer_timestamp = event.get(
                "producer_sent_at_ms"
            )

            live_delivery_latency_ms = None
            replay_delay_ms = None

            if producer_timestamp is not None:

                try:

                    elapsed_delivery_ms = (
                        consumer_received_at_ms
                        - float(producer_timestamp)
                    )

                    if elapsed_delivery_ms >= 0:

                        if elapsed_delivery_ms <= LIVE_MESSAGE_WINDOW_MS:

                            live_delivery_latency_ms = (
                                elapsed_delivery_ms
                            )

                        else:

                            replay_delay_ms = (
                                elapsed_delivery_ms
                            )

                except (ValueError, TypeError):

                    pass


            # --------------------------------------------------
            # SAVE PERFORMANCE METRICS
            # --------------------------------------------------

            record_performance(
                latency_ms=processing_latency_ms,
                was_duplicate=not was_inserted,
                quality_status=status,
                live_delivery_latency_ms=live_delivery_latency_ms,
                replay_delay_ms=replay_delay_ms
            )


            # --------------------------------------------------
            # ROLLING THROUGHPUT
            # --------------------------------------------------

            recent_completions.append(completed_at)

            while (
                recent_completions
                and completed_at - recent_completions[0] > 10
            ):

                recent_completions.popleft()


            throughput_1s = sum(
                1
                for timestamp in recent_completions
                if completed_at - timestamp <= 1
            )

            throughput_10s = (
                len(recent_completions) / 10.0
            )


            # --------------------------------------------------
            # PERFORMANCE REPORT
            # --------------------------------------------------

            if processed % 100 == 0:

                elapsed = (
                    time.perf_counter() - session_start
                )

                average_throughput = (
                    processed / elapsed
                    if elapsed > 0
                    else 0
                )

                print("\n--- AIRWATCH PERFORMANCE ---")

                print(
                    "Processed selected messages:",
                    processed
                )

                print("Stored:", inserted)
                print("Duplicates:", duplicates)

                print(
                    "Skipped other sensors:",
                    skipped_sensors
                )

                print(
                    "Skipped unsupported/invalid:",
                    skipped_other
                )

                print("Valid:", valid_count)
                print("Suspicious:", suspicious_count)
                print("Invalid:", invalid_count)

                print(
                    f"Rolling throughput (1s): "
                    f"{throughput_1s:.2f} msg/sec"
                )

                print(
                    f"Rolling throughput (10s): "
                    f"{throughput_10s:.2f} msg/sec"
                )

                print(
                    f"Session average throughput: "
                    f"{average_throughput:.2f} msg/sec"
                )

                print(
                    f"Processing latency: "
                    f"{processing_latency_ms:.3f} ms"
                )

                if live_delivery_latency_ms is not None:

                    print(
                        f"Live delivery latency: "
                        f"{live_delivery_latency_ms:.3f} ms"
                    )

                if replay_delay_ms is not None:

                    print(
                        f"Kafka replay delay: "
                        f"{replay_delay_ms / 1000:.2f} seconds"
                    )

                print("-----------------------------\n")


    # --------------------------------------------------
    # STOP CONSUMER
    # --------------------------------------------------

    except KeyboardInterrupt:

        print("\nConsumer stopped by user.")


    finally:

        try:

            commit_pending()

            print(
                "Pending completed messages committed."
            )

        except Exception as error:

            print(
                "Final commit failed:",
                error
            )


        elapsed = (
            time.perf_counter() - session_start
        )

        consumer.close()


        # --------------------------------------------------
        # FINAL SUMMARY
        # --------------------------------------------------

        print("\n--- FINAL PROCESSING SUMMARY ---")

        print(
            "Processed selected messages:",
            processed
        )

        print("Stored:", inserted)
        print("Duplicates:", duplicates)

        print(
            "Skipped other sensors:",
            skipped_sensors
        )

        print(
            "Skipped unsupported/invalid:",
            skipped_other
        )

        print("Valid:", valid_count)
        print("Suspicious:", suspicious_count)
        print("Invalid:", invalid_count)

        if elapsed > 0:

            print(
                "Session average throughput:",
                f"{processed / elapsed:.2f} messages/second"
            )


# --------------------------------------------------
# RUN APPLICATION
# --------------------------------------------------

if __name__ == "__main__":
    start_consumer()
