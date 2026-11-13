
import sqlite3
import subprocess
from datetime import datetime, timezone

DATABASE = "airwatch.db"
KAFKA_SERVER = "localhost:9092"
CONSUMER_GROUP = "airwatch-consumer-realtime"
TOPIC = "air-quality"


def initialize_lag_table():

    connection = sqlite3.connect(DATABASE)

    try:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS kafka_lag_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recorded_at TEXT NOT NULL,
                partition_id INTEGER,
                current_offset INTEGER,
                log_end_offset INTEGER,
                lag INTEGER
            )
        """)

        connection.commit()

    finally:
        connection.close()


def collect_consumer_lag():

    command = [
        "kafka-consumer-groups",
        "--bootstrap-server", KAFKA_SERVER,
        "--describe",
        "--group", CONSUMER_GROUP
    ]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=15
    )

    if result.returncode != 0:
        raise RuntimeError(result.stderr)

    rows = []

    for line in result.stdout.splitlines():

        parts = line.split()

        if len(parts) < 7:
            continue

        if parts[0] != CONSUMER_GROUP or parts[1] != TOPIC:
            continue

        try:
            rows.append({
                "partition_id": int(parts[2]),
                "current_offset": int(parts[3]),
                "log_end_offset": int(parts[4]),
                "lag": int(parts[5])
            })

        except ValueError:
            continue

    return rows


def save_consumer_lag(rows):

    connection = sqlite3.connect(DATABASE)

    try:
        recorded_at = datetime.now(
            timezone.utc
        ).isoformat()

        for row in rows:

            connection.execute("""
                INSERT INTO kafka_lag_metrics (
                    recorded_at,
                    partition_id,
                    current_offset,
                    log_end_offset,
                    lag
                )
                VALUES (?, ?, ?, ?, ?)
            """, (
                recorded_at,
                row["partition_id"],
                row["current_offset"],
                row["log_end_offset"],
                row["lag"]
            ))

        connection.commit()

    finally:
        connection.close()
