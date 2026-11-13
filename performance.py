
import sqlite3
from datetime import datetime, timezone

DATABASE = "airwatch.db"


def initialize_performance_db():

    connection = sqlite3.connect(DATABASE)

    try:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS performance_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recorded_at TEXT NOT NULL,
                processing_latency_ms REAL NOT NULL,
                was_duplicate INTEGER NOT NULL,
                quality_status TEXT NOT NULL,
                producer_to_consumer_latency_ms REAL,
                live_delivery_latency_ms REAL,
                replay_delay_ms REAL
            )
        """)

        columns = {
            row[1]
            for row in connection.execute(
                "PRAGMA table_info(performance_metrics)"
            ).fetchall()
        }

        migrations = {
            "producer_to_consumer_latency_ms":
                "REAL",
            "live_delivery_latency_ms":
                "REAL",
            "replay_delay_ms":
                "REAL"
        }

        for column, data_type in migrations.items():
            if column not in columns:
                connection.execute(
                    f"ALTER TABLE performance_metrics "
                    f"ADD COLUMN {column} {data_type}"
                )

        connection.execute("""
            CREATE INDEX IF NOT EXISTS
            idx_performance_recorded_at
            ON performance_metrics(recorded_at)
        """)

        connection.commit()

    finally:
        connection.close()


def record_performance(
    latency_ms,
    was_duplicate,
    quality_status,
    live_delivery_latency_ms=None,
    replay_delay_ms=None
):

    connection = sqlite3.connect(DATABASE)

    try:
        connection.execute("""
            INSERT INTO performance_metrics (
                recorded_at,
                processing_latency_ms,
                was_duplicate,
                quality_status,
                live_delivery_latency_ms,
                replay_delay_ms
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            datetime.now(timezone.utc).isoformat(),
            float(latency_ms),
            int(was_duplicate),
            quality_status,
            (
                float(live_delivery_latency_ms)
                if live_delivery_latency_ms is not None
                else None
            ),
            (
                float(replay_delay_ms)
                if replay_delay_ms is not None
                else None
            )
        ))

        connection.commit()

    finally:
        connection.close()
