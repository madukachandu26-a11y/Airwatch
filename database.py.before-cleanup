
import sqlite3

DATABASE = "airwatch.db"


def initialize_db():
    connection = sqlite3.connect(DATABASE)

    try:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS measurements (
                event_id TEXT PRIMARY KEY,
                sensor_id TEXT,
                timestamp TEXT,
                country TEXT,
                latitude REAL,
                longitude REAL,
                parameter TEXT,
                value REAL,
                sensor_type TEXT
            )
        """)

        connection.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                event_id TEXT PRIMARY KEY,
                sensor_id TEXT,
                timestamp TEXT,
                parameter TEXT,
                value REAL,
                alert_type TEXT,
                message TEXT
            )
        """)

        connection.execute("""
            CREATE TABLE IF NOT EXISTS quality_checks (
                event_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                reason TEXT NOT NULL,
                checked_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (event_id) REFERENCES measurements(event_id)
            )
        """)

        connection.commit()

    finally:
        connection.close()


def save_measurement(event):
    connection = sqlite3.connect(DATABASE)

    try:
        cursor = connection.execute("""
            INSERT OR IGNORE INTO measurements
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event["event_id"],
            event["sensor_id"],
            event["timestamp"],
            event["country"],
            float(event["latitude"]),
            float(event["longitude"]),
            event["parameter"],
            float(event["value"]),
            event["sensor_type"]
        ))

        connection.commit()
        return cursor.rowcount == 1

    finally:
        connection.close()


def save_quality_check(event, result):
    connection = sqlite3.connect(DATABASE)

    try:
        cursor = connection.execute("""
            INSERT OR REPLACE INTO quality_checks
            (event_id, status, reason)
            VALUES (?, ?, ?)
        """, (
            event["event_id"],
            result["status"],
            result["reason"]
        ))

        connection.commit()
        return cursor.rowcount == 1

    finally:
        connection.close()


def save_alert(event, result):
    connection = sqlite3.connect(DATABASE)

    try:
        cursor = connection.execute("""
            INSERT OR IGNORE INTO alerts
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            event["event_id"],
            event["sensor_id"],
            event["timestamp"],
            event["parameter"],
            float(event["value"]),
            result["alert"],
            result["message"]
        ))

        connection.commit()
        return cursor.rowcount == 1

    finally:
        connection.close()
