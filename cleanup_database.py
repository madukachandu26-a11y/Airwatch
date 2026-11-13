
import sqlite3
from pathlib import Path

db_path = Path(__file__).resolve().parent / "airwatch.db"

tables = [
    "alerts",
    "quality_checks",
    "measurements",
    "performance_metrics",
    "kafka_lag_metrics",
]

with sqlite3.connect(db_path) as conn:
    conn.execute("PRAGMA foreign_keys = OFF")

    existing = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }

    for table in tables:
        if table in existing:
            conn.execute(f'DELETE FROM "{table}"')
            print(f"Cleared {table}")

    if "sqlite_sequence" in existing:
        conn.execute("DELETE FROM sqlite_sequence")

    conn.commit()

    print("\nVerification:")
    for table in tables:
        if table in existing:
            count = conn.execute(
                f'SELECT COUNT(*) FROM "{table}"'
            ).fetchone()[0]
            print(f"{table}: {count}")

    conn.execute("VACUUM")

# Delete old database backups
for filename in [
    "airwatch.db.before-cleanup",
    "airwatch.db.pre-rebuild.sqlite",
]:
    backup = db_path.parent / filename
    if backup.exists():
        backup.unlink()
        print(f"Deleted {filename}")

print("\nDatabase cleanup completed.")
