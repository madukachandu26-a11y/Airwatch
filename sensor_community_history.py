
import os
import calendar
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ==================================================
# 1. CONFIGURATION
# ==================================================

ARCHIVE_URL = "https://archive.sensor.community"

START_DATE = date(2026, 5, 12)
END_DATE = date(2026, 11, 10)

SENSORS_PER_CITY = 5
MAX_WORKERS = 2

SENSOR_FILE = "sensor_community_sensors.csv"
CACHE_DIR = "data/sensor_community_history_cache"
MONTHLY_DIR = "data/sensor_community_monthly"
OUTPUT_FILE = "data/sensor_community_pilot.csv"
FAILED_DIR = "data/sensor_community_failed"

PARTICULATE_TYPES = [
    "SDS011",
    "SPS30",
    "PMS5003",
    "PMS7003",
    "HPM",
    "SEN5x",
]

HEADERS = {
    "User-Agent": "AirWatch-University-Project/1.0"
}


# ==================================================
# 2. CREATE HTTP SESSION
# ==================================================

def create_session():

    retry_strategy = Retry(
        total=2,
        connect=2,
        read=2,
        status=2,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy
    )

    session = requests.Session()

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update({
        "User-Agent": "AirWatch-AirQualityMonitoring/1.0"
    })

    return session


# ==================================================
# 3. SELECT SENSORS
# ==================================================

def select_test_sensors():

    print("\nReading sensor list...")

    if not os.path.exists(SENSOR_FILE):
        raise FileNotFoundError(
            f"{SENSOR_FILE} not found."
        )

    sensors = pd.read_csv(SENSOR_FILE)

    sensors["sensor_type"] = (
        sensors["sensor_type"]
        .astype(str)
        .str.strip()
    )

    valid_types = [
        item.upper()
        for item in PARTICULATE_TYPES
    ]

    sensors = sensors[
        sensors["sensor_type"].str.upper().isin(valid_types)
    ].copy()

    selected = (
        sensors.groupby(
            "city",
            group_keys=False
        )
        .head(SENSORS_PER_CITY)
        .reset_index(drop=True)
    )

    print("\nSelected sensors by city:")

    print(
        selected.groupby("city")["sensor_id"]
        .nunique()
        .to_string()
    )

    print("\nTotal selected sensors:", len(selected))

    return selected


# ==================================================
# 4. GENERATE MONTHS
# ==================================================

def get_month_ranges():

    ranges = []

    current = START_DATE

    while current <= END_DATE:

        last_day = calendar.monthrange(
            current.year,
            current.month
        )[1]

        month_end = min(
            date(
                current.year,
                current.month,
                last_day
            ),
            END_DATE
        )

        ranges.append((current, month_end))

        current = month_end + timedelta(days=1)

    return ranges


# ==================================================
# 5. DOWNLOAD ONE SENSOR FOR ONE DAY
# ==================================================

def download_sensor_day(sensor, measurement_date):

    city = str(sensor["city"])
    sensor_id = str(sensor["sensor_id"])

    sensor_type = (
        str(sensor["sensor_type"])
        .strip()
        .lower()
    )

    date_text = measurement_date.isoformat()

    cache_folder = os.path.join(
        CACHE_DIR,
        city,
        date_text
    )

    os.makedirs(cache_folder, exist_ok=True)

    cache_file = os.path.join(
        cache_folder,
        f"{sensor_type}_{sensor_id}.csv"
    )

    missing_file = cache_file + ".missing"

    # Reuse successful cached file
    if os.path.exists(cache_file):

        try:

            df = pd.read_csv(
                cache_file,
                sep=";"
            )

            if not df.empty and "timestamp" in df.columns:

                df["city"] = city

                return city, sensor_id, df, "cached"

            os.remove(cache_file)

        except Exception:

            try:
                os.remove(cache_file)
            except OSError:
                pass

    # Reuse confirmed 404 result
    if os.path.exists(missing_file):

        return city, sensor_id, None, "not_found_cached"

    url = (
        f"{ARCHIVE_URL}/{date_text}/"
        f"{date_text}_{sensor_type}_sensor_{sensor_id}.csv"
    )

    session = create_session()

    try:

        response = session.get(
            url,
            timeout=(20, 120)
        )

        if response.status_code == 404:

            with open(missing_file, "w") as file:
                file.write("404")

            return city, sensor_id, None, "not_found"

        response.raise_for_status()

        df = pd.read_csv(
            StringIO(response.text),
            sep=";"
        )

        if df.empty:
            return city, sensor_id, None, "empty"

        if (
            "P1" not in df.columns
            and "P2" not in df.columns
        ):
            return city, sensor_id, None, "no_pm_columns"

        # Save successful original CSV
        df.to_csv(
            cache_file,
            sep=";",
            index=False
        )

        df["city"] = city

        return city, sensor_id, df, "downloaded"

    except requests.RequestException as error:

        return city, sensor_id, None, f"error: {error}"

    except Exception as error:

        return city, sensor_id, None, f"error: {error}"

    finally:

        session.close()


# ==================================================
# 6. CLEAN AND COMBINE DATA
# ==================================================

def clean_history(all_data):

    if not all_data:
        return pd.DataFrame()

    history = pd.concat(
        all_data,
        ignore_index=True
    )

    if "P1" not in history.columns:
        history["P1"] = pd.NA

    if "P2" not in history.columns:
        history["P2"] = pd.NA

    history["P1"] = pd.to_numeric(
        history["P1"],
        errors="coerce"
    )

    history["P2"] = pd.to_numeric(
        history["P2"],
        errors="coerce"
    )

    history["timestamp"] = pd.to_datetime(
        history["timestamp"],
        errors="coerce",
        utc=True
    )

    history = history.dropna(
        subset=["timestamp"]
    )

    history = history.drop_duplicates(
        subset=["sensor_id", "timestamp"]
    )

    return history


# ==================================================
# 7. SAVE FINAL OUTPUT FROM COMPLETED MONTHS
# ==================================================

def rebuild_final_output():

    monthly_files = sorted([
        os.path.join(MONTHLY_DIR, name)
        for name in os.listdir(MONTHLY_DIR)
        if name.endswith(".csv")
    ])

    if not monthly_files:
        print("\nNo completed monthly files yet.")
        return

    monthly_data = []

    for file_path in monthly_files:

        try:

            df = pd.read_csv(file_path)

            if not df.empty:
                monthly_data.append(df)

        except Exception as error:

            print(
                f"Could not read {file_path}: {error}"
            )

    if not monthly_data:
        return

    history = clean_history(monthly_data)

    if history.empty:
        return

    history.to_csv(
        OUTPUT_FILE,
        index=False
    )

    print(
        "\nUpdated final historical CSV:",
        OUTPUT_FILE
    )

    print("Total records:", len(history))


# ==================================================
# 8. PROCESS ONE MONTH
# ==================================================

def process_month(sensors, month_start, month_end):

    month_name = month_start.strftime("%Y-%m")

    print("\n====================================")
    print("Processing month:", month_name)
    print("From:", month_start)
    print("To:", month_end)
    print("====================================")

    dates = []

    current = month_start

    while current <= month_end:
        dates.append(current)
        current += timedelta(days=1)

    jobs = []

    for _, sensor in sensors.iterrows():

        for measurement_date in dates:

            jobs.append(
                (sensor, measurement_date)
            )

    total_jobs = len(jobs)

    print("Total tasks:", total_jobs)

    all_data = []

    counts = {
        "downloaded": 0,
        "cached": 0,
        "not_found": 0,
        "not_found_cached": 0,
        "empty": 0,
        "no_pm_columns": 0,
        "errors": 0,
    }

    failed_tasks = []

    completed = 0

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                download_sensor_day,
                sensor,
                measurement_date
            ): (
                sensor,
                measurement_date
            )
            for sensor, measurement_date in jobs
        }

        for future in as_completed(futures):

            sensor, measurement_date = futures[future]

            try:

                city, sensor_id, df, status = future.result()

            except Exception as error:

                counts["errors"] += 1

                failed_tasks.append({
                    "city": sensor["city"],
                    "sensor_id": sensor["sensor_id"],
                    "date": measurement_date,
                    "error": str(error),
                })

                completed += 1
                continue

            completed += 1

            if status in counts:
                counts[status] += 1

            if status.startswith("error"):

                counts["errors"] += 1

                failed_tasks.append({
                    "city": city,
                    "sensor_id": sensor_id,
                    "date": measurement_date,
                    "error": status,
                })

            if df is not None and not df.empty:

                df["city"] = city

                if "P1" not in df.columns:
                    df["P1"] = pd.NA

                if "P2" not in df.columns:
                    df["P2"] = pd.NA

                all_data.append(df)

            if completed % 20 == 0 or completed == total_jobs:

                percentage = (
                    completed / total_jobs
                ) * 100

                print(
                    f"Progress: {completed}/{total_jobs} "
                    f"({percentage:.1f}%)"
                )

    print("\nMonth summary:")

    for status, count in counts.items():
        print(f"{status}: {count}")

    # Save failed task list for inspection and later retry
    os.makedirs(FAILED_DIR, exist_ok=True)

    failed_file = os.path.join(
        FAILED_DIR,
        f"{month_name}_failed.csv"
    )

    if failed_tasks:

        pd.DataFrame(failed_tasks).to_csv(
            failed_file,
            index=False
        )

        print(
            "\nFailed downloads saved to:",
            failed_file
        )

    elif os.path.exists(failed_file):

        os.remove(failed_file)

    # Do not mark this month complete if downloads failed
    if counts["errors"] > 0:

        print(
            "\nThis month has download errors."
            "\nIts monthly output was not marked complete."
            "\nRun the script again later to retry missing files."
        )

        return False

    history = clean_history(all_data)

    if history.empty:

        print(
            "\nNo valid measurements found for this month."
        )

        return False

    os.makedirs(MONTHLY_DIR, exist_ok=True)

    month_file = os.path.join(
        MONTHLY_DIR,
        f"{month_name}.csv"
    )

    history.to_csv(
        month_file,
        index=False
    )

    print(
        "\nCompleted month saved:",
        month_file
    )

    print("Records:", len(history))

    return True


# ==================================================
# 9. MAIN COLLECTION
# ==================================================

def collect_pilot_history():

    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(MONTHLY_DIR, exist_ok=True)
    os.makedirs(FAILED_DIR, exist_ok=True)
    os.makedirs("data", exist_ok=True)

    sensors = select_test_sensors()

    if sensors.empty:
        print("\nNo particulate sensors found.")
        return

    months = get_month_ranges()

    print("\nHistorical collection started.")
    print("Date range:", START_DATE, "to", END_DATE)
    print("Sensors:", len(sensors))
    print("Months:", len(months))
    print("Workers:", MAX_WORKERS)

    for month_start, month_end in months:

        month_name = month_start.strftime("%Y-%m")

        month_file = os.path.join(
            MONTHLY_DIR,
            f"{month_name}.csv"
        )

        print("\nChecking month:", month_name)

        # Reprocess months with no completed output.
        # Existing successful daily cache files are reused.
        if os.path.exists(month_file):

            print("Already completed. Skipping:", month_name)
            continue

        completed = process_month(
            sensors,
            month_start,
            month_end
        )

        rebuild_final_output()

        if not completed:

            print(
                "\nStopping here so this month can be retried."
            )

            break

    rebuild_final_output()

    print("\n========== COLLECTION STATUS ==========")

    print("Monthly files:")

    for name in sorted(os.listdir(MONTHLY_DIR)):

        if name.endswith(".csv"):
            print(" -", name)

    print("\nFinal output:", OUTPUT_FILE)


# ==================================================
# 10. RUN
# ==================================================

if __name__ == "__main__":

    try:

        collect_pilot_history()

    except KeyboardInterrupt:

        print("\nCollection interrupted.")
        print("Successful daily cache files have been preserved.")

    except Exception as error:

        print("\nProgram error:", error)
