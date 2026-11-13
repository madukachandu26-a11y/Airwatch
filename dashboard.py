
import sqlite3
import pandas as pd
import plotly.express as px
import streamlit as st

# ============================================================
# CONFIGURATION
# ============================================================

DATABASE = "airwatch.db"
HISTORICAL_FILE = "data/sensor_community_cleaned.csv"

DETAIL_LIMIT = 5000
TABLE_LIMIT = 100
LIVE_WINDOW_MINUTES = 30
HISTORICAL_TIMEZONE = "Europe/Berlin"

st.set_page_config(
    page_title="AirWatch",
    page_icon="🌍",
    layout="wide"
)

CITY_BOUNDS = {
    "Hamburg": {
        "lat_min": 53.35, "lat_max": 53.75,
        "lon_min": 9.70, "lon_max": 10.30,
        "lat": 53.5511, "lon": 9.9937,
    },
    "Kiel": {
        "lat_min": 54.25, "lat_max": 54.45,
        "lon_min": 9.90, "lon_max": 10.25,
        "lat": 54.3233, "lon": 10.1228,
    },
    "Berlin": {
        "lat_min": 52.30, "lat_max": 52.70,
        "lon_min": 13.00, "lon_max": 13.80,
        "lat": 52.5200, "lon": 13.4050,
    },
    "Bremen": {
        "lat_min": 53.00, "lat_max": 53.25,
        "lon_min": 8.60, "lon_max": 9.00,
        "lat": 53.0793, "lon": 8.8017,
    },
    "Hannover": {
        "lat_min": 52.25, "lat_max": 52.50,
        "lon_min": 9.55, "lon_max": 9.90,
        "lat": 52.3759, "lon": 9.7320,
    },
    "Munich": {
        "lat_min": 48.00, "lat_max": 48.25,
        "lon_min": 11.35, "lon_max": 11.75,
        "lat": 48.1372, "lon": 11.5756,
    },
}

# ============================================================
# STYLING
# ============================================================

st.markdown(
    """
    <style>
    .stApp {
        background-color: #F5F7FA;
    }

    h1, h2, h3 {
        color: #1F2937;
    }

    [data-testid="stMetric"] {
        background: white;
        border: 1px solid #E5E7EB;
        padding: 18px;
        border-radius: 12px;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# ============================================================
# DATABASE FUNCTIONS
# ============================================================

def load_data(query, params=None):

    connection = sqlite3.connect(DATABASE, timeout=10)

    try:
        return pd.read_sql_query(
            query,
            connection,
            params=params
        )
    finally:
        connection.close()


def assign_city(dataframe):

    result = dataframe.copy()
    result["City"] = None

    if result.empty:
        return result

    result["latitude"] = pd.to_numeric(
        result["latitude"], errors="coerce"
    )

    result["longitude"] = pd.to_numeric(
        result["longitude"], errors="coerce"
    )

    for city, bounds in CITY_BOUNDS.items():

        inside = (
            result["latitude"].between(
                bounds["lat_min"], bounds["lat_max"]
            )
            &
            result["longitude"].between(
                bounds["lon_min"], bounds["lon_max"]
            )
        )

        result.loc[
            inside & result["City"].isna(),
            "City"
        ] = city

    return result


def classify_pollution(value, parameter):

    if pd.isna(value):
        return "No data"

    if parameter == "PM2.5":

        if value > 25:
            return "High"

        if value <= 12:
            return "Low"

        return "Normal"

    if value > 50:
        return "High"

    if value <= 20:
        return "Low"

    return "Normal"


def classify_city_pollution(pm25, pm10):

    if pd.isna(pm25) or pd.isna(pm10):
        return "Insufficient data"

    # Project-defined thresholds.
    if pm25 > 25 or pm10 > 50:
        return "High"

    if pm25 <= 12 and pm10 <= 20:
        return "Low"

    return "Normal"


# ============================================================
# HISTORICAL CITY STATUS
# ============================================================

@st.cache_data(ttl=300, show_spinner="Loading historical data...")
def get_historical_city_status():

    columns = ["city", "timestamp", "P1", "P2"]
    daily_parts = []

    try:

        chunks = pd.read_csv(
            HISTORICAL_FILE,
            usecols=columns,
            chunksize=250000
        )

        for chunk in chunks:

            chunk["timestamp"] = pd.to_datetime(
                chunk["timestamp"],
                errors="coerce",
                utc=True
            )

            chunk["P1"] = pd.to_numeric(
                chunk["P1"], errors="coerce"
            )

            chunk["P2"] = pd.to_numeric(
                chunk["P2"], errors="coerce"
            )

            chunk = chunk.dropna(subset=columns)

            chunk["Date"] = (
                chunk["timestamp"]
                .dt.tz_convert(HISTORICAL_TIMEZONE)
                .dt.date
            )

            grouped = (
                chunk.groupby(["city", "Date"])
                .agg(
                    PM10_sum=("P1", "sum"),
                    PM10_count=("P1", "count"),
                    PM25_sum=("P2", "sum"),
                    PM25_count=("P2", "count")
                )
                .reset_index()
            )

            daily_parts.append(grouped)

        if not daily_parts:
            return pd.DataFrame()

        combined = pd.concat(
            daily_parts,
            ignore_index=True
        )

        daily = (
            combined.groupby(["city", "Date"])
            .agg(
                PM10_sum=("PM10_sum", "sum"),
                PM10_count=("PM10_count", "sum"),
                PM25_sum=("PM25_sum", "sum"),
                PM25_count=("PM25_count", "sum")
            )
            .reset_index()
        )

        daily["PM10"] = (
            daily["PM10_sum"] / daily["PM10_count"]
        )

        daily["PM2.5"] = (
            daily["PM25_sum"] / daily["PM25_count"]
        )

        daily["Status"] = daily.apply(
            lambda row: classify_city_pollution(
                row["PM2.5"],
                row["PM10"]
            ),
            axis=1
        )

        return daily

    except FileNotFoundError:

        st.error(
            f"Historical file not found: {HISTORICAL_FILE}"
        )

        return pd.DataFrame()

    except Exception as error:

        st.error(
            f"Historical data error: {error}"
        )

        return pd.DataFrame()


def get_last_historical_status():

    daily = get_historical_city_status()

    if daily.empty:
        return pd.DataFrame()

    results = []

    for city in CITY_BOUNDS:

        city_data = daily[
            daily["city"] == city
        ].sort_values("Date")

        result = {"City": city}

        for status in ["Low", "Normal", "High"]:

            matches = city_data[
                city_data["Status"] == status
            ]

            if matches.empty:

                result[
                    f"Last {status} Pollution"
                ] = "Not recorded"

            else:

                latest_date = matches["Date"].max()

                result[
                    f"Last {status} Pollution"
                ] = pd.Timestamp(
                    latest_date
                ).strftime("%d %b %Y")

        results.append(result)

    return pd.DataFrame(results)


# ============================================================
# LIVE AIR QUALITY DATA
# ============================================================

def get_live_city_status():

    live_data = load_data(
        """
        SELECT
            m.sensor_id,
            m.timestamp,
            m.latitude,
            m.longitude,
            m.parameter,
            m.value,
            m.sensor_type,
            q.status AS quality_status

        FROM measurements AS m

        LEFT JOIN quality_checks AS q
            ON m.event_id = q.event_id

        WHERE datetime(m.timestamp) >=
            datetime('now', ?)

        AND m.country = 'DE'

        ORDER BY m.timestamp DESC

        LIMIT ?
        """,
        (
            f"-{LIVE_WINDOW_MINUTES} minutes",
            DETAIL_LIMIT
        )
    )

    if live_data.empty:
        return pd.DataFrame(), pd.DataFrame()

    live_data = assign_city(live_data)

    live_data = live_data[
        live_data["City"].notna()
    ].copy()

    if live_data.empty:
        return pd.DataFrame(), pd.DataFrame()

    live_data["value"] = pd.to_numeric(
        live_data["value"],
        errors="coerce"
    )

    live_data["timestamp"] = pd.to_datetime(
        live_data["timestamp"],
        errors="coerce",
        utc=True
    )

    live_data = live_data.dropna(
        subset=["timestamp", "value"]
    )

    live_data = live_data[
        live_data["quality_status"].fillna("VALID") == "VALID"
    ]

    if live_data.empty:
        return pd.DataFrame(), pd.DataFrame()

    pivot = live_data.pivot_table(
        index=["City", "sensor_id", "timestamp"],
        columns="parameter",
        values="value",
        aggfunc="mean"
    ).reset_index()

    if "PM2.5" not in pivot.columns:
        pivot["PM2.5"] = pd.NA

    if "PM10" not in pivot.columns:
        pivot["PM10"] = pd.NA

    sensor_latest = (
        pivot.sort_values("timestamp")
        .groupby(["City", "sensor_id"], as_index=False)
        .tail(1)
    )

    results = []

    for city in CITY_BOUNDS:

        city_data = sensor_latest[
            sensor_latest["City"] == city
        ]

        if city_data.empty:

            results.append({
                "City": city,
                "Current Pollution Status": "No live data",
                "Last Updated": "No recent reading",
                "Sensors": 0
            })

            continue

        pm25 = city_data["PM2.5"].mean()
        pm10 = city_data["PM10"].mean()

        latest = city_data["timestamp"].max()

        results.append({
            "City": city,
            "Current Pollution Status": classify_city_pollution(
                pm25, pm10
            ),
            "Last Updated": latest.tz_convert(
                HISTORICAL_TIMEZONE
            ).strftime("%d %b %Y %H:%M:%S"),
            "Sensors": int(
                city_data["sensor_id"].nunique()
            )
        })

    return pd.DataFrame(results), live_data


# ============================================================
# PERFORMANCE FUNCTIONS
# ============================================================

def get_performance_summary():

    return load_data(
        """
        SELECT
            COUNT(*) AS total,
            AVG(processing_latency_ms) AS avg_latency,
            MAX(processing_latency_ms) AS max_latency,
            COALESCE(SUM(was_duplicate), 0) AS duplicates

        FROM performance_metrics
        """
    )


def get_rolling_throughput():

    records = load_data(
        """
        SELECT recorded_at

        FROM performance_metrics

        WHERE recorded_at IS NOT NULL

        ORDER BY id DESC

        LIMIT 20000
        """
    )

    if records.empty:

        return {
            "last_1_second": 0,
            "last_10_seconds": 0
        }

    timestamps = pd.to_datetime(
        records["recorded_at"],
        errors="coerce",
        utc=True
    ).dropna()

    now = pd.Timestamp.now(tz="UTC")

    age_seconds = (
        now - timestamps
    ).dt.total_seconds()

    recent_1 = int(
        (
            (age_seconds >= 0)
            &
            (age_seconds <= 1)
        ).sum()
    )

    recent_10 = int(
        (
            (age_seconds >= 0)
            &
            (age_seconds <= 10)
        ).sum()
    )

    return {
        "last_1_second": recent_1,
        "last_10_seconds": recent_10
    }


def get_alert_summary():

    return load_data(
        """
        SELECT
            COUNT(*) AS total_alerts,

            SUM(
                CASE
                    WHEN parameter = 'PM2.5'
                    THEN 1 ELSE 0
                END
            ) AS pm25_alerts,

            SUM(
                CASE
                    WHEN parameter = 'PM10'
                    THEN 1 ELSE 0
                END
            ) AS pm10_alerts

        FROM alerts
        """
    ).iloc[0]


# ============================================================
# LIVE AIR QUALITY PAGE
# ============================================================

def render_live_air_quality():

    st.header("Live Air Quality Monitoring")

    st.caption(
        "Real-time measurements from selected "
        "Sensor.Community sensors."
    )

    live_status, live_data = get_live_city_status()

    historical_status = get_last_historical_status()

    if live_status.empty:

        st.warning(
            "No recent valid live measurements available."
        )

        return

    city_summary = live_status.merge(
        historical_status,
        on="City",
        how="left"
    )

    st.subheader("Current City Pollution Status")

    display_columns = [
        "City",
        "Current Pollution Status",
        "Last Updated",
        "Last Low Pollution",
        "Last Normal Pollution",
        "Last High Pollution"
    ]

    for column in display_columns:

        if column not in city_summary.columns:
            city_summary[column] = "Not available"

    st.dataframe(
        city_summary[display_columns],
        width="stretch",
        hide_index=True
    )

    st.caption(
        "Historical dates are based on daily averages "
        "from the Sensor.Community archive."
    )

    st.divider()

    st.subheader("City Pollution Map")

    if not live_data.empty:

        map_data = live_data.copy()

        map_data["Status"] = map_data.apply(
            lambda row: classify_pollution(
                row["value"],
                row["parameter"]
            ),
            axis=1
        )

        map_data = map_data.dropna(
            subset=["latitude", "longitude"]
        )

        if not map_data.empty:

            fig = px.scatter_geo(
                map_data,
                lat="latitude",
                lon="longitude",
                color="Status",
                hover_name="City",
                hover_data=[
                    "sensor_id",
                    "parameter",
                    "value",
                    "timestamp"
                ],
                scope="europe",
                projection="mercator",
                height=550,
                title="Live Sensor Locations and Pollution Levels"
            )

            fig.update_geos(
                fitbounds="locations",
                visible=True,
                showcountries=True,
                showcoastlines=True
            )

            st.plotly_chart(
                fig,
                width="stretch"
            )

    st.divider()

    st.subheader("Latest Sensor Measurements")

    latest_measurements = (
        live_data.sort_values(
            "timestamp",
            ascending=False
        ).head(TABLE_LIMIT)
    )

    display_live = latest_measurements[
        [
            "City",
            "sensor_id",
            "timestamp",
            "parameter",
            "value",
            "sensor_type"
        ]
    ].copy()

    display_live["timestamp"] = (
        display_live["timestamp"]
        .dt.tz_convert(HISTORICAL_TIMEZONE)
        .dt.strftime("%d %b %Y %H:%M:%S")
    )

    display_live = display_live.rename(
        columns={
            "sensor_id": "Sensor ID",
            "timestamp": "Timestamp",
            "parameter": "Parameter",
            "value": "Value (µg/m³)",
            "sensor_type": "Sensor Type"
        }
    )

    st.dataframe(
        display_live,
        width="stretch",
        hide_index=True
    )


# ============================================================
# POLLUTION ALERTS PAGE
# ============================================================

def render_pollution_alerts():

    st.header("Pollution Alerts")

    st.subheader("⚠ AirWatch Alert Center")

    st.write(
        "Measurements exceeding configured project thresholds."
    )

    alert_summary = get_alert_summary()

    alerts = load_data(
        """
        SELECT
            a.event_id,
            a.sensor_id,
            a.timestamp,
            a.parameter,
            a.value,
            a.alert_type,
            a.message,
            m.latitude,
            m.longitude

        FROM alerts AS a

        LEFT JOIN measurements AS m
            ON a.event_id = m.event_id

        ORDER BY a.timestamp DESC

        LIMIT ?
        """,
        (TABLE_LIMIT,)
    )

    if not alerts.empty:

        alerts = assign_city(alerts)

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Total Alerts",
        f"{int(alert_summary['total_alerts'] or 0):,}"
    )

    col2.metric(
        "PM2.5 Alerts",
        f"{int(alert_summary['pm25_alerts'] or 0):,}"
    )

    col3.metric(
        "PM10 Alerts",
        f"{int(alert_summary['pm10_alerts'] or 0):,}"
    )

    st.divider()

    st.subheader("Latest Alert History")

    if alerts.empty:

        st.success(
            "No pollution alerts recorded."
        )

    else:

        for _, alert in alerts.iterrows():

            city = (
                alert["City"]
                if pd.notna(alert["City"])
                else "City not identified"
            )

            with st.container(border=True):

                st.subheader(
                    f"⚠ {alert['alert_type']}"
                )

                st.markdown(
                    f"**City:** {city}"
                )

                st.markdown(
                    f"**Sensor ID:** {alert['sensor_id']}"
                )

                st.markdown(
                    f"**Parameter:** {alert['parameter']}"
                )

                st.markdown(
                    f"**Value:** {alert['value']} µg/m³"
                )

                st.markdown(
                    f"**Time:** {alert['timestamp']}"
                )

                st.write(
                    str(alert["message"])
                )

    st.caption(
        f"Showing the latest {len(alerts):,} alerts."
    )


# ============================================================
# DATA OVERVIEW PAGE
# ============================================================

def render_data_overview():

    st.header("Data Overview – Germany")

    summary = load_data(
        """
        SELECT
            COUNT(*) AS total_measurements,
            COUNT(DISTINCT sensor_id) AS unique_sensors,

            AVG(
                CASE
                    WHEN parameter = 'PM2.5'
                    THEN value
                END
            ) AS avg_pm25,

            AVG(
                CASE
                    WHEN parameter = 'PM10'
                    THEN value
                END
            ) AS avg_pm10

        FROM measurements

        WHERE country = 'DE'
        """
    ).iloc[0]

    alert_summary = get_alert_summary()

    parameter_counts = load_data(
        """
        SELECT
            parameter,
            COUNT(*) AS count

        FROM measurements

        WHERE country = 'DE'

        GROUP BY parameter

        ORDER BY count DESC
        """
    )

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Total Measurements",
        f"{int(summary['total_measurements'] or 0):,}"
    )

    col2.metric(
        "Unique Sensors",
        f"{int(summary['unique_sensors'] or 0):,}"
    )

    col3.metric(
        "Average PM2.5",
        (
            f"{summary['avg_pm25']:.2f} µg/m³"
            if pd.notna(summary["avg_pm25"])
            else "No data"
        )
    )

    col4.metric(
        "Average PM10",
        (
            f"{summary['avg_pm10']:.2f} µg/m³"
            if pd.notna(summary["avg_pm10"])
            else "No data"
        )
    )

    st.divider()

    st.subheader("Measurements by Parameter")

    if not parameter_counts.empty:

        fig = px.bar(
            parameter_counts,
            x="parameter",
            y="count",
            color="parameter",
            labels={
                "parameter": "Parameter",
                "count": "Number of Measurements"
            },
            title="Measurement Distribution"
        )

        st.plotly_chart(
            fig,
            width="stretch"
        )

    st.divider()

    st.subheader("Pollution Alert Summary")

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Total Alerts",
        f"{int(alert_summary['total_alerts'] or 0):,}"
    )

    col2.metric(
        "PM2.5 Alerts",
        f"{int(alert_summary['pm25_alerts'] or 0):,}"
    )

    col3.metric(
        "PM10 Alerts",
        f"{int(alert_summary['pm10_alerts'] or 0):,}"
    )


# ============================================================
# DATA QUALITY PAGE
# ============================================================

def render_data_quality():

    st.header("Data Quality Monitoring")

    quality_summary = load_data(
        """
        SELECT
            status,
            COUNT(*) AS count

        FROM quality_checks

        GROUP BY status
        """
    )

    if quality_summary.empty:

        st.info(
            "No data quality checks recorded yet."
        )

        return

    col1, col2, col3 = st.columns(3)

    for status, column in [
        ("VALID", col1),
        ("SUSPICIOUS", col2),
        ("INVALID", col3)
    ]:

        count = quality_summary.loc[
            quality_summary["status"] == status,
            "count"
        ].sum()

        column.metric(
            f"{status.title()} Records",
            f"{int(count):,}"
        )

    st.divider()

    st.subheader("Quality Distribution")

    fig = px.pie(
        quality_summary,
        names="status",
        values="count",
        title="Data Quality Status Distribution"
    )

    st.plotly_chart(
        fig,
        width="stretch"
    )

    st.divider()

    st.subheader("Latest Quality Check Results")

    quality_details = load_data(
        """
        SELECT
            q.event_id,
            q.status,
            q.reason,
            q.checked_at,
            m.sensor_id,
            m.timestamp,
            m.parameter,
            m.value,
            m.sensor_type

        FROM quality_checks AS q

        LEFT JOIN measurements AS m
            ON q.event_id = m.event_id

        ORDER BY q.checked_at DESC

        LIMIT ?
        """,
        (TABLE_LIMIT,)
    )

    if quality_details.empty:

        st.info(
            "No detailed quality records available."
        )

    else:

        st.dataframe(
            quality_details[
                [
                    "sensor_id",
                    "timestamp",
                    "parameter",
                    "value",
                    "sensor_type",
                    "status",
                    "reason"
                ]
            ],
            width="stretch",
            hide_index=True
        )


# ============================================================
# STREAMING PERFORMANCE PAGE
# ============================================================

def render_streaming_performance():

    st.header("Kafka Streaming Performance")

    st.caption(
        "Application throughput, processing latency, "
        "live delivery latency, replay delay and Kafka consumer lag."
    )

    # --------------------------------------------------------
    # KAFKA LAG
    # --------------------------------------------------------

    try:

        from kafka_monitor import (
            initialize_lag_table,
            collect_consumer_lag,
            save_consumer_lag
        )

        initialize_lag_table()

        lag_rows = collect_consumer_lag()

        if lag_rows:

            save_consumer_lag(lag_rows)
            lag_error = None

        else:

            lag_error = (
                "No active consumer group offset data returned."
            )

    except Exception as error:

        lag_rows = []
        lag_error = str(error)

    # --------------------------------------------------------
    # PERFORMANCE DATA
    # --------------------------------------------------------

    summary = get_performance_summary().iloc[0]

    throughput = get_rolling_throughput()

    total_records = int(
        summary["total"] or 0
    )

    avg_latency = (
        summary["avg_latency"] or 0
    )

    max_latency = (
        summary["max_latency"] or 0
    )

    duplicates = int(
        summary["duplicates"] or 0
    )

    count_1s = throughput["last_1_second"]
    count_10s = throughput["last_10_seconds"]

    rate_1s = float(count_1s)
    rate_10s = count_10s / 10.0

    # --------------------------------------------------------
    # LIVE DELIVERY AND REPLAY LATENCY
    # --------------------------------------------------------

    delivery_data = load_data(
        """
        SELECT
            live_delivery_latency_ms,
            replay_delay_ms

        FROM performance_metrics

        ORDER BY id DESC

        LIMIT 5000
        """
    )

    live_delivery = pd.to_numeric(
        delivery_data["live_delivery_latency_ms"],
        errors="coerce"
    ).dropna()

    replay_delay = pd.to_numeric(
        delivery_data["replay_delay_ms"],
        errors="coerce"
    ).dropna()

    live_average = (
        live_delivery.mean()
        if not live_delivery.empty
        else None
    )

    replay_average = (
        replay_delay.mean()
        if not replay_delay.empty
        else None
    )

    # --------------------------------------------------------
    # PROCESSING LATENCY
    # --------------------------------------------------------

    recent_latency = load_data(
        """
        SELECT
            id,
            recorded_at,
            processing_latency_ms

        FROM performance_metrics

        ORDER BY id DESC

        LIMIT 500
        """
    )

    if not recent_latency.empty:

        recent_latency["processing_latency_ms"] = pd.to_numeric(
            recent_latency["processing_latency_ms"],
            errors="coerce"
        )

        p95_latency = (
            recent_latency[
                "processing_latency_ms"
            ].quantile(0.95)
        )

    else:

        p95_latency = 0

    total_lag = sum(
        int(row.get("lag", 0))
        for row in lag_rows
    )

    # --------------------------------------------------------
    # THROUGHPUT
    # --------------------------------------------------------

    st.subheader("Live Throughput")

    col1, col2 = st.columns(2)

    col1.metric(
        "Throughput - Last 1 Second",
        f"{rate_1s:.0f} msg/sec"
    )

    col2.metric(
        "Throughput - Last 10 Seconds",
        f"{rate_10s:.2f} msg/sec"
    )

    st.caption(
        "Throughput is calculated from performance records "
        "inserted by the AirWatch consumer. The 10-second "
        "average is more stable than the 1-second value."
    )

    st.divider()

    # --------------------------------------------------------
    # PROCESSING LATENCY
    # --------------------------------------------------------

    st.subheader("Processing Latency")

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Average Processing Latency",
        f"{avg_latency:.3f} ms"
    )

    col2.metric(
        "Maximum Processing Latency",
        f"{max_latency:.3f} ms"
    )

    col3.metric(
        "95th Percentile Latency",
        f"{p95_latency:.3f} ms"
    )

    st.divider()

    # --------------------------------------------------------
    # MESSAGE DELIVERY LATENCY
    # --------------------------------------------------------

    st.subheader("Message Delivery Latency")

    col1, col2 = st.columns(2)

    col1.metric(
        "Average Live Delivery Latency",
        (
            f"{live_average:.2f} ms"
            if live_average is not None
            else "No live data"
        )
    )

    col2.metric(
        "Average Replay Delay",
        (
            f"{replay_average / 1000:.2f} seconds"
            if replay_average is not None
            else "No replay data"
        )
    )

    st.caption(
        "Live delivery latency and replay delay are separate metrics."
    )

    st.divider()

    # --------------------------------------------------------
    # KAFKA CONSUMER LAG
    # --------------------------------------------------------

    st.subheader("Kafka Consumer Lag")

    if lag_error:
        st.warning(lag_error)

    col1, col2 = st.columns(2)

    col1.metric(
        "Total Consumer Lag",
        f"{total_lag:,} messages"
    )

    col2.metric(
        "Consumer Group",
        "airwatch-consumer-realtime"
    )

    if lag_rows:

        lag_dataframe = pd.DataFrame(lag_rows)

        st.dataframe(
            lag_dataframe,
            width="stretch",
            hide_index=True
        )

    st.divider()

    # --------------------------------------------------------
    # PERFORMANCE SUMMARY
    # --------------------------------------------------------

    st.subheader("Performance Summary")

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Total Performance Records",
        f"{total_records:,}"
    )

    col2.metric(
        "Duplicate Records",
        f"{duplicates:,}"
    )

    col3.metric(
        "Average Processing Latency",
        f"{avg_latency:.3f} ms"
    )

    st.divider()

    # --------------------------------------------------------
    # PROCESSING LATENCY CHART
    # --------------------------------------------------------

    st.subheader("Recent Processing Latency")

    if not recent_latency.empty:

        recent_latency["recorded_at"] = pd.to_datetime(
            recent_latency["recorded_at"],
            errors="coerce",
            utc=True
        )

        recent_latency = recent_latency.dropna(
            subset=[
                "recorded_at",
                "processing_latency_ms"
            ]
        )

        recent_latency = recent_latency.sort_values(
            "recorded_at"
        )

        if not recent_latency.empty:

            fig = px.line(
                recent_latency,
                x="recorded_at",
                y="processing_latency_ms",
                title="Processing Latency Over Time",
                labels={
                    "recorded_at": "Recorded At",
                    "processing_latency_ms": "Latency (ms)"
                }
            )

            st.plotly_chart(
                fig,
                width="stretch"
            )

    else:

        st.info(
            "No processing latency records available."
        )


# ============================================================
# MAIN NAVIGATION
# ============================================================

def main():

    st.sidebar.title("🌍 AirWatch")

    st.sidebar.markdown("### Navigation")

    page = st.sidebar.radio(
        "Select a page",
        [
            "Live Air Quality",
            "Pollution Alerts",
            "Data Overview",
            "Data Quality",
            "Streaming Performance"
        ]
    )

    st.sidebar.divider()

    st.sidebar.caption(
        "Real-world IoT air quality monitoring"
    )

    st.sidebar.caption(
        "Dashboard refreshes every 5 seconds"
    )

    st.title("🌍 AirWatch")

    st.caption(
        "Real-Time Air Quality Monitoring "
        "and Anomaly Detection"
    )

    if page == "Live Air Quality":

        render_live_air_quality()

    elif page == "Pollution Alerts":

        render_pollution_alerts()

    elif page == "Data Overview":

        render_data_overview()

    elif page == "Data Quality":

        render_data_quality()

    elif page == "Streaming Performance":

        render_streaming_performance()


# ============================================================
# AUTOMATIC REFRESH
# ============================================================

@st.fragment(run_every="5s")
def dashboard_fragment():

    main()


dashboard_fragment()
