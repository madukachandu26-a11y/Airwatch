
PM25_THRESHOLD = 25.0
PM10_THRESHOLD = 50.0


def detect_anomaly(event):

    parameter = event["parameter"]
    value = float(event["value"])

    if parameter == "PM2.5" and value > PM25_THRESHOLD:
        return {
            "is_anomaly": True,
            "alert": "HIGH PM2.5",
            "message": f"PM2.5 is {value} µg/m³"
        }

    if parameter == "PM10" and value > PM10_THRESHOLD:
        return {
            "is_anomaly": True,
            "alert": "HIGH PM10",
            "message": f"PM10 is {value} µg/m³"
        }

    return {
        "is_anomaly": False,
        "alert": "NORMAL",
        "message": "Measurement within configured threshold"
    }
