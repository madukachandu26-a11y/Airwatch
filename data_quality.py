
def validate_measurement(event):

    parameter = event.get("parameter")
    value = event.get("value")

    try:
        value = float(value)
    except (ValueError, TypeError):
        return {
            "valid": False,
            "status": "INVALID",
            "reason": "Non-numeric measurement"
        }

    if value < 0:
        return {
            "valid": False,
            "status": "INVALID",
            "reason": "Negative measurement"
        }

    limits = {
        "PM2.5": 500,
        "PM10": 500
    }

    limit = limits.get(parameter)

    if limit is not None and value > limit:
        return {
            "valid": False,
            "status": "SUSPICIOUS",
            "reason": f"{parameter} exceeds project validation limit"
        }

    return {
        "valid": True,
        "status": "VALID",
        "reason": "Measurement passed validation"
    }
