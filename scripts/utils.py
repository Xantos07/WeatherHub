import re
import pandas as pd
from datetime import datetime

def extract_value_unit(val, default_unit=None):
    """Extrait la valeur et l'unité d'une chaîne de caractères."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None, default_unit, None
    if isinstance(val, (int, float)):
        return float(val), default_unit, str(val)
    if isinstance(val, str):
        original = val
        match = re.search(r'([-+]?\d*\.?\d+)', val.replace(',', '.'))
        value = float(match.group(1)) if match else None
        unit_match = re.search(r'([a-zA-Z%°/]+)', val.replace(',', '.'))
        unit = unit_match.group(1) if unit_match else default_unit
        return value, unit, original
    return None, default_unit, str(val)

def f_to_c(f):
    """Convertit Fahrenheit en Celsius."""
    return (f - 32) * 5.0/9.0

def inhg_to_hpa(inhg):
    """Convertit pouces de mercure en hectopascals."""
    return inhg * 33.8639

def mph_to_kmh(mph):
    """Convertit miles par heure en kilomètres par heure."""
    return mph * 1.60934

def in_to_mm(inches):
    """Convertit pouces en millimètres."""
    return inches * 25.4

def build_weather_doc(record, s3_key, row_index, hour_index=None, station_id=None):
    """Construit un document météorologique pour MongoDB."""
    doc = {
        "station_id": station_id,
        "dh_utc": record.get('dh_utc') or record.get('Time'),
        "measurements": {
            "temperature": record.get("Temperature"),
            "dew_point": record.get("Dew Point"),
            "humidity": record.get("Humidity"),
            "wind": {
                "speed": record.get("Speed", {}).get("value"),
                "speed_unit": record.get("Speed", {}).get("unit"),
                "gust": record.get("Gust", {}).get("value"),
                "gust_unit": record.get("Gust", {}).get("unit"),
                "direction": record.get("vent_direction", {}).get("value"),
                "direction_original": record.get("vent_direction_original")
            },
            "pressure": record.get("Pressure"),
            "precipitation": {
                "rate": record.get("Precip. Rate.", {}).get("value"),
                "accumulation": record.get("Precip. Accum.", {}).get("value"),
                "unit": record.get("Precip. Rate.", {}).get("unit") or record.get("Precip. Accum.", {}).get("unit")
            },
            "solar_radiation": record.get("Solar"),
            "uv_index": record.get("UV", {}).get("value")
        },
        "metadata": {
            "source_file": s3_key,
            "row_index": row_index,
            "hour_index": hour_index,
            "created_at": datetime.now()
        }
    }
    return doc
