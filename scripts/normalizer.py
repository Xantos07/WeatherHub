from config import HOURLY_TO_BE_FR
from utils import extract_value_unit, f_to_c, inhg_to_hpa, mph_to_kmh, in_to_mm

class WeatherDataNormalizer:
    """Classe pour normaliser les données météorologiques."""
    
    @staticmethod
    def normalize_be_fr_record(record):
        """Normalise un enregistrement WeatherBE/WeatherFR."""
        norm = {}
        for k, v in record.items():
            key = HOURLY_TO_BE_FR.get(k, k)
            value, unit, original = extract_value_unit(v)
            
            # Conversion des unités
            if key == "Temperature" and unit == "°F":
                original = v
                value = f_to_c(value)
                unit = "degC"
            elif key == "Dew Point" and unit == "°F":
                original = v
                value = f_to_c(value)
                unit = "degC"
            elif key == "Pressure" and unit == "in":
                original = v
                value = inhg_to_hpa(value)
                unit = "hPa"
            elif key in ["Speed", "Gust"] and unit == "mph":
                original = v
                value = mph_to_kmh(value)
                unit = "km/h"
            elif key in ["Precip. Rate.", "Precip. Accum."] and unit == "in":
                original = v
                value = in_to_mm(value)
                unit = "mm"
            
            norm[key] = {
                "value": value,
                "unit": unit,
                "original": original
            }
        
        # Traitement des champs spéciaux
        if "UV" in record:
            norm["UV"] = {"value": extract_value_unit(record["UV"])[0]}
        if "Solar" in record:
            norm["Solar"] = {"value": extract_value_unit(record["Solar"])[0], "unit": "w/m²"}
        if "Wind" in record:
            wind_val = record.get("Wind")
            if wind_val is not None and isinstance(wind_val, str) and not wind_val.replace('.', '', 1).isdigit():
                norm["vent_direction_original"] = wind_val
        if "Time" in record:
            norm["dh_utc"] = record["Time"]
        if "station_id" in record:
            norm["station_id"] = record["station_id"]
        
        return norm
    
    @staticmethod
    def normalize_hourly_record(record):
        """Normalise un enregistrement horaire standard."""
        norm = {}
        for k, v in record.items():
            key = HOURLY_TO_BE_FR.get(k, k)
            value, unit, original = extract_value_unit(v)
            
            # Unités par défaut
            unit_defaults = {
                "Temperature": "degC",
                "Dew Point": "degC",
                "Pressure": "hPa",
                "Speed": "km/h",
                "Gust": "km/h",
                "Precip. Rate.": "mm",
                "Precip. Accum.": "mm",
                "Humidity": "%",
                "Solar": "w/m²"
            }
            
            if key in unit_defaults:
                unit = unit or unit_defaults[key]
            
            norm[key] = {
                "value": value,
                "unit": unit,
                "original": original
            }
        
        # Traitement des champs spéciaux
        if "UV" in record:
            norm["UV"] = {"value": extract_value_unit(record["UV"])[0]}
        if "Solar" in record:
            norm["Solar"] = {"value": extract_value_unit(record["Solar"])[0], "unit": "w/m²"}
        if "vent_direction" in record:
            norm["vent_direction_original"] = record["vent_direction"]
        if "dh_utc" in record:
            norm["dh_utc"] = record["dh_utc"]
        
        return norm
