import os
import re
import boto3
import pandas as pd
import json
from pymongo import MongoClient
from io import BytesIO
import time
from datetime import datetime
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv(os.path.join(os.path.dirname(__file__), '../.env'))

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_BUCKET = os.getenv('S3_BUCKET')
MONGO_INITDB_ROOT_USERNAME = os.getenv('MONGO_INITDB_ROOT_USERNAME')
MONGO_INITDB_ROOT_PASSWORD = os.getenv('MONGO_INITDB_ROOT_PASSWORD')
MONGO_HOST = os.getenv('MONGO_HOST', '172.31.0.23')

# Connexions
s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

# ECR production
# mongo_client = MongoClient(f"mongodb://{MONGO_INITDB_ROOT_USERNAME}:{MONGO_INITDB_ROOT_PASSWORD}@{MONGO_HOST}:27017/")

# Pour le développement local, décommenter la ligne suivante
mongo_client = MongoClient(f"mongodb://{MONGO_INITDB_ROOT_USERNAME}:{MONGO_INITDB_ROOT_PASSWORD}@mongodb:27017/")
db = mongo_client['weatherhub']

# Données des stations WeatherFR et WeatherBE
WEATHER_STATIONS = {
    'WeatherFR': {
        'id': 'ILAMAD25',
        'name': 'La Madeleine',
        'latitude': 50.659,
        'longitude': 3.07,
        'elevation': 23,
        'city': 'La Madeleine',
        'state': '-/-',
        'hardware': 'other',
        'software': 'EasyWeatherPro_V5.1.6',
        'type': 'weather_underground',
        'license': {
            'source': 'Weather Underground'
        }
    },
    'WeatherBE': {
        'id': 'IICHTE19',
        'name': 'WeerstationBS',
        'latitude': 51.092,
        'longitude': 2.999,
        'elevation': 15,
        'city': 'Ichtegem',
        'state': '-/-',
        'hardware': 'other',
        'software': 'EasyWeatherV1.6.6',
        'type': 'weather_underground',
        'license': {
            'source': 'Weather Underground'
        }
    }
}

'''
    "metadata pour toutes les valeurs meme Weather BE et FR": {
        "temperature": "temperature,degC",
        "pression": "mean sea level pressure,hPa",
        "humidite": "relative humidity,%",
        "point_de_rosee": "dewpoint,degC",
        "visibilite": "horizontal visibility,m",
        "vent_moyen": "mean wind speed,km\/h",
        "vent_rafales": "wind gust,km\/h",
        "vent_direction": "wind direction,deg",
        "pluie_3h": "precipitation over 3h,mm",
        "pluie_1h": "precipitation over 1h,mm",
        "neige_au_sol": "snow depth,cm",
        "nebulosite": "Ncloud cover,octats",
        "temps_omm": "present weather,http:\/\/www.infoclimat.fr\/stations-meteo\/ww.php"
    },

    "metadata pour les valeurs Weather BE et FR": {
   
    Time	 3:54 PM	
    Temperature	60.3 °F	
    Dew Point	53.5 °F	
    Humidity	78 %								
    Wind	WSW
    Speed	7.8 mph
    Gust	9.6 mph
    Pressure	29.49 in
    Precip. Rate.	0.00 in
    Precip. Accum.	0.07 in
    UV		2
    Solar 273.9 w/m²
'''


def extract_value_unit(val, default_unit=None):
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

def get_tuple(record, key, default_unit=None):
    v = record.get(key)
    if isinstance(v, tuple):
        return v
    return extract_value_unit(v, default_unit=default_unit)

def build_weather_doc(record, s3_key, row_index, hour_index=None, station_id=None):
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


HOURLY_TO_BE_FR = {
    "temperature": "Temperature",
    "point_de_rosee": "Dew Point",
    "humidite": "Humidity",
    "pression": "Pressure",
    "vent_moyen": "Speed",
    "vent_rafales": "Gust",
    "vent_direction": "Wind",
    "visibilite": "Visibility",
    "pluie_1h": "Precip. Rate.",
    "pluie_3h": "Precip. Accum.",
    "UV": "UV",
    "Solar": "Solar",
    "nebulosite": "Cloud Cover",
    "temps_omm": "Weather Code",
    "dh_utc": "Time",
    "station_id": "station_id"
}

def f_to_c(f):
    return (f - 32) * 5.0/9.0

def inhg_to_hpa(inhg):
    return inhg * 33.8639

def mph_to_kmh(mph):
    return mph * 1.60934

def in_to_mm(inches):
    return inches * 25.4


def normalize_be_fr_record(record):
    norm = {}
    for k, v in record.items():
        key = HOURLY_TO_BE_FR.get(k, k)
        value, unit, original = extract_value_unit(v)
        # Conversion des unités
        if key == "Temperature" and unit == "°F":
            original = v
            value = f_to_c(value)
            unit = "degC"
        if key == "Dew Point" and unit == "°F":
            original = v
            value = f_to_c(value)
            unit = "degC"
        if key == "Pressure" and unit == "in":
            original = v
            value = inhg_to_hpa(value)
            unit = "hPa"
        if key in ["Speed", "Gust"] and unit == "mph":
            original = v
            value = mph_to_kmh(value)
            unit = "km/h"
        if key in ["Precip. Rate.", "Precip. Accum."] and unit == "in":
            original = v
            value = in_to_mm(value)
            unit = "mm"
        norm[key] = {
            "value": value,
            "unit": unit,
            "original": original
        }
    # Pour les champs sans unité (ex: UV, Solar, Wind direction)
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
    # Ajouter station_id s'il est fourni
    if "station_id" in record:
        norm["station_id"] = record["station_id"]
    return norm

def normalize_hourly_record(record):
    norm = {}
    for k, v in record.items():
        key = HOURLY_TO_BE_FR.get(k, k)
        value, unit, original = extract_value_unit(v)

        if key == "Temperature":
            unit = unit or "degC"
        if key == "Dew Point":
            unit = unit or "degC"
        if key == "Pressure":
            unit = unit or "hPa"
        if key in ["Speed", "Gust"]:
            unit = unit or "km/h"
        if key in ["Precip. Rate.", "Precip. Accum."]:
            unit = unit or "mm"
        if key == "Humidity":
            unit = unit or "%"
        if key == "Solar":
            unit = unit or "w/m²"
        norm[key] = {
            "value": value,
            "unit": unit,
            "original": original
        }
    # Pour les champs sans unité (ex: UV, Wind direction)
    if "UV" in record:
        norm["UV"] = {"value": extract_value_unit(record["UV"])[0]}
    if "Solar" in record:
        norm["Solar"] = {"value": extract_value_unit(record["Solar"])[0], "unit": "w/m²"}
    if "vent_direction" in record:
        norm["vent_direction_original"] = record["vent_direction"]
    if "dh_utc" in record:
        norm["dh_utc"] = record["dh_utc"]
    return norm

def create_weather_station(station_type, s3_key):
    """Crée une station WeatherFR ou WeatherBE dans la base de données."""
    if station_type not in WEATHER_STATIONS:
        return None
    
    station_data = WEATHER_STATIONS[station_type].copy()
    station_data['source_file'] = s3_key
    station_data['created_at'] = datetime.now()
    
    # Vérifier si la station existe déjà
    existing_station = db['stations'].find_one({'id': station_data['id']})
    if not existing_station:
        db['stations'].insert_one(station_data)
        print(f"  ➕ Station {station_data['name']} ({station_data['id']}) créée")
        return station_data['id']
    else:
        print(f"  ℹ️  Station {station_data['name']} ({station_data['id']}) existe déjà")
        return existing_station['id']

def import_csv_to_mongo(s3_key):

    # Lire le fichier CSV depuis S3
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
    df = pd.read_csv(BytesIO(obj['Body'].read()))

    if '_airbyte_data' in df.columns:
        if 'StationsMeteorologiques' in s3_key:
                # Importer les stations météorologiques
                stations_count = 0
                weather_count = 0
                
                for index, row in df.iterrows():
                    try:
                        data = json.loads(row['_airbyte_data'])
                        if 'hourly' in data:
                            print(f"Ligne {index} : hourly trouvé avec {len(data['hourly'])} stations")
                        else:
                            print(f"Ligne {index} : pas de clé 'hourly'")
                        # Import stations
                        if 'stations' in data:
                            for station in data['stations']:
                                station['source_file'] = s3_key
                                station['created_at'] = datetime.now()
                                db['stations'].insert_one(station)
                                stations_count += 1
                        
                        # Import données hourly directement
                        if 'hourly' in data and isinstance(data['hourly'], dict):
                            for station_id, records in data['hourly'].items():
                                for hour_index, record in enumerate(records):
                                    norm_record = normalize_hourly_record(record)
                                    doc = build_weather_doc(norm_record, s3_key, row_index=None, hour_index=hour_index, station_id=station_id)
                                    db['weather'].insert_one(doc)
                                    weather_count += 1

                    except Exception as e:
                        print(f"    ❌ Erreur lors du traitement de la ligne {index}: {e}")
                
                print(f"  ✅ {stations_count} stations, {weather_count} données weather")

     # WEATHER BE/FR
        elif 'WeatherBE' in s3_key or 'WeatherFR' in s3_key:
            # Déterminer le type de station
            station_type = 'WeatherBE' if 'WeatherBE' in s3_key else 'WeatherFR'
            
            # Créer la station correspondante
            station_id = create_weather_station(station_type, s3_key)
            
            weather_count = 0 
            for index, row in df.iterrows():
                try:
                    data = json.loads(row['_airbyte_data'])
                    norm_record = normalize_be_fr_record(data)
                    # Ajouter l'ID de station aux données normalisées
                    norm_record['station_id'] = station_id
                    doc = build_weather_doc(norm_record, s3_key, row_index=index, station_id=station_id)
                    db['weather'].insert_one(doc)
                    weather_count += 1
                except Exception as e:
                    print(f"    ❌ Erreur ligne {index}: {e}")
            
            print(f"  ✅ Station {station_type} créée avec {weather_count} données weather")

def measure_data_quality():
    """Mesure la qualité des données après migration."""
    print("\n____MESURE DE QUALITÉ DES DONNÉES____")
    print("=" * 50)
    
    # Comptage général
    total_stations = db['stations'].count_documents({})
    total_weather = db['weather'].count_documents({})
    print(f"=> Total stations : {total_stations}")
    print(f"=> Total mesures météo : {total_weather}")
    
    # Répartition des stations par type/source
    print(f"\n____RÉPARTITION DES STATIONS :____")
    station_types = db['stations'].aggregate([
        {"$group": {"_id": "$type", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ])
    for station_type in station_types:
        type_name = station_type['_id'] if station_type['_id'] else "Unknown"
        print(f"Type {type_name} : {station_type['count']} stations")
    
    # Lister les stations Weather Underground
    weather_stations = list(db['stations'].find({"type": "weather_underground"}, {"id": 1, "name": 1, "city": 1}))
    if weather_stations:
        print(f"\n____STATIONS WEATHER UNDERGROUND :____")
        for ws in weather_stations:
            print(f"{ws.get('name', 'Unknown')} ({ws.get('id', 'Unknown')}) - {ws.get('city', 'Unknown')}")
    
    print(f"=> Total stations : {total_stations}")
    print(f"=>  Total mesures météo : {total_weather}")
    print(f"=> Total stations : {total_stations}")
    print(f"=>  Total mesures météo : {total_weather}")
    
    # Mesures de qualité pour les stations
    stations_without_id = db['stations'].count_documents({"id": {"$in": [None, ""]}})
    stations_without_name = db['stations'].count_documents({"name": {"$in": [None, ""]}})
    stations_without_coords = db['stations'].count_documents({
        "$or": [
            {"latitude": {"$in": [None, ""]}},
            {"longitude": {"$in": [None, ""]}}
        ]
    })
    
    print(f"\n____QUALITÉ DES STATIONS :____")
    print(f"  => Sans ID : {stations_without_id} ({stations_without_id/total_stations*100:.1f}%)")
    print(f"  => Sans nom : {stations_without_name} ({stations_without_name/total_stations*100:.1f}%)")
    print(f"  => Sans coordonnées : {stations_without_coords} ({stations_without_coords/total_stations*100:.1f}%)")
    
    # Mesures de qualité pour les données météo
    weather_without_station = db['weather'].count_documents({"station_id": {"$in": [None, ""]}})
    weather_without_date = db['weather'].count_documents({"dh_utc": {"$in": [None, ""]}})
    weather_without_temp = db['weather'].count_documents({"measurements.temperature.value": {"$in": [None, ""]}})
    weather_without_pressure = db['weather'].count_documents({"measurements.pressure.value": {"$in": [None, ""]}})
    weather_without_humidity = db['weather'].count_documents({"measurements.humidity.value": {"$in": [None, ""]}})
    
    print(f"\n____QUALITÉ DES DONNÉES MÉTÉO :____")
    print(f"  => Sans station_id : {weather_without_station} ({weather_without_station/total_weather*100:.1f}%)")
    print(f"  => Sans date : {weather_without_date} ({weather_without_date/total_weather*100:.1f}%)")
    print(f"  => Sans température : {weather_without_temp} ({weather_without_temp/total_weather*100:.1f}%)")
    print(f"  => Sans pression : {weather_without_pressure} ({weather_without_pressure/total_weather*100:.1f}%)")
    print(f"  => Sans humidité : {weather_without_humidity} ({weather_without_humidity/total_weather*100:.1f}%)")
    
    # Répartition par source
    print(f"\n📁 RÉPARTITION PAR SOURCE :")
    sources = db['weather'].aggregate([
        {"$group": {"_id": "$metadata.source_file", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ])
    for source in sources:
        file_name = source['_id'].split('/')[-1] if source['_id'] else "Unknown"
        print(f"  📄 {file_name} : {source['count']} mesures")
    
    # Calcul du taux d'erreur global
    total_fields_checked = total_weather * 5  # 5 champs critiques
    total_errors = weather_without_station + weather_without_date + weather_without_temp + weather_without_pressure + weather_without_humidity
    error_rate = (total_errors / total_fields_checked) * 100
    
    print(f"\n📈 TAUX D'ERREUR GLOBAL : {error_rate:.2f}%")
    
    if error_rate < 5:
        print("✅ Excellente qualité des données")
    elif error_rate < 15:
        print("⚠️  Qualité des données acceptable")
    else:
        print("❌ Qualité des données à améliorer")
    
    return {
        "total_stations": total_stations,
        "total_weather": total_weather,
        "error_rate": error_rate,
        "stations_errors": {
            "without_id": stations_without_id,
            "without_name": stations_without_name,
            "without_coords": stations_without_coords
        },
        "weather_errors": {
            "without_station": weather_without_station,
            "without_date": weather_without_date,
            "without_temp": weather_without_temp,
            "without_pressure": weather_without_pressure,
            "without_humidity": weather_without_humidity
        }
    }

def benchmark_weather_query(station_id, date_str):
    """Mesure le temps d'accès aux données météo pour une station et une date."""
    start = time.time()
    result = list(db['weather'].find({
        "station_id": station_id,
        "dh_utc": {"$regex": date_str}
    }))
    duration = (time.time() - start) * 1000  # ms
    print(f"- Temps de requête pour {station_id} le {date_str} : {duration:.2f} ms ({len(result)} résultats)")
    return duration

def get_first_date_for_station(station_id):

    doc = db['weather'].find_one({"station_id": station_id}, sort=[("dh_utc", 1)])
    if doc and "dh_utc" in doc:
        # Extraire juste la date (YYYY-MM-DD)
        return str(doc["dh_utc"])[:10]
    return None

def main():
    # Vider les collections
    db['stations'].delete_many({})
    db['weather'].delete_many({})
    print("Collections vidées")
    
    # Importer tous les CSV
    files = s3_client.list_objects_v2(Bucket=S3_BUCKET)
    if 'Contents' in files:
        csv_files = [obj['Key'] for obj in files['Contents'] if obj['Key'].endswith('.csv')]
        
        for csv_file in csv_files:
            print(f"Importation de {csv_file}...")
            import_csv_to_mongo(csv_file)
    
    print("\n---- Import terminé! -----")
    
    # Mesure de qualité des données
    quality_metrics = measure_data_quality()

def get_station_with_most_precipitation():
    """Trouve la station avec le plus de précipitations totales."""
    pipeline = [
        {"$group": {
            "_id": "$station_id",
            "total_precip": {"$sum": {"$ifNull": ["$measurements.precipitation.accumulation", 0]}}
        }},
        {"$sort": {"total_precip": -1}},
        {"$limit": 1}
    ]

    start = time.time()
    result = list(db['weather'].aggregate(pipeline))
    duration = (time.time() - start) * 1000  # ms
    if result:
        station_id = result[0]['_id']
        total_precip = result[0]['total_precip']
        station = db['stations'].find_one({'id': station_id})
        station_name = station['name'] if station and 'name' in station else station_id
        print(f"\nStation avec le plus de précipitations : {station_name} ({station_id})")
        print(f"=> Précipitations totales : {total_precip:.2f} mm")
        print(f"- Temps de requête : {duration:.2f} ms")
    else:
        print("Aucune donnée de précipitation trouvée.")

if __name__ == "__main__":
    main()
    get_station_with_most_precipitation()