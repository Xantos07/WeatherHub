import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv(os.path.join(os.path.dirname(__file__), '../.env'))

# Configuration AWS
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_BUCKET = os.getenv('S3_BUCKET')

# Configuration MongoDB
MONGO_INITDB_ROOT_USERNAME = os.getenv('MONGO_INITDB_ROOT_USERNAME')
MONGO_INITDB_ROOT_PASSWORD = os.getenv('MONGO_INITDB_ROOT_PASSWORD')
MONGO_HOST = os.getenv('MONGO_HOST', '172.31.0.23')

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

# Mapping des champs météo
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
