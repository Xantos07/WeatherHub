from datetime import datetime
from config import WEATHER_STATIONS
from database import db_connector

class StationManager:
    """Gestionnaire des stations météorologiques."""
    
    def __init__(self):
        self.db = db_connector.get_database()
    
    def create_weather_station(self, station_type, s3_key):
        """Crée une station WeatherFR ou WeatherBE dans la base de données."""
        if station_type not in WEATHER_STATIONS:
            return None
        
        station_data = WEATHER_STATIONS[station_type].copy()
        station_data['source_file'] = s3_key
        station_data['created_at'] = datetime.now()
        
        # Vérifier si la station existe déjà
        existing_station = self.db['stations'].find_one({'id': station_data['id']})
        if not existing_station:
            self.db['stations'].insert_one(station_data)
            print(f"+ Station {station_data['name']} ({station_data['id']}) créée")
            return station_data['id']
        else:
            print(f"-> Station {station_data['name']} ({station_data['id']}) existe déjà")
            return existing_station['id']
    
    def get_first_date_for_station(self, station_id):
        """Récupère la première date disponible pour une station."""
        doc = self.db['weather'].find_one(
            {"station_id": station_id}, 
            sort=[("dh_utc", 1)]
        )
        if doc and "dh_utc" in doc:
            # Extraire juste la date (YYYY-MM-DD)
            return str(doc["dh_utc"])[:10]
        return None
