import time
from database import db_connector

class DataQualityAnalyzer:
    """Analyseur de qualité des données météorologiques."""
    
    def __init__(self):
        self.db = db_connector.get_database()
    
    def measure_data_quality(self):
        """Mesure la qualité des données après migration."""
        print("\n____MESURE DE QUALITÉ DES DONNÉES____")
        print("=" * 50)
        
        # Comptage général
        total_stations = self.db['stations'].count_documents({})
        total_weather = self.db['weather'].count_documents({})
        print(f"=> Total stations : {total_stations}")
        print(f"=> Total mesures météo : {total_weather}")
        
        # Répartition des stations par type/source
        self._analyze_station_types()
        
        # Lister les stations Weather Underground
        self._list_weather_underground_stations()
        
        # Qualité des stations
        self._analyze_station_quality(total_stations)
        
        # Qualité des données météo
        self._analyze_weather_quality(total_weather)
        
        # Répartition par source
        self._analyze_sources()
        
        # Calcul du taux d'erreur global
        error_rate = self._calculate_error_rate(total_weather)
        
        return {
            "total_stations": total_stations,
            "total_weather": total_weather,
            "error_rate": error_rate
        }
    
    def _analyze_station_types(self):
        """Analyse les types de stations."""
        print(f"\n____RÉPARTITION DES STATIONS :____")
        station_types = self.db['stations'].aggregate([
            {"$group": {"_id": "$type", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ])
        for station_type in station_types:
            type_name = station_type['_id'] if station_type['_id'] else "Unknown"
            print(f"Type {type_name} : {station_type['count']} stations")
    
    def _list_weather_underground_stations(self):
        """Liste les stations Weather Underground."""
        weather_stations = list(self.db['stations'].find(
            {"type": "weather_underground"}, 
            {"id": 1, "name": 1, "city": 1}
        ))
        if weather_stations:
            print(f"\n____STATIONS WEATHER UNDERGROUND :____")
            for ws in weather_stations:
                print(f"{ws.get('name', 'Unknown')} ({ws.get('id', 'Unknown')}) - {ws.get('city', 'Unknown')}")
    
    def _analyze_station_quality(self, total_stations):
        """Analyse la qualité des données de stations."""
        stations_without_id = self.db['stations'].count_documents({"id": {"$in": [None, ""]}})
        stations_without_name = self.db['stations'].count_documents({"name": {"$in": [None, ""]}})
        stations_without_coords = self.db['stations'].count_documents({
            "$or": [
                {"latitude": {"$in": [None, ""]}},
                {"longitude": {"$in": [None, ""]}}
            ]
        })
        
        print(f"\n____QUALITÉ DES STATIONS :____")
        if total_stations > 0:
            print(f"  => Sans ID : {stations_without_id} ({stations_without_id/total_stations*100:.1f}%)")
            print(f"  => Sans nom : {stations_without_name} ({stations_without_name/total_stations*100:.1f}%)")
            print(f"  => Sans coordonnées : {stations_without_coords} ({stations_without_coords/total_stations*100:.1f}%)")
    
    def _analyze_weather_quality(self, total_weather):
        """Analyse la qualité des données météorologiques."""
        weather_without_station = self.db['weather'].count_documents({"station_id": {"$in": [None, ""]}})
        weather_without_date = self.db['weather'].count_documents({"dh_utc": {"$in": [None, ""]}})
        weather_without_temp = self.db['weather'].count_documents({"measurements.temperature.value": {"$in": [None, ""]}})
        weather_without_pressure = self.db['weather'].count_documents({"measurements.pressure.value": {"$in": [None, ""]}})
        weather_without_humidity = self.db['weather'].count_documents({"measurements.humidity.value": {"$in": [None, ""]}})
        
        print(f"\n____QUALITÉ DES DONNÉES MÉTÉO :____")
        if total_weather > 0:
            print(f"  => Sans station_id : {weather_without_station} ({weather_without_station/total_weather*100:.1f}%)")
            print(f"  => Sans date : {weather_without_date} ({weather_without_date/total_weather*100:.1f}%)")
            print(f"  => Sans température : {weather_without_temp} ({weather_without_temp/total_weather*100:.1f}%)")
            print(f"  => Sans pression : {weather_without_pressure} ({weather_without_pressure/total_weather*100:.1f}%)")
            print(f"  => Sans humidité : {weather_without_humidity} ({weather_without_humidity/total_weather*100:.1f}%)")
    
    def _analyze_sources(self):
        """Analyse la répartition par source."""
        print(f"\n____RÉPARTITION PAR SOURCE :____")
        sources = self.db['weather'].aggregate([
            {"$group": {"_id": "$metadata.source_file", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ])
        for source in sources:
            file_name = source['_id'].split('/')[-1] if source['_id'] else "Unknown"
            print(f"  -> {file_name} : {source['count']} mesures")
    
    def _calculate_error_rate(self, total_weather):
        """Calcule le taux d'erreur global."""
        if total_weather == 0:
            return 0
        
        weather_without_station = self.db['weather'].count_documents({"station_id": {"$in": [None, ""]}})
        weather_without_date = self.db['weather'].count_documents({"dh_utc": {"$in": [None, ""]}})
        weather_without_temp = self.db['weather'].count_documents({"measurements.temperature.value": {"$in": [None, ""]}})
        weather_without_pressure = self.db['weather'].count_documents({"measurements.pressure.value": {"$in": [None, ""]}})
        weather_without_humidity = self.db['weather'].count_documents({"measurements.humidity.value": {"$in": [None, ""]}})
        
        total_fields_checked = total_weather * 5  # 5 champs critiques
        total_errors = weather_without_station + weather_without_date + weather_without_temp + weather_without_pressure + weather_without_humidity
        error_rate = (total_errors / total_fields_checked) * 100
        
        print(f"\n____AUX D'ERREUR GLOBAL : {error_rate:.2f}%____")
        
        if error_rate < 5:
            print("---Excellente qualité des données---")
        elif error_rate < 15:
            print("---Qualité des données acceptable---")
        else:
            print("---Qualité des données à améliorer---")
        
        return error_rate
    
    def benchmark_weather_query(self, station_id, date_str):
        """Mesure le temps d'accès aux données météo pour une station et une date."""
        start = time.time()
        result = list(self.db['weather'].find({
            "station_id": station_id,
            "dh_utc": {"$regex": date_str}
        }))
        duration = (time.time() - start) * 1000  # ms
        print(f"- Temps de requête pour {station_id} le {date_str} : {duration:.2f} ms ({len(result)} résultats)")
        return duration
    
    def get_station_with_most_precipitation(self):
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
        result = list(self.db['weather'].aggregate(pipeline))
        duration = (time.time() - start) * 1000  # ms
        
        if result:
            station_id = result[0]['_id']
            total_precip = result[0]['total_precip']
            station = self.db['stations'].find_one({'id': station_id})
            station_name = station['name'] if station and 'name' in station else station_id
            print(f"\nStation avec le plus de précipitations : {station_name} ({station_id})")
            print(f"=> Précipitations totales : {total_precip:.2f} mm")
            print(f"- Temps de requête : {duration:.2f} ms")
        else:
            print("Aucune donnée de précipitation trouvée.")
