import pandas as pd
import json
from datetime import datetime
from io import BytesIO
from database import db_connector
from normalizer import WeatherDataNormalizer
from station_manager import StationManager
from utils import build_weather_doc

class WeatherDataImporter:
    """Importateur de données météorologiques depuis S3 vers MongoDB."""
    
    def __init__(self):
        self.s3_client = db_connector.get_s3_client()
        self.db = db_connector.get_database()
        self.normalizer = WeatherDataNormalizer()
        self.station_manager = StationManager()
    
    def import_csv_to_mongo(self, s3_key, s3_bucket):
        """Importe un fichier CSV depuis S3 vers MongoDB."""
        try:
            # Lire le fichier CSV depuis S3
            obj = self.s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
            df = pd.read_csv(BytesIO(obj['Body'].read()))
            
            if '_airbyte_data' not in df.columns:
                print(f"  /!\  Colonne '_airbyte_data' manquante dans {s3_key}")
                return
            
            if 'StationsMeteorologiques' in s3_key:
                self._import_stations_meteorologiques(df, s3_key)
            elif 'WeatherBE' in s3_key or 'WeatherFR' in s3_key:
                self._import_weather_be_fr(df, s3_key)
            else:
                print(f"  /!\  Type de fichier non reconnu: {s3_key}")
                
        except Exception as e:
            print(f"  /!\ Erreur lors de l'importation de {s3_key}: {e}")
    
    def _import_stations_meteorologiques(self, df, s3_key):
        """Importe les données des stations météorologiques."""
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
                        self.db['stations'].insert_one(station)
                        stations_count += 1
                
                # Import données hourly
                if 'hourly' in data and isinstance(data['hourly'], dict):
                    for station_id, records in data['hourly'].items():
                        for hour_index, record in enumerate(records):
                            norm_record = self.normalizer.normalize_hourly_record(record)
                            doc = build_weather_doc(
                                norm_record, s3_key, 
                                row_index=None, 
                                hour_index=hour_index, 
                                station_id=station_id
                            )
                            self.db['weather'].insert_one(doc)
                            weather_count += 1
                            
            except Exception as e:
                print(f" /!\ Erreur lors du traitement de la ligne {index}: {e}")
        
        print(f" --{stations_count} stations, {weather_count} données weather--")
    
    def _import_weather_be_fr(self, df, s3_key):
        """Importe les données WeatherBE/WeatherFR."""
        # Déterminer le type de station
        station_type = 'WeatherBE' if 'WeatherBE' in s3_key else 'WeatherFR'
        
        # Créer la station correspondante
        station_id = self.station_manager.create_weather_station(station_type, s3_key)
        
        weather_count = 0
        for index, row in df.iterrows():
            try:
                data = json.loads(row['_airbyte_data'])
                norm_record = self.normalizer.normalize_be_fr_record(data)
                # Ajouter l'ID de station aux données normalisées
                norm_record['station_id'] = station_id
                doc = build_weather_doc(
                    norm_record, s3_key, 
                    row_index=index, 
                    station_id=station_id
                )
                self.db['weather'].insert_one(doc)
                weather_count += 1
            except Exception as e:
                print(f"    /!\ Erreur ligne {index}: {e}")
        
        print(f"-- Station {station_type} créée avec {weather_count} données weather--")
    
    def import_all_csv_files(self, s3_bucket):
        """Importe tous les fichiers CSV du bucket S3."""
        try:
            files = self.s3_client.list_objects_v2(Bucket=s3_bucket)
            if 'Contents' in files:
                csv_files = [obj['Key'] for obj in files['Contents'] if obj['Key'].endswith('.csv')]
                
                for csv_file in csv_files:
                    print(f"Importation de {csv_file}...")
                    self.import_csv_to_mongo(csv_file, s3_bucket)
            else:
                print("Aucun fichier trouvé dans le bucket S3")
                
        except Exception as e:
            print(f"/!\ Erreur lors de la liste des fichiers S3: {e} /!`\`")
    
    def clear_collections(self):
        """Vide les collections MongoDB."""
        self.db['stations'].delete_many({})
        self.db['weather'].delete_many({})
        print("Collections vidées")
