import time
import os
from datetime import datetime, timedelta
from database import db_connector
from importer import WeatherDataImporter
from analyzer import DataQualityAnalyzer
from station_manager import StationManager

class S3SyncMonitor:

    print(f"🚀 Démarrage de la surveillance S3 (intervalle: 10 mins)")
    """Surveillance et synchronisation automatique avec S3."""
    
    def __init__(self, s3_bucket, check_interval=600):  # 10 minutes par défaut
        self.s3_bucket = s3_bucket
        self.check_interval = check_interval
        self.importer = WeatherDataImporter()
        self.db = db_connector.get_database()
        self.analyzer = DataQualityAnalyzer()
        self.station_manager = StationManager()
        
    def get_last_sync_time(self):
        """Récupère la dernière heure de synchronisation."""
        sync_info = self.db.sync_metadata.find_one({"_id": "last_sync"})
        return sync_info["timestamp"] if sync_info else datetime.min
    
    def update_last_sync_time(self):
        """Met à jour l'heure de dernière synchronisation."""
        self.db.sync_metadata.update_one(
            {"_id": "last_sync"},
            {"$set": {"timestamp": datetime.now()}},
            upsert=True
        )
    
    def get_new_files(self):
        """Détecte les nouveaux fichiers S3 depuis la dernière sync."""
        last_sync = self.get_last_sync_time()
        
        try:
            files = self.importer.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                # Optionnel: filtrer par date de modification
            )
            
            if 'Contents' not in files:
                return []
            
            new_files = []
            for obj in files['Contents']:
                if (obj['Key'].endswith('.csv') and 
                    obj['LastModified'].replace(tzinfo=None) > last_sync):
                    new_files.append(obj['Key'])
            
            return new_files
            
        except Exception as e:
            print(f"❌ Erreur lors de la détection des nouveaux fichiers: {e}")
            return []
    
    def sync_new_files(self):
        """Synchronise uniquement les nouveaux fichiers."""
        new_files = self.get_new_files()
        
        if not new_files:
            print("✅ Aucun nouveau fichier détecté")
            return
        
        print(f"🔄 Synchronisation de {len(new_files)} nouveaux fichiers:")
        for file_key in new_files:
            print(f"  - {file_key}")
            try:
                self.importer.import_csv_to_mongo(file_key, self.s3_bucket)
                print(f"  ✅ {file_key} importé avec succès")
            except Exception as e:
                print(f"  ❌ Erreur lors de l'import de {file_key}: {e}")
        
        self.update_last_sync_time()
        print(f"🎉 Synchronisation terminée à {datetime.now()}")
    
    def start_monitoring(self):
        """Démarre la surveillance continue."""
        print(f"🚀 Démarrage de la surveillance S3 (intervalle: {self.check_interval}s)")
        print(f"📂 Bucket surveillé: {self.s3_bucket}")
        
        # Import initial au démarrage si la DB est vide
        self.initial_import_if_empty()
        
        while True:
            try:
                print(f"\n⏰ Vérification à {datetime.now()}")
                self.sync_new_files()
                
                print(f"😴 Attente de {self.check_interval} secondes...")
                time.sleep(self.check_interval)
                
            except KeyboardInterrupt:
                print("\n⏹️  Arrêt de la surveillance demandé")
                break
            except Exception as e:
                print(f"❌ Erreur durante la surveillance: {e}")
                print(f"🔄 Reprise dans {self.check_interval} secondes...")
                time.sleep(self.check_interval)
    
    def initial_import_if_empty(self):
        """Import initial si les collections sont vides."""
        stations_count = self.db['stations'].count_documents({})
        weather_count = self.db['weather'].count_documents({})
        
        if stations_count == 0 and weather_count == 0:
            print("|| -- Import initial complet - Mode MAIN -- ||")
            
            # Vider les collections (comme dans main.py)
            print("🧹 Vidage des collections MongoDB...")
            self.importer.clear_collections()
            
            # Import complet
            print("🌱 Import initial en cours...")
            self.importer.import_all_csv_files(self.s3_bucket)
            self.update_last_sync_time()
            print("✅ Import initial terminé!")
            
            # Analyse qualité des données (comme dans main.py)
            print("\n🔍 Lancement de l'analyse qualité...")
            quality_metrics = self.analyzer.measure_data_quality()
            
            # Analyse des précipitations (comme dans main.py)
            print("\n🌧️ Analyse des précipitations...")
            self.analyzer.get_station_with_most_precipitation()
            
            # Benchmarks automatiques (comme dans main.py)
            print("\n⚡ BENCHMARKS DE PERFORMANCE:")
            for sid in ["07015", "ILAMAD25", "IICHTE19"]:
                date = self.station_manager.get_first_date_for_station(sid)
                if date:
                    self.analyzer.benchmark_weather_query(sid, date)
                else:
                    print(f"/!\ Aucune donnée trouvée pour la station {sid}")
            
            print("\n🎉 Import initial et analyse terminés - Passage en mode surveillance")
            
        else:
            print(f"📊 DB existante: {stations_count} stations, {weather_count} mesures")

if __name__ == "__main__":
    S3_BUCKET = os.getenv('S3_BUCKET')
    CHECK_INTERVAL = int(os.getenv('SYNC_INTERVAL', '600'))  # 105 minutes
    
    monitor = S3SyncMonitor(S3_BUCKET, CHECK_INTERVAL)
    monitor.start_monitoring()
