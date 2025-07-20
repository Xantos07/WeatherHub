from config import S3_BUCKET
from importer import WeatherDataImporter
from analyzer import DataQualityAnalyzer
from station_manager import StationManager

def main():
    """Fonction principale d'importation et d'analyse des données."""
    print("|| -- Début de l'importation des données météorologiques -- ||")
    
    # Initialisation des composants
    importer = WeatherDataImporter()
    analyzer = DataQualityAnalyzer()
    station_manager = StationManager()
    
    # Vider les collections
    importer.clear_collections()
    
    # Importer tous les CSV
    importer.import_all_csv_files(S3_BUCKET)
    
    print("\n---- Import terminé! -----")
    
    # Mesure de qualité des données
    quality_metrics = analyzer.measure_data_quality()
    
    # Analyse des précipitations
    analyzer.get_station_with_most_precipitation()
    
    # Benchmarks automatiques
    print("\n____ BENCHMARKS DE PERFORMANCE :____")
    for sid in ["07015", "ILAMAD25", "IICHTE19"]:
        date = station_manager.get_first_date_for_station(sid)
        if date:
            analyzer.benchmark_weather_query(sid, date)
        else:
            print(f"/!\ Aucune donnée trouvée pour la station {sid}")

if __name__ == "__main__":
    main()
