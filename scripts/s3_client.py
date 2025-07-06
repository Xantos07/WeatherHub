import os
import boto3
from dotenv import load_dotenv
import pandas as pd
import json
from pymongo import MongoClient
from io import BytesIO
from datetime import datetime

# Charger les variables d'environnement depuis le fichier .env
load_dotenv(os.path.join(os.path.dirname(__file__), '../.env'))

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_BUCKET = os.getenv('S3_BUCKET')
MONGO_INITDB_ROOT_USERNAME = os.getenv('MONGO_INITDB_ROOT_USERNAME')
MONGO_INITDB_ROOT_PASSWORD = os.getenv('MONGO_INITDB_ROOT_PASSWORD')

# Créer une connexion S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Connexion MongoDB
mongo_client = MongoClient(f"mongodb://{MONGO_INITDB_ROOT_USERNAME}:{MONGO_INITDB_ROOT_PASSWORD}@mongodb:27017/")
db = mongo_client['weatherhub']

def list_files(bucket_name):
    """Liste les fichiers dans le bucket S3 donné."""
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    if 'Contents' in response:
        return [obj['Key'] for obj in response['Contents']]
    return []

def import_csv_to_mongo(s3_key):
    """Importe les données CSV dans MongoDB avec une structure optimisée."""
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        df = pd.read_csv(BytesIO(obj['Body'].read()))
        
        print(f"Fichier {s3_key}:")
        print(f"  - Nombre de lignes: {len(df)}")
        print(f"  - Nombre de colonnes: {len(df.columns)}")
        print(f"  - Colonnes: {list(df.columns)}")
        
        # Vérifier si c'est un fichier avec des données JSON dans _airbyte_data
        if '_airbyte_data' in df.columns and 'StationsMeteorologiques' in s3_key:
            # Extraire et parser le JSON de la colonne _airbyte_data
            for index, row in df.iterrows():
                json_string = row['_airbyte_data']
                try:
                    data = json.loads(json_string)
                    print(f"  - JSON parsé avec succès, keys: {list(data.keys()) if isinstance(data, dict) else 'Non-dict'}")
                    
                    # Appliquer la logique de découpage JSON
                    import_json_to_mongo(s3_key, data)
                    return  # Sortir après avoir traité le JSON
                    
                except json.JSONDecodeError as e:
                    print(f"  - Erreur de parsing JSON: {e}")
                    continue
        
        # Vérifier si c'est un fichier Weather avec des données JSON dans _airbyte_data
        elif '_airbyte_data' in df.columns and ('WeatherBE' in s3_key or 'WeatherFR' in s3_key):
            # Traiter comme données météo structurées
            weather_documents = []
            
            for index, row in df.iterrows():
                json_string = row['_airbyte_data']
                try:
                    data = json.loads(json_string)
                    
                    # Ajouter les métadonnées et traiter chaque enregistrement
                    if isinstance(data, dict):
                        data['source_file'] = s3_key
                        data['created_at'] = datetime.now()
                        data['row_index'] = index
                        weather_documents.append(data)
                    elif isinstance(data, list):
                        for i, item in enumerate(data):
                            if isinstance(item, dict):
                                item['source_file'] = s3_key
                                item['created_at'] = datetime.now()
                                item['row_index'] = index
                                item['item_index'] = i
                                weather_documents.append(item)
                                
                except json.JSONDecodeError as e:
                    print(f"  - Erreur de parsing JSON ligne {index}: {e}")
                    continue
            
            # Déterminer la collection
            if 'WeatherBE' in s3_key:
                collection_name = 'weather_data_be'
            elif 'WeatherFR' in s3_key:
                collection_name = 'weather_data_fr'
            else:
                collection_name = 'weather_data_csv'
                
            collection = db[collection_name]
            collection.delete_many({'source_file': s3_key})
            
            if weather_documents:
                collection.insert_many(weather_documents)
                print(f"Importé {len(weather_documents)} enregistrements de {s3_key} dans {collection_name}")
            else:
                print(f"Aucun document valide trouvé dans {s3_key}")
                
        else:
            # Traitement CSV classique
            # Convertir les types problématiques en string pour MongoDB
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str)
                elif 'datetime' in str(df[col].dtype):
                    df[col] = df[col].astype(str)
            
            # Déterminer la collection basée sur le nom du fichier/dossier
            if 'StationsMeteorologiques' in s3_key:
                collection_name = 'stations'
            elif 'WeatherBE' in s3_key:
                collection_name = 'weather_data_be'
            elif 'WeatherFR' in s3_key:
                collection_name = 'weather_data_fr'
            else:
                collection_name = 'weather_data_csv'
            
            collection = db[collection_name]
            
            # Ajouter des métadonnées
            df['source_file'] = s3_key
            df['created_at'] = datetime.now()
            df['dataset_type'] = collection_name
            
            # Nettoyer les données existantes de ce fichier
            collection.delete_many({'source_file': s3_key})
            
            records = df.to_dict(orient='records')
            collection.insert_many(records)
            print(f"Importé {len(records)} enregistrements de {s3_key} dans {collection_name}")
        
    except Exception as e:
        print(f"Erreur lors de l'import CSV {s3_key}: {str(e)}")

def import_json_to_mongo(s3_key, data):
    """Importe les données JSON dans MongoDB avec une structure optimisée."""
    
    # Gestion spéciale pour StationsMeteorologiques
    if 'stations' in data and isinstance(data['stations'], list):
        # Collection pour les stations
        stations_collection = db['stations']
        stations_collection.delete_many({'source_file': s3_key})
        
        stations = data['stations']
        for station in stations:
            if isinstance(station, dict):
                station['source_file'] = s3_key
                station['created_at'] = datetime.now()
        
        stations_collection.insert_many(stations)
        print(f"Importé {len(stations)} stations de {s3_key} dans stations")
    
    # Gestion des données horaires (hourly)
    if 'hourly' in data and isinstance(data['hourly'], dict):
        hourly_data = data['hourly']
        weather_documents = []
        
        for station_id, station_data in hourly_data.items():
            if isinstance(station_data, list):
                for i, hourly_record in enumerate(station_data):
                    if isinstance(hourly_record, dict):
                        base_doc = {
                            'station_id': station_id,
                            'hour_index': i,
                            'source_file': s3_key,
                            'created_at': datetime.now()
                        }
                        
                        # Document météo principal
                        weather_doc = {**base_doc}
                        weather_doc.update(hourly_record)
                        weather_documents.append(weather_doc)
        
        # Insérer dans la collection appropriée
        if weather_documents:
            weather_collection = db['weather_hourly_data']
            weather_collection.delete_many({'source_file': s3_key})
            weather_collection.insert_many(weather_documents)
            print(f"Importé {len(weather_documents)} enregistrements horaires de {s3_key} dans weather_hourly_data")
    
    # Gestion des autres structures JSON
    if not ('stations' in data or 'hourly' in data):
        json_collection = db['weather_json_data']
        
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    item['source_file'] = s3_key
                    item['created_at'] = datetime.now()
            json_collection.insert_many(data)
            print(f"Importé {len(data)} enregistrements de {s3_key} dans weather_json_data")
        else:
            if isinstance(data, dict):
                data['source_file'] = s3_key
                data['created_at'] = datetime.now()
            json_collection.insert_one(data)
            print(f"Importé 1 enregistrement de {s3_key} dans weather_json_data")

def create_indexes():
    """Crée les index pour optimiser les performances."""
    try:
        # Index pour les stations
        db['stations'].create_index([('id', 1)], background=True)
        db['stations'].create_index([('source_file', 1)], background=True)
        
        # Index pour les données CSV météo
        db['weather_data_be'].create_index([('source_file', 1)], background=True)
        db['weather_data_fr'].create_index([('source_file', 1)], background=True)
        db['weather_hourly_data'].create_index([('station_id', 1), ('hour_index', 1)], background=True)
        
        print("Index créés avec succès")
    except Exception as e:
        print(f"Erreur lors de la création des index: {str(e)}")

if __name__ == "__main__":
    print("Connexion à S3 et MongoDB...")
    print(f"Bucket S3: {S3_BUCKET}")
    
    files = list_files(S3_BUCKET)
    csv_files = [f for f in files if f.endswith('.csv')]
    print(f"Fichiers CSV trouvés: {csv_files}")
    
    # Nettoyer toutes les collections avant l'import
    collections_to_clean = ['stations', 'weather_data_be', 'weather_data_fr', 'weather_hourly_data', 'weather_json_data']
    for collection_name in collections_to_clean:
        db[collection_name].delete_many({})
        print(f"Collection {collection_name} nettoyée")
    
    # Traiter tous les fichiers CSV
    for csv_file in csv_files:
        print(f"\nTraitement du fichier: {csv_file}")
        import_csv_to_mongo(csv_file)
    
    # Créer les index pour optimiser les performances
    create_indexes()
    
    # Afficher un résumé des collections
    print("\n=== RÉSUMÉ DES COLLECTIONS ===")
    for collection_name in collections_to_clean:
        count = db[collection_name].count_documents({})
        print(f"{collection_name}: {count} documents")
    
    print("\nImport terminé!")