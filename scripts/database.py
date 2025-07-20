import boto3
from pymongo import MongoClient
from config import (
    AWS_ACCESS_KEY_ID, 
    AWS_SECRET_ACCESS_KEY, 
    S3_BUCKET,
    MONGO_INITDB_ROOT_USERNAME,
    MONGO_INITDB_ROOT_PASSWORD,
    MONGO_HOST
)

class DatabaseConnector:
    """Gestionnaire des connexions aux bases de données."""
    
    def __init__(self):
        self.s3_client = None
        self.mongo_client = None
        self.db = None
    
    def get_s3_client(self):
        """Retourne le client S3."""
        if self.s3_client is None:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )
        return self.s3_client
    
    def get_mongo_client(self):
        """Retourne le client MongoDB."""
        if self.mongo_client is None:
            # Pour le développement local avec Docker
            self.mongo_client = MongoClient(
                f"mongodb://{MONGO_INITDB_ROOT_USERNAME}:{MONGO_INITDB_ROOT_PASSWORD}@mongodb:27017/"
            )
            # Pour ECR production, décommenter la ligne suivante et commenter la précédente
            # self.mongo_client = MongoClient(
            #     f"mongodb://{MONGO_INITDB_ROOT_USERNAME}:{MONGO_INITDB_ROOT_PASSWORD}@{MONGO_HOST}:27017/"
            # )
        return self.mongo_client
    
    def get_database(self, db_name='weatherhub'):
        """Retourne la base de données MongoDB."""
        if self.db is None:
            client = self.get_mongo_client()
            self.db = client[db_name]
        return self.db
    
    def close_connections(self):
        """Ferme toutes les connexions."""
        if self.mongo_client:
            self.mongo_client.close()

# Instance globale du connecteur
db_connector = DatabaseConnector()
