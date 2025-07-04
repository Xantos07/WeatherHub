import os
import boto3
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv(os.path.join(os.path.dirname(__file__), '../.env'))

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_BUCKET = os.getenv('S3_BUCKET')

# Créer une connexion S3
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def list_files(bucket_name):
    """Liste les fichiers dans le bucket S3 donné."""
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    if 'Contents' in response:
        return [obj['Key'] for obj in response['Contents']]
    return []

if __name__ == "__main__":
    files = list_files(S3_BUCKET)
    print("Fichiers dans le bucket S3:")
    for f in files:
        print(f)
