# WeatherHub

## 📋 Description

Projet comportant Airbyte, le service ECR d'AWS et un système d'importation et de synchronisation.

Lien vers comment déployer le projt sous le Service ECR d'AWS 
[wiki de déploiement sur AWS](https://github.com/Xantos07/WeatherHub/wiki/D%C3%A9ployer-sur-AWS-ECR)


## 🏗️ Architecture des Données

### Collections MongoDB

#### Collection `stations`
```json
{
  "_id": ObjectId,
  "id_station": "07015",
  "nom": "Abbeville",
  "latitude": 50.1357,
  "longitude": 1.8331,
  "altitude": 69,
  "source_file": "data/StationsMeteorologiques-dataset/...",
  "created_at": ISODate
}
```

#### Collection `weather`
```json
{
  "_id": ObjectId,
  "station_id": "07015",
  "dh_utc": ISODate,
  "measurements": {
    "temperature": {
      "value": 18.5,
      "unit": "degC",
      "original": "65.3 °F"
    },
    "pressure": {
      "value": 1013.25,
      "unit": "hPa",
      "original": "29.91 in"
    },
    "humidity": {
      "value": 78,
      "unit": "%"
    },
    "wind": {
      "speed": 15.3,
      "speed_unit": "km/h",
      "gust": 22.1,
      "gust_unit": "km/h",
      "direction": 270,
      "direction_original": "W"
    },
    "precipitation": {
      "rate": 0.5,
      "accumulation": 1.2,
      "unit": "mm"
    },
    "visibility": {
      "value": 10000,
      "unit": "m"
    },
    "dew_point": {
      "value": 14.2,
      "unit": "degC",
      "original": "57.6 °F"
    },
    "solar_radiation": {
      "value": 650,
      "unit": "w/m²"
    },
    "uv_index": 5,
    "cloud_cover": "FEW",
    "weather_code": "10"
  },
  "metadata": {
    "source_file": "data/WeatherFR-dataset/...",
    "row_index": 1234,
    "hour_index": 5,
    "created_at": ISODate
  }
}
```

## 🔄 Logique de Transformation

### 1. Sources de Données

- **StationsMeteorologiques** : Données de référence des stations
- **Hourly** : Données horaires en français (°C, hPa, km/h, mm)
- **WeatherBE/FR** : Données en anglais avec unités impériales (°F, in, mph)

### 2. Processus de Normalisation

#### Étape 1 : Extraction des Valeurs
```python
# Exemple pour "65.3 °F"
value = 65.3        # Valeur numérique
unit = "°F"         # Unité extraite
original = "65.3 °F" # Chaîne d'origine
```

#### Étape 2 : Conversion des Unités (WeatherBE/FR → Hourly)
- **Température** : °F → °C `(f - 32) * 5/9`
- **Pression** : inHg → hPa `inhg * 33.8639`
- **Vitesse** : mph → km/h `mph * 1.60934`
- **Précipitations** : in → mm `inches * 25.4`

#### Étape 3 : Harmonisation des Noms de Champs
```python
HOURLY_TO_BE_FR = {
    "temperature": "Temperature",
    "point_de_rosee": "Dew Point",
    "humidite": "Humidity",
    "pression": "Pressure",
    # ...
}
```

### 3. Structure Unifiée

Toutes les données sont transformées pour avoir :
- **Même structure** : `measurements` avec sous-objets
- **Mêmes unités** : °C, hPa, km/h, mm, etc.
- **Mêmes noms de champs** : harmonisés selon le format hourly
- **Métadonnées** : traçabilité de la source et de la transformation

## 🚀 Utilisation

### Prérequis
```bash
pip install boto3 pandas pymongo python-dotenv
```

### Variables d'Environnement (.env)
```env
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
S3_BUCKET=your_bucket_name
MONGO_INITDB_ROOT_USERNAME=your_mongo_user
MONGO_INITDB_ROOT_PASSWORD=your_mongo_password
```

## 📊 Mesure de Qualité

Le script génère automatiquement un rapport de qualité incluant :

- **Taux d'erreur global** : Pourcentage de champs manquants/invalides
- **Qualité des stations** : ID, nom, coordonnées manquants
- **Qualité des mesures** : Champs météo manquants
- **Répartition par source** : Nombre de mesures par fichier

### Seuils de Qualité
- **< 5%** : ✅ Excellente qualité
- **5-15%** : ⚠️ Qualité acceptable
- **> 15%** : ❌ Qualité à améliorer

## 🛠️ Fonctions Principales

- `extract_value_unit()` : Extraction valeur/unité/original
- `normalize_be_fr_record()` : Conversion unités + harmonisation
- `normalize_hourly_record()` : Harmonisation noms de champs
- `build_weather_doc()` : Construction document final
- `measure_data_quality()` : Mesure qualité post-migration

## 📝 Logs et Debugging

Le script affiche :
- Progression de l'import par fichier
- Nombre de stations/mesures importées
- Erreurs détaillées avec numéro de ligne
- Rapport de qualité complet
