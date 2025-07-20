FROM python:3.11-slim

WORKDIR /app

COPY ./scripts /app/scripts
COPY ./data /app/data
COPY requirements.txt /app/requirements.txt
COPY .env /app/.env

RUN pip install --no-cache-dir -r /app/requirements.txt

# Point d'entrée par défaut (peut être surchargé par docker-compose)
CMD ["python", "scripts/sync_monitor.py"]