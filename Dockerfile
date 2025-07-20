FROM python:3.11-slim

WORKDIR /app

COPY ./scripts /app/scripts
COPY ./data /app/data
COPY requirements.txt /app/requirements.txt
COPY .env /app/.env

RUN pip install --no-cache-dir -r /app/requirements.txt

CMD ["python", "scripts/import_refactored.py"]