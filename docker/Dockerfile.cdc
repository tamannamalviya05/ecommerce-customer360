
FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir "kafka-python>=2.0.2,<3.0"

ENV PYTHONPATH=/app

CMD ["python", "-m", "src.ingestion.mysql_cdc_consumer"]
