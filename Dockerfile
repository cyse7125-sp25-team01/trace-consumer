FROM python:3.10-slim

WORKDIR /app

# COPY kafka_pdf_consumer.py .
# COPY config ./config
# COPY gcs ./gcs
# COPY parser ./parser
# COPY db ./db
# COPY embedding ./embedding
COPY requirements.txt .


RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "kafka_pdf_consumer.py"]
