import os
from dotenv import load_dotenv

# Try to load from .env file (useful for local development), 
# but don't fail if it's not there
try:
    load_dotenv()
except:
    pass

DB_CONFIG = {
    'dbname': os.environ.get("DB_NAME"),
    'user': os.environ.get("DB_USER"),
    'password': os.environ.get("DB_PASSWORD"),
    'host': os.environ.get("DB_HOST"),
    'port': os.environ.get("DB_PORT"),
}

PINECONE_API_KEY = os.environ.get("PINECONE_API_KEY")
PINECONE_ENVIRONMENT = os.environ.get("PINECONE_ENVIRONMENT")
PINECONE_INDEX = os.environ.get("PINECONE_INDEX")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC_NAME = os.environ.get("KAFKA_TOPIC_NAME")
LOCAL_PDF_DIR = os.environ.get("LOCAL_PDF_DIR")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID")