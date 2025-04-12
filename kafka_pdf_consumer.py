import json
import os
import sys
import logging
import traceback
from kafka import KafkaConsumer
import psycopg2
from config.settings import DB_CONFIG
from gcs.downloader import download_pdf_from_gcs
from parser.pdf_text_extractor import extract_text_from_pdf
from parser.trace_cleaner import process_pdf_text, extract_metadata_from_filename
from db.db_insert import store_in_database
from embedding.chunker import chunk_document_data
from embedding.pinecone_uploader import upload_chunks_to_pinecone
from config.settings import PINECONE_API_KEY, PINECONE_ENVIRONMENT, PINECONE_INDEX

# Set up logging to be captured in Kubernetes
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stderr)]
)
logger = logging.getLogger('kafka-pdf-consumer')

# Create a directory for PDFs with proper absolute path
local_dir = "/tmp/pdfs"  # Use a standard temp directory in containers
os.makedirs(local_dir, exist_ok=True)

# Initialize database connection
try:
    conn = psycopg2.connect(**DB_CONFIG)
    logger.info("✅ Database connection established")
except Exception as e:
    logger.error(f"❌ Database connection failed: {e}")
    sys.exit(1)

# Initialize the Kafka consumer
try:
    consumer = KafkaConsumer(
        "gcs-topic",
        bootstrap_servers="kafka:9092",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="pdf-processing-group",
        # Improved settings for reliability
        auto_offset_reset='earliest',  # Start from earliest unprocessed message
        enable_auto_commit=True,  # Auto-commit offsets
        auto_commit_interval_ms=5000,  # Commit every 5 seconds
        session_timeout_ms=30000,  # 30-second session timeout
        heartbeat_interval_ms=10000  # 10-second heartbeat
    )

    # Log what topics the consumer is actually subscribed to
    logger.info(f"Consumer subscribed to topics: {consumer.subscription()}")
    logger.info(f"Consumer group ID: pdf-processing-group")
    logger.info("📥 Kafka Consumer is running and waiting for PDF upload events...")

except Exception as e:
    logger.error(f"❌ Failed to initialize Kafka consumer: {e}")
    logger.error(traceback.format_exc())
    sys.exit(1)

# Main processing loop
try:
    # Poll for messages with a timeout to allow for clean shutdown
    while True:
        # Poll with a timeout to allow for clean shutdown
        message_batch = consumer.poll(timeout_ms=1000)
        if not message_batch:
            # No messages received in this poll cycle
            continue

        # Process each partition's messages
        for tp, messages in message_batch.items():
            logger.info(f"Received {len(messages)} messages from partition {tp.partition}")

            for message in messages:
                try:
                    data = message.value
                    logger.info(f"Received message: {json.dumps(data)[:200]}...")  # Log first 200 chars

                    # Extract message data
                    bucket = data.get("bucket")
                    file_name = data.get("file")

                    if not bucket or not file_name:
                        logger.error(f"❌ Missing bucket or file_name in message: {data}")
                        continue

                    logger.info(f"📂 Processing file: {file_name} from bucket: {bucket}")

                    # Create local path for PDF
                    local_path = os.path.join(local_dir, os.path.basename(file_name))

                    # Process the PDF
                    try:
                        # Download PDF
                        logger.info(f"⬇️ Downloading PDF from GCS: {bucket}/{file_name}")
                        download_pdf_from_gcs(bucket, file_name, local_path)

                        # Extract text
                        logger.info(f"📄 Extracting text from PDF")
                        text = extract_text_from_pdf(local_path)

                        # Parse and process
                        logger.info(f"🔍 Processing PDF text")
                        parsed_data = process_pdf_text(text)
                        parsed_data["course_info"].update(extract_metadata_from_filename(file_name))

                        # Store in database
                        logger.info(f"💾 Storing data in PostgreSQL")
                        document_id = store_in_database(conn, parsed_data, file_name)

                        # Vectorize and store in Pinecone
                        if document_id:
                            logger.info(f"🔗 Chunking document {document_id} for embedding")
                            chunked_data = chunk_document_data({
                                "document_id": document_id,
                                "document_name": file_name,
                                "full_text": parsed_data["full_text"],
                                "comments": parsed_data.get("comments", []),
                                "professor": parsed_data["course_info"].get("instructor", "Unknown")
                            })

                            logger.info(f"📤 Uploading chunks to Pinecone")
                            upload_chunks_to_pinecone(
                                chunked_data,
                                index_name=PINECONE_INDEX,
                                api_key=PINECONE_API_KEY,
                                environment=PINECONE_ENVIRONMENT
                            )

                            logger.info(f"✅ Vectorized and uploaded document ID {document_id} to Pinecone")

                        # Cleanup
                        if os.path.exists(local_path):
                            os.remove(local_path)
                            logger.info(f"🧹 Removed temporary file: {local_path}")

                        logger.info(f"✅ Successfully processed '{file_name}'")

                    except Exception as e:
                        logger.error(f"❌ Error processing file {file_name}: {e}")
                        logger.error(traceback.format_exc())

                except Exception as e:
                    logger.error(f"❌ Error processing message: {e}")
                    logger.error(traceback.format_exc())

except KeyboardInterrupt:
    logger.info("👋 Shutting down consumer")
except Exception as e:
    logger.error(f"❌ Unexpected error in consumer loop: {e}")
    logger.error(traceback.format_exc())
finally:
    # Clean shutdown
    if 'consumer' in locals():
        consumer.close()
    if 'conn' in locals() and conn:
        conn.close()
    logger.info("🛑 Consumer has shut down")