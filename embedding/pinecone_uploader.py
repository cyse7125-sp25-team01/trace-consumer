import logging
import traceback
from pinecone import Pinecone, ServerlessSpec
from embedding.embedder import get_openai_embedding

logger = logging.getLogger('kafka-pdf-consumer')

def upload_chunks_to_pinecone(chunks, index_name, api_key, environment):
    try:
        logger.info(f"Starting Pinecone upload with API key: {api_key[:4]}... to index: {index_name}")
        logger.info(f"Uploading {len(chunks)} chunks to Pinecone in environment: {environment}")
        
        # Init Pinecone client
        pc = Pinecone(api_key=api_key)
        logger.info("Pinecone client initialized")
        
        # Check existing indexes
        existing_indexes = pc.list_indexes().names()
        logger.info(f"Existing Pinecone indexes: {existing_indexes}")
        
        # Create index if it doesn't exist
        if index_name not in existing_indexes:
            logger.info(f"Creating new index: {index_name} with dimension 1536")
            pc.create_index(
                name=index_name,
                dimension=1536,
                metric="cosine",
                spec=ServerlessSpec(
                    cloud="aws",
                    region=environment.split("-")[0]  # e.g., "us-east-1"
                )
            )
            logger.info(f"✅ Created Pinecone index: {index_name}")
        
        index = pc.Index(index_name)
        logger.info(f"Obtained index reference for: {index_name}")
        
        # Process chunks in batches
        batch_size = 10  # Smaller batch for testing
        total_uploaded = 0
        
        for i in range(0, len(chunks), batch_size):
            batch = chunks[i:i+batch_size]
            batch_vectors = []
            
            for chunk in batch:
                try:
                    # Get embedding
                    vector = get_openai_embedding(chunk["text"])
                    
                    # Validate vector
                    if len(vector) != 1536:
                        logger.warning(f"Vector dimension mismatch: {len(vector)} (expected 1536)")
                    
                    # Add to batch
                    batch_vectors.append({
                        "id": chunk["id"],
                        "values": vector,
                        "metadata": {
                            "professor": chunk.get("professor", "Unknown"),
                            "chunk_type": chunk.get("chunk_type", "unknown"),
                            "text": chunk["text"][:500]  # Truncate text for metadata
                        }
                    })
                except Exception as e:
                    logger.error(f"❌ Failed to create embedding for {chunk['id']}: {e}")
            
            # Upload batch
            if batch_vectors:
                try:
                    result = index.upsert(vectors=batch_vectors)
                    logger.info(f"✅ Batch {i//batch_size + 1}: Uploaded {len(batch_vectors)} vectors. Result: {result}")
                    total_uploaded += len(batch_vectors)
                except Exception as e:
                    logger.error(f"❌ Failed to upload batch: {e}")
                    logger.error(traceback.format_exc())
        
        # Verify upload with stats
        try:
            stats = index.describe_index_stats()
            logger.info(f"Index stats after upload: {stats}")
            logger.info(f"Total vectors in index: {stats.get('total_vector_count', 'unknown')}")
        except Exception as e:
            logger.error(f"❌ Failed to get index stats: {e}")
        
        logger.info(f"✅ Completed Pinecone upload. Total chunks processed: {len(chunks)}, successfully uploaded: {total_uploaded}")
        return True
    
    except Exception as e:
        logger.error(f"❌ Fatal error in upload_chunks_to_pinecone: {e}")
        logger.error(traceback.format_exc())
        return False