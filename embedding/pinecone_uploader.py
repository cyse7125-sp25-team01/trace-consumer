from pinecone import Pinecone, ServerlessSpec
from embedding.embedder import get_openai_embedding

def upload_chunks_to_pinecone(chunks, index_name, api_key, environment):
    # Init Pinecone client
    pc = Pinecone(api_key=api_key)

    # Create index if it doesn't exist
    if index_name not in pc.list_indexes().names():
        pc.create_index(
            name=index_name,
            dimension=1536,
            metric="cosine",
            spec=ServerlessSpec(
                cloud="aws",
                region=environment.split("-")[0]  # e.g., "us-east-1"
            )
        )
        print(f"✅ Created Pinecone index: {index_name}")

    index = pc.Index(index_name)

    # Upload embeddings
    for chunk in chunks:
        try:
            vector = get_openai_embedding(chunk["text"])
            index.upsert([
                {
                    "id": chunk["id"],
                    "values": vector,
                    "metadata": {
                        "professor": chunk.get("professor", "Unknown"),
                        "chunk_type": chunk.get("chunk_type", "unknown"),
                        "text": chunk["text"]
                    }
                }
            ])
            print(f"✅ Uploaded: {chunk['id']}")
        except Exception as e:
            print(f"❌ Failed to upload {chunk['id']}: {e}")
