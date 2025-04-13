import os
import sys
import traceback
from openai import OpenAI
from dotenv import load_dotenv

# Try to load from .env file, but don't fail if it's not there
try:
    load_dotenv()
except:
    pass

def get_openai_embedding(text, model="text-embedding-3-small"):
    print(f"Debug: Starting get_openai_embedding with model={model}")
    try:
        # Initialize client with API key from environment variable
        api_key = os.environ.get("OPENAI_API_KEY")
        print(f"Debug: API key starts with: {api_key[:4] if api_key else 'None'}")
        
        client = OpenAI(api_key=api_key)
        print("Debug: OpenAI client initialized successfully")
        
        response = client.embeddings.create(
            input=[text],
            model=model
        )
        print("Debug: Got response from OpenAI API")
        
        return response.data[0].embedding
    except Exception as e:
        print(f"Debug: Error in get_openai_embedding: {e}")
        traceback.print_exc(file=sys.stdout)
        raise