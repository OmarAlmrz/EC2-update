import chromadb
import os
from langchain_openai import OpenAIEmbeddings
from dotenv import load_dotenv
import boto3
from functions import get_s3_json
load_dotenv()

s3 = boto3.resource(
        service_name='s3',
        aws_access_key_id= os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key= os.getenv('AWS_SECRET_ACCESS_KEY'),
    )
bondar_s3 = s3.Bucket(os.getenv('S3_NAME'))

COLLECTIONS_CONFIG = [
    {
        "path": "/mnt/data/vectordb/",
        "collection": "estatal",
        "metadata_path": "Estatal/estados.json"
    },
    {
        "path": "/mnt/data/vectordb/",
        "collection": "leyes",
        "metadata_path": "Federal/LF/procesamiento.json"
    },
    {
        "path": "/mnt/data/vectordb/",
        "collection": "internacional",
        "metadata_path": "Internacional/procesamiento.json"
    },
    {
        "path": "/mnt/data/vectordb/",
        "collection": "otros",
        "metadata_path": "Otros/procesamiento.json"
    },
    {
        "path": "/mnt/data/vectordb/",
        "collection": "aislada",
        "metadata_path": "SCJN/Aislada/metadata.json"
    },
    {
        "path": "/mnt/data/vectordb/",
        "collection": "juris",
        "metadata_path": "SCJN/Juris/metadata.json"
    }
    
]

if __name__ == "__main__":
    embedding_model = OpenAIEmbeddings(model="text-embedding-3-large", openai_api_key=os.getenv('OPENAI_API_KEY'))
    query = "que dice el primer artículo de la ley"
    query_embedding = embedding_model.embed_query(query)
    
    for config in COLLECTIONS_CONFIG:
        print(f"Processing collection: {config['collection']}")
        metadata_df = get_s3_json(bondar_s3, config["metadata_path"])
        
        if metadata_df.empty:
            print(f"No metadata found in: {config['metadata_path']}")
            continue
        
        # Initialize ChromaDB client and collection
        client = chromadb.PersistentClient(config["path"])
        collection = client.get_collection(name=config["collection"])

        print(f"Total items in collection: {collection.count()}")
        if config['collection'] == "Aislada" or config['collection'] == "Juris" or config['collection'] == "precedentes":
            if metadata_df.shape[0] != collection.count():
                print(f"❌ Mismatch in counts for collection '{config['collection']}': Metadata has {metadata_df.shape[0]}, Collection has {collection.count()}")
                continue
            else:
                print(f"✅ Counts match for collection '{config['collection']}': {metadata_df.shape[0]} items")
            
            continue
            
        if "name" in metadata_df.columns:
            leyes_nombre = metadata_df['name'].tolist()
        elif "ley" in metadata_df.columns:
            leyes_nombre = metadata_df['ley'].tolist()
        else:
            raise ValueError("No 'name' or 'ley' column found in metadata")
        
        checked = 0
        missing = []
        for ley in leyes_nombre:
            filtro = {'name': {'$eq': ley}}
            results =  collection.query(
                query_embeddings=query_embedding, 
                n_results=5,
                where=filtro,  
            )
            ids = results['ids'][0]

            if len(ids) == 0:
                missing.append(ley)
            else:
                checked += 1
        
        if checked == len(leyes_nombre):
            print(f"✅ Todo en orden en la colección '{config['collection']}'")
        
        else:
            print(f"❌ Problemas encontrados en la colección '{config['collection']}'")
            print(f"Documentos faltantes ({len(missing)}): {"\n".join(missing)}")
            print(f"Documentos verificados: {checked}/{len(leyes_nombre)}")