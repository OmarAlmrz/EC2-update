import chromadb
import os
from langchain_openai import OpenAIEmbeddings
import boto3
from functions import get_s3_json
from dotenv import load_dotenv
load_dotenv()

# s3 file 

s3 = boto3.resource(
        service_name='s3',
        aws_access_key_id= os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key= os.getenv('AWS_SECRET_ACCESS_KEY'),
    )
bondar_s3 = s3.Bucket(os.getenv('S3_NAME'))

# Vector database
v_directory = "/mnt/data/vectordb/Federal/"
embedding_model = OpenAIEmbeddings(model="text-embedding-3-large", openai_api_key=os.getenv('OPENAI_API_KEY'))
client = chromadb.PersistentClient(v_directory)
collection = client.get_collection("Leyes")

df = get_s3_json(bondar_s3, "Federal/LF/procesamiento.json")

# QUERY
query = "que dice el primer artículo de la ley"
query_embedding = embedding_model.embed_query(query)

checked = 0
for idx, item in df.iterrows():
    filtro = {'$and': [{'estado': {'$eq': item["estado"]}}, {'name': {'$eq': item["name"]}}]}
 
    results = collection.query(
        query_embeddings=query_embedding, 
        n_results=5,
        where=filtro,  
    )
    
    ids = results['ids'][0]
   
    if len(ids) == 0:
        print(f"No se encontraron resultados para la ley: {item["estado"]} - {item["name"]}")

    else: checked += 1
    
print(f"Total de leyes revisadas: {checked} de {len(df)}")