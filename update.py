# Description: This script is used to update the embeddings of db.
import boto3
import os
import functions as f
import chromadb

# Load the environment variables
from dotenv import load_dotenv
load_dotenv()

# Initialize the logger
logger = f.init_logger('update')

# Initialize the S3 client
s3 = boto3.resource(
        service_name='s3',
        aws_access_key_id= os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key= os.getenv('AWS_SECRET_ACCESS_KEY')	,
    )

bucket = s3.Bucket(os.getenv('S3_EMBEDDINGS'))

# Path to report 
# /mnt/data/vectordb/SCJN/Tesis/ - Collection name[Juris,Aislada ]
# /mnt/data/vectordb/SCJN/Precedentes/ -  Collection name[Precedentes]
# /mnt/data/vectordb/Federal/ -  Collection name[Leyes]


## Change variables ##
folder_path = "Federal/LF/"
database_path = "/mnt/data/vectordb/SCJN/Precedentes/"
collection_name = "Precedentes"
#######################

# Load database
client = chromadb.PersistentClient(path=database_path)
if not client.heartbeat(): exit()

# Load the collection
collection = client.get_collection(collection_name)
if not collection: exit()


# Load the report
report_path = f"{folder_path}report.json"
df_report = f.get_s3_json(report_path, bucket)

if df_report.empty: exit()

for _, row in df_report.iterrows():
    
    
    if not row['file'].endswith('.json'):
        file_without_extension = row['file'].split('.')[0]
        embeddings_path = f"{folder_path}{file_without_extension}.json.gz"
    
    else:
        # Path to the embeddings
        embeddings_path = f"{folder_path}{row['file']}.gz"
    
    # Load compressed data 
    try:
        compressed_data = f.get_s3_gzip_json(embeddings_path, bucket) 
    
    except Exception as e:
        logger.error(f"Error loading {embeddings_path}: {e}")
        continue
    
    action = row['action']
    
    if collection_name == "Leyes":
        if action == "delete" or action == "update":
            # Delete document from collection or vector db.
            f.delete_document(collection, row['name'])
            logger.info(f"Deleting {row['file']} from {collection_name}")
            
        if action == "add" or action == "update":
            collection.add( 
                documents=compressed_data['documents'],
                metadatas=compressed_data['metadata'],
                embeddings=compressed_data['embeddings'],
                ids=compressed_data['ids']
            )
            logger.info(f"Adding {row['file']} for {collection_name}")
    else:
        collection.add( 
            documents=compressed_data['documents'],
            metadatas=compressed_data['metadata'],
            embeddings=compressed_data['embeddings'],
            ids=compressed_data['ids']
        )
    
        logger.info(f"Adding {row['file']} for {collection_name}")

logger.info(f"Collection {collection_name} updated. Total elements: {collection.count()}")
    
    