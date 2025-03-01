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
# /mnt/data/vectordb/SCJN/Tesis/ - [Juris,Aislada ]
# /mnt/data/vectordb/SCJN/Precedentes/ - [Precedentes]
# /mnt/data/vectordb/Federal/ - [Leyes]

## Change variables ##
folder_path = "SCJN/Aislada/"
database_path = "/mnt/data/vectordb/SCJN/Tesis/"
collection_name = "Aislada"
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
    
    # Path to the embeddings
    embeddings_path = f"{folder_path}{row['file']}.gz"
    
    # Load compressed data 
    compressed_data = f.get_s3_gzip_json(embeddings_path, bucket) 
    
    collection.add( 
        documents=compressed_data['documents'],
        metadatas=compressed_data['metadata'],
        embeddings=compressed_data['embeddings'],
        ids=compressed_data['ids']
    )
    
    logger.info(f"Adding {row['file']} for {collection_name}")

logger.info(f"Collection {collection_name} updated. Total elements: {collection.count()}")
    
    