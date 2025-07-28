# Description: This script is used to update the embeddings of db.
import boto3
import chromadb
from abc import ABC, abstractmethod
import gzip
import json
import logging
import os
import pandas as pd

# Load the environment variables
from dotenv import load_dotenv
load_dotenv()

class Updater(ABC):
    def __init__(self):
        # Initialize the S3 client
        self.s3 = boto3.resource(
                service_name='s3',
                aws_access_key_id= os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key= os.getenv('AWS_SECRET_ACCESS_KEY')	,
            )

        self.bucket = self.s3.Bucket(os.getenv('S3_EMBEDDINGS'))
        
        # Initialize the logger
        self.logger = self.init_logger('update')
            
    @abstractmethod
    def update_collection(self, collection, df_report, collection_name, folder_path):
        pass
    
    def load_collection(self, database_path, collection_name):
        # Load database
        client = chromadb.PersistentClient(path=database_path)
        if not client.heartbeat(): exit()

        # Load the collection
        collection = client.get_collection(collection_name)
        if not collection: exit()
        
        self.logger.info(f"Loading collection {collection_name}")
        
        return collection
        
    def load_report(self, folder_path):
        # Load the report
        report_path = f"{folder_path}report.json"
        df_report = self.get_s3_json(report_path)
        if df_report.empty: exit()
        return df_report

    
    def get_s3_json(self,key):
        obj = self.bucket.Object(key).get()
        try:
            return pd.read_json(obj['Body'])
        except ValueError:
            return pd.read_json(obj['Body'], lines=True)

    def get_s3_gzip_json(self,key):
        obj = self.bucket.Object(key).get()
        return json.loads(gzip.decompress(obj['Body'].read()).decode('utf-8'))


    def init_logger(self,logger_name:str,  level=logging.INFO):
        log_file = f"{logger_name}.log"
    
    
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)

            
        # Check if a FileHandler already exists by matching the absolute file path
        if any(isinstance(h, logging.FileHandler) and os.path.basename(h.baseFilename) == f"{logger_name}.log" for h in logger.handlers):
            return  logger
                
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        logger.addHandler(file_handler)

        # Set the formatting for the handler messages
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        file_handler.setFormatter(formatter)
        return logger

    def delete_document(self,collection, value:str, key:str = "name"):
        """
        Delete document from collection or vector db.
        The document is search and delete according to key:value metadata
        """ 
        collection.delete(
            where={key: value}
        )
        removed = collection.get(
            where={key: value}
        )
        ids = removed["ids"]
        if len(ids) > 0:
            self.logger.warning(f"Document {value} not deleted from {collection.name} collection. Still exists.")


    def delete_all_files_in_s3_folder(self, folder_prefix):
        # Ensure folder_prefix ends with a slash
        if not folder_prefix.endswith('/'):
            folder_prefix += '/'
        
        # Collect all objects under the folder prefix
        objects_to_delete = []
        
        for obj in self.bucket.objects.filter(Prefix=folder_prefix):
            # Skip the folder object itself (which is just the prefix)
            if obj.key != folder_prefix:
                objects_to_delete.append({'Key': obj.key})
        
        if not objects_to_delete:
            print(f"No files found in {folder_prefix}")
            return

        # Delete in batches of 1000 (S3 API limit)
        for i in range(0, len(objects_to_delete), 1000):
            response = self.bucket.delete_objects(
                Delete={'Objects': objects_to_delete[i:i+1000]}
            )
            print(f"Deleted {len(objects_to_delete[i:i+1000])} objects:", response)

    
    def check_document_exist(self, collection, key:str, value:str):
        """Check if document exists in collection according to key:value metadata"""
        query = collection.get(where={key: value})
        if query["metadatas"] == []:
            return False
        else:
            for item in query["metadatas"]:
                if item[key] == value:
                    return True
                
        return True
    
    def get_folders_from_s3_path(self, prefix=""):
        """
        Get all top-level folders from a specific S3 bucket path.
        
        Args:
            prefix (str): Path prefix (e.g., "data/processed/")
        
        Returns:
            list: List of folder names (without the prefix)
        """
        folders = set()
        
        # Ensure prefix ends with '/' if not empty
        if prefix and not prefix.endswith('/'):
            prefix += '/'
        
        try:
            # Better approach: use the client through the bucket
            paginator = self.bucket.meta.client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(
                Bucket=self.bucket.name,
                Prefix=prefix,
                Delimiter='/'
            ):
                # Get common prefixes (these are the folders)
                if 'CommonPrefixes' in page:
                    for obj in page['CommonPrefixes']:
                        folder_path = obj['Prefix']
                        # Remove the input prefix and trailing slash to get just the folder name
                        folder_name = folder_path[len(prefix):].rstrip('/')
                        if folder_name:  # Only add non-empty folder names
                            folders.add(folder_name)
        
        except Exception as e:
            print(f"Error accessing S3: {e}")
            return []
        
        return sorted(list(folders))








