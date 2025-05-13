import pandas as pd
import gzip
import json
import logging
import os

def get_s3_json(key, bucket):
    obj = bucket.Object(key).get()
    try:
        return pd.read_json(obj['Body'])
    except ValueError:
        return pd.read_json(obj['Body'], lines=True)

def get_s3_gzip_json(key, bucket):
    obj = bucket.Object(key).get()
    return json.loads(gzip.decompress(obj['Body'].read()).decode('utf-8'))

def init_logger(logger_name:str,  level=logging.INFO):
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

def delete_document(collection, value:str, key:str = "name"):
    """
    Delete document from collection or vector db.
    The document is search and delete according to key:value metadata
    """ 
    collection.delete(
        where={key: value}
    )
