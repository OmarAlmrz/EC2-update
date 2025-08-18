
import boto3
import os

# Load the environment variables
from dotenv import load_dotenv
load_dotenv()

def delete_all_files_in_s3_folder(bucket, folder_prefix):
    print(f"Removing: {folder_prefix}")
    # Ensure folder_prefix ends with a slash
    if not folder_prefix.endswith('/'):
        folder_prefix += '/'
    
    # Collect all objects under the folder prefix
    objects_to_delete = []
    
    for obj in bucket.objects.filter(Prefix=folder_prefix):
        # Skip the folder object itself (which is just the prefix)
        if obj.key != folder_prefix:
            objects_to_delete.append({'Key': obj.key})
    
    if not objects_to_delete:
        print(f"No files found in {folder_prefix}")
        return

    # Delete in batches of 1000 (S3 API limit)
    for i in range(0, len(objects_to_delete), 1000):
        response = bucket.delete_objects(
            Delete={'Objects': objects_to_delete[i:i+1000]}
        )
        print(f"Deleted {len(objects_to_delete[i:i+1000])} objects:", response)




def get_folders_from_s3_path(bucket, prefix=""):
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
        paginator = bucket.meta.client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(
            Bucket=bucket.name,
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
                


if __name__ == "__main__":
    s3 = boto3.resource(
            service_name='s3',
            aws_access_key_id= os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key= os.getenv('AWS_SECRET_ACCESS_KEY')	,
        )

    bucket = s3.Bucket(os.getenv('S3_EMBEDDINGS'))
    
    paths = [
        "SCJN/Juris/",
        "SCJN/Aislada/",
        "SCJN/Precedentes/",
        "Federal/LF/",
        "Internacional/",
        "Otros/",
    ]
    for path in paths:
        delete_all_files_in_s3_folder(bucket, path)
    
    # Delete estados
    estado_path = "Estatal/"
    # Exisnting folders (estados)
    estados = get_folders_from_s3_path(bucket, estado_path)
    
    for estado in estados:
        folder_path_estado = f"{estado_path}{estado}/"
        delete_all_files_in_s3_folder(bucket, folder_path_estado)
    