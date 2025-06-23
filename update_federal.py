# Description: This script is used to update the embeddings of leyes federal database.
from updater import Updater

class FederalUpdater(Updater):
    def __init__(self):
        super().__init__()
        
    def update_collection(self, collection, df_report, collection_name, folder_path):
        for _, row in df_report.iterrows():
            if not row['file'].endswith('.json'):
                file_without_extension = row['file'].split('.')[0]
                embeddings_path = f"{folder_path}{file_without_extension}.json.gz"
            
            else:
                # Path to the embeddings
                embeddings_path = f"{folder_path}{row['file']}.gz"
            
            # Load compressed data 
            try:
                compressed_data = self.get_s3_gzip_json(embeddings_path) 
            
            except Exception as e:
                self.logger.error(f"Error loading {embeddings_path}: {e}")
                continue
            
            action = row['action']
            
            if action == "delete" or action == "update":
                # Delete document from collection or vector db.
                self.delete_document(collection, row['name'])
                self.logger.info(f"Deleting {row['file']} from {collection_name}")
                
            if action == "add" or action == "update":
                collection.add( 
                    documents=compressed_data['documents'],
                    metadatas=compressed_data['metadata'],
                    embeddings=compressed_data['embeddings'],
                    ids=compressed_data['ids']
                )
                logger.info(f"Adding {row['file']} for {collection_name}")
            else:
                logger.warning(f"Unknown action {action} for {row['file']} in {collection_name}")
        
        self.delete_all_files_in_s3_folder(folder_path)
        self.logger.info(f"Collection {collection_name} updated. Total elements: {collection.count()}")


if __name__ == "__main__":
    
    # Path to report
    databases = [
        {
            "folder_path": "Federal/LF/",
            "database_path": "/mnt/data/vectordb/Federal/",
            "collection_name": "Leyes"
        }
    ]

    upd = FederalUpdater()

    for db in databases:
        database_path = db['database_path']
        folder_path = db['folder_path']
        collection_name = db['collection_name']
        
        # Load collection    
        collection = upd.load_collection(database_path, collection_name)

        # Load the report
        df_report = upd.load_report(folder_path)

        # Update the collection with the report data
        upd.update_collection(collection, df_report, collection_name, folder_path)
                
        
        