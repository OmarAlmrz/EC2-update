# Description: This script is used to update the embeddings of scjn database.
from updater import Updater

class SCJNUpdater(Updater):
    def __init__(self):
        super().__init__()
        
    def update_collection(self, collection, df_report, collection_name, folder_path):
        for _, row in df_report.iterrows():
            if not row['file'].endswith('.json'):
                file_without_extension = row['file'].split('.')[0]
                embeddings_path = f"{folder_path}{file_without_extension}.json.json.gz"
            
            else:
                # Path to the embeddings
                embeddings_path = f"{folder_path}{row['file']}.json.gz"
            
            # Load compressed data 
            try:
                compressed_data = self.get_s3_gzip_json(embeddings_path) 
            
            except Exception as e:
                self.logger.error(f"Error loading {embeddings_path}: {e}")
                continue
            
            action = row['action']
            
            collection.add( 
                documents=compressed_data['documents'],
                metadatas=compressed_data['metadata'],
                embeddings=compressed_data['embeddings'],
                ids=compressed_data['ids']
            )

            self.logger.info(f"Adding {row['file']} for {collection_name}")
        
        self.delete_all_files_in_s3_folder(folder_path)
        self.logger.info(f"Collection {collection_name} updated. Total elements: {collection.count()}")


if __name__ == "__main__":
    
    # Path to report
    databases = [
        {
            "folder_path": "SCJN/Juris/",
            "database_path": "/mnt/data/vectordb/SCJN/Tesis/",
            "collection_name": "Juris"
        },
        {
            "folder_path": "SCJN/Aislada/",
            "database_path": "/mnt/data/vectordb/SCJN/Tesis/",
            "collection_name": "Aislada"
        },
        {
            "folder_path": "SCJN/Precedentes/",
            "database_path": "/mnt/data/vectordb/SCJN/Precedentes/",
            "collection_name": "Precedentes"
        },
    ]
    
    upd = SCJNUpdater()

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
                
                
        
        
        
        