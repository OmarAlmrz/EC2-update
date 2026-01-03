# Description: This script is used to update the embeddings of scjn database.
from updater import Updater

class SCJNUpdater(Updater):
    def __init__(self):
        super().__init__(logger_name='scjn_updater')
        
    def update_collection(self, collection, df_report, collection_name, folder_path):
        for _, row in df_report.iterrows():
            action = row["action"]
            if action == "add":
                compressed_data = self.get_compress_data(folder_path=folder_path, file=row["file"])
                exist = self.check_document_exist(collection, key="registro digital",value=row["registro digital"])
                if exist: 
                    self.logger.info(f"Document {row['registro digital']} already exists in {collection_name} collection. Could not add it")
                    continue
                
                # Check if embeddings list exists and is not empty
                if not compressed_data.get("embeddings") or len(compressed_data["embeddings"]) == 0:
                    self.logger.warning(f"Skipping {row['registro digital']}: embeddings list is empty.")
                    continue
                
                # Check if any individual embedding is empty
                if any(not embedding or len(embedding) == 0 for embedding in compressed_data["embeddings"]):
                    self.logger.warning(f"Skipping {row['registro digital']}: one or more embeddings are empty.")
                    continue
            
                collection.add( 
                    documents=compressed_data['documents'],
                    metadatas=compressed_data['metadata'],
                    embeddings=compressed_data['embeddings'],
                    ids=compressed_data['ids']
                )

                self.logger.info(f"Adding {row['file']} for {collection_name}")
        
        self.logger.info(f"Collection {collection_name} updated. Total elements: {collection.count()}")


if __name__ == "__main__":
    
    # Path to report
    databases = [
        {
            "folder_path": "SCJN/juris/",
            "database_path": "/mnt/data/vectordb/",
            "collection_name": "juris"
        },
        {
            "folder_path": "SCJN/aislada/",
            "database_path": "/mnt/data/vectordb/",
            "collection_name": "aislada"
        },
        {
            "folder_path": "SCJN/precedentes/",
            "database_path": "/mnt/data/vectordb/",
            "collection_name": "precedentes"
        },
    ]
    
    upd = SCJNUpdater()

    for db in databases:
        database_path = db['database_path']
        folder_path = db['folder_path']
        collection_name = db['collection_name']
        
        # Load collection    
        collection = upd.load_collection(database_path, collection_name)

        try:
            # Load the report
            df_report = upd.load_report(folder_path)
            
            if df_report is None:
                print(f"No report found for {collection_name} in {folder_path}")
                continue
            
        except Exception as e:
            print(f"Error loading report for {collection_name} in {folder_path}: {e}")
            continue

        # Update the collection with the report data
        upd.update_collection(collection, df_report, collection_name, folder_path)
                
                
        
        
        
        