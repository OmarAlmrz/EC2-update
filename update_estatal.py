# Description: This script is used to update the embeddings of leyes estatales database.
from updater import Updater

class EstatalUpdater(Updater):
    def __init__(self):
        super().__init__(logger_name='estatal_updater')
        
    def update_collection(self, collection, df_report, collection_name, folder_path):
        for _, row in df_report.iterrows():
            action = row['action']
            if action == "delete" or action == "update":
                # Delete document from collection or vector db.
                self.delete_document(collection, row['name'])
                self.logger.info(f"Deleting {row['file']} from {collection_name}")
                
            if action == "add" or action == "update":
                compressed_data = self.get_compress_data(folder_path=folder_path, file=row["file"])
                exist = self.check_document_exist(collection, key="name", value=row["name"])
                if exist: 
                    self.logger.warning(f"Document {row['name']}  already exists in {collection_name} collection. Could not add it")
                    continue
                
                # Add document to collection or vector db.
                self.add_data_to_collection(
                    row['name'],
                    collection,
                    compressed_data
                )
                self.logger.info(f"Adding {row['file']} for {collection_name}")
        
        
        self.logger.info(f"Collection {collection_name} updated. Total elements: {collection.count()}")


if __name__ == "__main__":
    
    # Path to report
    databases = [
        {
            "folder_path": "Estatal/",
            "database_path": "/mnt/data/vectordb/Estatal/",
            "collection_name": "Estatal"
        }
    ]

    upd = EstatalUpdater()

    for db in databases:
        database_path = db['database_path']
        folder_path = db['folder_path']
        collection_name = db['collection_name']
        
        # Load collection    
        collection = upd.load_collection(database_path, collection_name)

        # Exisnting folders (estados)
        estados = upd.get_folders_from_s3_path(folder_path)
        
        for estado in estados:
            folder_path_estado = f"{folder_path}{estado}/"
            upd.logger.info(f"Updating {estado} collection")
            
            # Load the report
            df_report = upd.load_report(folder_path_estado)

            # Update the collection with the report data
            upd.update_collection(collection, df_report, collection_name, folder_path_estado)
        
                
        
        