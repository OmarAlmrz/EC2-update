# Description: This script is used to update the embeddings of leyes federal database.
from update_internacional import InternacionalUpdater
from updater import Updater

class OtrosUpdater(Updater):
    def __init__(self):
        super().__init__( logger_name='otros_updater')

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
            "folder_path": "Otros/",
            "database_path": "/mnt/data/vectordb/Otros/",
            "collection_name": "otros"
        }
    ]

    upd = OtrosUpdater()

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
                
        
        