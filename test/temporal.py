import chromadb
import os

client = chromadb.PersistentClient("/mnt/data/vectordb/")
collection = client.get_collection("Juris")

data = collection.get(where={"epoca": 12})
print(f"Total items retrieved: {len(data['ids'])}")

# delete 
collection.delete(where={"epoca": 12})

# Verify deletion
data_after_deletion = collection.get(where={"epoca": 12})
print(f"Total items after deletion: {len(data_after_deletion['ids'])}")