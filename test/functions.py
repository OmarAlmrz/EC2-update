import pandas as pd

def get_s3_json(bucket, key):
    obj = bucket.Object(key).get()
    try:
        return pd.read_json(obj['Body'])
    except ValueError:
        return pd.read_json(obj['Body'], lines=True)