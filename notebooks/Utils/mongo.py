from pymongo import MongoClient
import json
import pandas as pd

class Mongo():
    host = "mongodb"
    db = "prodiac"
    port = 27017
    username = False
    password = False
    connect = None

    def __init__(self):
        self.connect = self.connect_mongo(
            host=self.host, db=self.db, port=self.port, username=self.username, password=self.password)

    def connect_mongo(self, host=host, db=db, port=port, username=username, password=password):
        if username and password:
            mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (
                username, password, host, port, db)
            conn = MongoClient(mongo_uri)
        else:
            conn = MongoClient(host, port)
        return conn[db]

    def df_to_mongo(self, df, collection):
        json_data = json.loads(df.to_json(orient='records'))
        return self.connect[collection].insert_many(json_data)
    
    def mongo_to_df(self,collection,query):
        return pd.DataFrame(list(self.connect[collection].find(query)))
    
    def delete_many(self,collection,query):
        return self.connect[collection].delete_many(query)

        

