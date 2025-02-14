#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from hdfs import InsecureClient
from pymongo import MongoClient
import json

#Connexion à HDFS
hdfs_client = InsecureClient('http://172.31.249.92:9870', user = 'hadoop')

#Connexion à MongoDB
mongo_client = MongoClient("mongodb://172.31.253.153:27017")
db = mongo_client["gsk_db"]
collection = db["my_collection"]

#Lecture du fichier JSON depuis HDFS
with hdfs_client.read('/usr/local/hadoop/big_data.json') as reader:

        #Lecture ligne par ligne du contenu JSON
        for line in reader:
                try:
                        data = json.loads(line)
                        if isinstance(data, list):
                                collection.insert_many(data)
                        else:
                                collection.insert_one(data)
                except json.JSONDecodeError as e:
                        print(f"Erreur de décodage du JSON : {e}")

        #data = json.load(reader)

#Insertion des données dans MongoDB
#if isinstance(data,list): #Si j'ai une lise de documents
        #collection.insert_many(data)
#else:
#       collection.insert_one(data)

print("Données transférées de HDFS à MongoDB avec succès ...!")

