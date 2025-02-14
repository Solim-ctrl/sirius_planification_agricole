#!/usr/bin/env python
# coding: utf-8

# In[2]:


pip install hdfs


# In[1]:


import hdfs
print(hdfs.__version__)


# In[5]:


import pandas as pd
import json
import logging
from hdfs import InsecureClient
from io import StringIO

# Configuration de la journalisation
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constantes de configuration
CSV_FILE_PATH = 'crop_production_enriched.csv'  # Chemin vers votre fichier CSV local
#HDFS_URL = 'http://172.31.249.92:9870'  # URL du serveur HDFS
HDFS_URL = 'http://localhost:9870'  # Utilisez le nom d'hôte que vous avez ajouté dans le fichier hosts
HDFS_PATH = '/usr/local/hadoop/big_data.json'  # Chemin du fichier de stockage dans HDFS

def process_csv_data(csv_data):
    """Lecture du fichier CSV et transformation en JSON"""
    try:
        # Utilisation de pandas pour lire le CSV à partir de la chaine de texte récupérée
        df = pd.read_csv(StringIO(csv_data))
        logging.info(f"{len(df)} lignes de données extraites du CSV.")

        # Transformation des données en JSON
        json_data = df.to_json(orient='records', lines=True)
        return json_data
    except Exception as e:
        logging.error(f"Erreur lors du traitement du CSV : {e}")
        return None

def load_data_to_hdfs(json_data, hdfs_url, hdfs_path):
    """Chargement des données JSON dans Hadoop HDFS"""
    if not json_data:
        logging.warning("Aucune donnée à charger dans HDFS !")
        return
    try:
        hdfs_client = InsecureClient(hdfs_url, user='hadoop')

        # Écrire les données dans le fichier HDFS
        with hdfs_client.write(hdfs_path, encoding='utf-8') as writer:
            writer.write(json_data)
        logging.info(f"Données chargées avec succès dans HDFS à {hdfs_path}.")
    except Exception as e:
        logging.error(f"Erreur lors du chargement des données dans HDFS : {e}")

def main():
    """Exécution du process ETL"""
    logging.info("Début du traitement du fichier CSV local...")
    
    try:
        # Lecture du fichier CSV local
        with open(CSV_FILE_PATH, 'r', encoding='utf-8') as file:
            csv_data = file.read()
            logging.info("Fichier CSV lu avec succès.")
            
            # Traitement du fichier CSV et transformation en JSON
            logging.info("Lancement de la transformation...")
            json_data = process_csv_data(csv_data)
            print(json_data)

            # Chargement dans HDFS Hadoop
            logging.info("Chargement des données dans HDFS -->")
            load_data_to_hdfs(json_data, HDFS_URL, HDFS_PATH)

    except FileNotFoundError as e:
        logging.error(f"Le fichier CSV spécifié n'a pas été trouvé : {e}")
    except Exception as e:
        logging.error(f"Erreur lors du traitement du fichier CSV local : {e}")

if __name__ == "__main__":
    main()


# In[ ]:




