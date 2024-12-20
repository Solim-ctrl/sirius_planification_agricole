import requests
import pandas as pd
import json
import logging
from hdfs import InsecureClient
from io import StringIO

#Configuration de la journalisation
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#Constantes de configuration
CSV_URL = 'https://www.data.gouv.fr/fr/datasets/r/e3d83ab3-dc52-4c99-abaf-8a38050cc68c' #URL du fichier CSV
HDFS_URL = 'http://localhost:9870' #URL du serveur HDFS
HDFS_PATH = '/usr/local/hadoop/data_bis.json' #Chemin du fichier de stockage dans HDFS

def download_csv(url):
        try:
                response = requests.get(url)
                response.raise_for_status() #Vérification  de l'état de la requête
                return response.text #Retourne le contenu du csv sous forme de texte
        except request.exceptions.RequestException as e:
                logging.error(f"Erreur lors du téléchargement du fichier CSV: {e}")
                return None

def process_csv_data(csv_data):
        """Lecture du fichier csv et transformation en JSON"""
        try:
                #J'utilise pandas pour lire le CSV à partir de la chaine de texte récupérée
                df = pd.read_csv(StringIO(csv_data))
                logging.info(f"{len(df)} lignes de données extraites du CSV.")

                #Transformation des données en JSON
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
                hdfs_client = InsecureClient(hdfs_url, user = 'hadoop')

                #Ecrire les données dans le fichier HDFS
                with hdfs_client.write(hdfs_path, encoding='utf-8') as writer:
                        writer.write(json_data)
                logging.info(f"Données chargées avec succès dans HDFS à {hdfs_path}.")
        except Exception as e:
                logging.error(f"Erreur lors du chargement des données dans HDFS : {e}")

def main():
        """Exécution du process ETL"""
        logging.info("Début du téléchargement du CSV...")
        csv_data = download_csv(CSV_URL)

        if csv_data:
                #Traitement du fichier CSV et transformation en JSON
                logging.info("Téléchargement terminé --> Lancement de la transformation...")
                json_data = process_csv_data(csv_data)
                print(json_data)

                #Chargement dans HDFS Hadoop
                logging.info("Chargement des données dans HDFS -->")
                load_data_to_hdfs(json_data, HDFS_URL, HDFS_PATH)
        else:
                logging.error("Le téléchargement du fichier CSV a échoué. Le process ETL  a échoué")

if __name__ == "__main__":
        main()
