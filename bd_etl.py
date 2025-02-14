#!/usr/bin/env python
# coding: utf-8

# In[3]:


pip install ruamel.yaml


# In[4]:


pip install clyent==1.2.1


# In[5]:


pip install nbformat==5.4.0


# In[1]:


pip install kaggle


# In[6]:


import os
import shutil


os.environ["KAGGLE_CONFIG_DIR"] = "."

# Identifiant du dataset sur Kaggle (correspondant à l'URL fournie)
dataset = "abhinand05/crop-production-in-india"

# Téléchargement du dataset
os.system(f"kaggle datasets download -d {dataset} --unzip")


zip_filename = f"{dataset.split('/')[-1]}.zip"

if os.path.exists(zip_filename):
    shutil.unpack_archive(zip_filename, ".", "zip")
    print("Extraction terminée ✅")
    os.remove(zip_filename)
else:
    print("⚠️ Aucun fichier ZIP trouvé après le téléchargement !")


# In[21]:


import pandas as pd

# Chargement du dataset
df = pd.read_csv("crop_production.csv")  

# Identification des types de cultures
cultures_distinctes = df["Crop"].unique()
print("Types de cultures distinctes :")
print(cultures_distinctes)

# Mise à jour du dictionnaire
duree_culture_estimee = {
    "Rice": {"Kharif": 120, "Rabi": 150, "Summer": 90},
    "Wheat": {"Rabi": 140},
    "Maize": {"Kharif": 110, "Rabi": 130, "Summer": 100},
    "Sugarcane": {"Annual": 365},
    "Other Kharif pulses": {"Kharif": 100},
    "Arecanut": {"Kharif": 200},
    "Cotton": {"Kharif": 150, "Summer": 180},
    "Barley": {"Rabi": 130},
    "Sorghum": {"Kharif": 100, "Rabi": 120},
    "Groundnut": {"Kharif": 110, "Rabi": 120},
    "Soybean": {"Kharif": 110, "Rabi": 120},
    "Chili": {"Kharif": 150, "Rabi": 180},
    "Tomato": {"Kharif": 90, "Rabi": 150},
    "Onion": {"Kharif": 130, "Rabi": 150},
    "Cabbage": {"Rabi": 120, "Summer": 120},
    "Potato": {"Winter": 90, "Spring": 120, "Summer": 120},
    "Pea": {"Rabi": 120},
}

# Ajout d'une durée par défaut 
duree_par_defaut = 120

# Fonction pour estimer la durée de culture
def estimer_duree(crop, season):
    return duree_culture_estimee.get(crop, {}).get(season, duree_par_defaut)

# Application de la durée estimée au dataset
df["estimated_duration"] = df.apply(lambda row: estimer_duree(row["Crop"], row["Season"].strip()), axis=1)

# Sauvegarde du  dataset enrichi
df.to_csv("crop_production_enriched.csv", index=False)

# Affichage d'un aperçu du dataset enrichi
print("\n✅ Dataset enrichi avec la durée de culture sauvegardé sous 'crop_production_enriched.csv'")
print("Quelques exemples enrichis :")
print(df.head())


# In[17]:


df.head()

