from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col

#Création d'une sessions Spark
spark = SparkSession.builder.appName("TransformData").getOrCreate()

#Lecture des données depuis HDFS (ici un fichier JSON)
df = spark.read.json("hdfs://localhost:9000/usr/local/hadoop/data_bis.json")

#Vérification du contenu du dataframe lu
if len (df.columns) > 0:
        #Application de la transformation: Majuscules de la première colonne --> minuscules
        first_column = df.columns[0]
        df = df.withColumn(first_column, lower(col(first_column))) #Majuscule --> minuscules

        #Réduction de la partition pour générer un seul ficier de sortie
        df = df.coalesce(1)

        #Enregistrement des données transformées dans HDFS
        df.write.json("hdfs://localhost:9000/usr/local/hadoop/transformed_data_bis.json")
        print ("Transformation terminée et fichier sauvegardée avec succès dans HDFS")
else:
        print("Le fichier JSON est vide !")