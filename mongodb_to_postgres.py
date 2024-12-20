from pymongo import MongoClient, errors as mongo_errors
import psycopg2
from psycopg2 import sql, OperationalError, Error
from psycopg2.extras import Json

try:
    # Connexion à MongoDB
    try:
        mongo_client = MongoClient("mongodb://localhost:27017/")
        mongo_db = mongo_client["gsk_db"]
        mongo_collection = mongo_db["ma_collection_bis"]  # Remplacez par votre nom de collection
        print("Connexion réussie à MongoDB ...!")
    except mongo_errors.ConnectionFailure as e:
        print(f"Erreur de connexion à MongoDB ---> : {e}")
        raise

    # Connexion à PostgreSQL
    try:
        pg_conn = psycopg2.connect(
            dbname="gsk_postgres",
            user="postgres",
            password="postgres",  # Remplacez par votre mot de passe PostgreSQL
            host="localhost",
            port="5432"
        )
        pg_cur = pg_conn.cursor()
        print("Connexion réussie à PostgreSQL ...!")
    except OperationalError as e:
        print(f"Erreur de connexion à PostgreSQL ---> : {e}")
        raise

    # Extraction des données depuis MongoDB
    try:
        documents = mongo_collection.find({})
        document_count = mongo_collection.count_documents({})
        print(f"Nombre total de documents trouvés dans MongoDB : {document_count}")
        if document_count == 0:
            print("Aucun document à insérer. Fin du script.")
            raise SystemExit
                except mongo_errors.PyMongoError as e:
        print(f"Erreur lors de la récupération des données depuis MongoDB ---> : {e}")
        raise

    # Insertion dans PostgreSQL
    try:
        for document in documents:
            # Extraction de la chaîne de données
            data_str = document.get(
                'fra;jour;clage_90;PourAvec;tx_indic_7J_DC;tx_indic_7J_hosp;tx_indic_7J_SC;tx_prev_hosp;tx_prev_SC',
                None
            )

            if data_str:
                print(f"Document extrait : {data_str}")
                # Transformation en liste de valeurs
                values = data_str.split(';')

                if len(values) == 9:
                    fra, jour, clage_90, PourAvec, tx_indic_7J_DC, tx_indic_7J_hosp, tx_indic_7J_SC, tx_prev_hosp, tx_prev_SC = values

                    # Mapping des champs
                    clage_90 = int(clage_90) if clage_90 else None
                    PourAvec = int(PourAvec) if PourAvec else None
                    tx_indic_7J_DC = float(tx_indic_7J_DC) if tx_indic_7J_DC else None
                    tx_indic_7J_hosp = float(tx_indic_7J_hosp) if tx_indic_7J_hosp else None
                    tx_indic_7J_SC = float(tx_indic_7J_SC) if tx_indic_7J_SC else None
                    tx_prev_hosp = float(tx_prev_hosp) if tx_prev_hosp else None
                    tx_prev_SC = float(tx_prev_SC) if tx_prev_SC else None

                    # Insertion dans la table PostgreSQL
                    pg_cur.execute("""
                        INSERT INTO transformed_data (
                            fra, jour, clage_90, PourAvec, tx_indic_7J_DC,
                            tx_indic_7J_hosp, tx_indic_7J_SC, tx_prev_hosp, tx_prev_SC
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (fra, jour, clage_90, PourAvec, tx_indic_7J_DC, tx_indic_7J_hosp, tx_indic_7J_SC, tx_prev_hosp, tx_prev_SC))

                    print(f"Document inséré avec succès : {values}")
                else:
                                    print(f"Document avec un format inattendu : {values}")
            else:
                print("Document manquant la clé de données.")

        # Validation des changements
        pg_conn.commit()
        print("Toutes les données ont été transférées avec succès ...!")

    except Error as e:
        pg_conn.rollback()
        print(f"Erreur lors de l'insertion des données ---> : {e}")
        raise

except Exception as e:
    print(f"Une erreur inattendue est survenue ---> : {e}")

finally:
    # Fermeture des connexions
    try:
        if 'pg_cur' in locals() and pg_cur:
            pg_cur.close()
        if 'pg_conn' in locals() and pg_conn:
            pg_conn.close()
        if 'mongo_client' in locals() and mongo_client:
            mongo_client.close()
        print("Connexions fermées ...!")
    except Exception as e:
        print(f"Erreur lors de la fermeture des connexions ---> : {e}")