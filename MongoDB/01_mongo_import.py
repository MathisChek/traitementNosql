import json
import os
import sys
from pymongo import MongoClient

# Connexion au conteneur Docker MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["shopnow"]

def import_jsonl(filename, collection_name):
    """Lit un fichier .jsonl et l'insère dans MongoDB. Retourne True si succès, False sinon."""
    if not os.path.exists(filename):
        print(f"❌ ERREUR : Le fichier '{filename}' est introuvable.")
        return False

    # On nettoie la collection pour avoir une base propre à chaque exécution
    db[collection_name].drop()

    documents = []
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():  # Ignore les lignes vides
                documents.append(json.loads(line))

    if documents:
        db[collection_name].insert_many(documents)
        print(f"✅ {len(documents)} documents insérés dans la collection '{collection_name}'")
        return True
    else:
        print(f"⚠️ ATTENTION : Le fichier '{filename}' est vide.")
        return False

print("Démarrage de l'importation MongoDB...\n")

# Liste des fichiers à importer
files_to_import = [
    ("products.jsonl", "products"),
    ("events.jsonl", "events"),
    ("orders.jsonl", "orders"),
    ("order_items.jsonl", "order_items")
]

# On traque si tout s'est bien passé
tout_est_ok = True

for filename, collection in files_to_import:
    success = import_jsonl(filename, collection)
    if not success:
        tout_est_ok = False

print("-" * 40)
if tout_est_ok:
    print("✅IMPORTATION RÉUSSIE ! La base 'shopnow' est prête pour les exos.")
else:
    print("❌ ÉCHEC DE L'IMPORTATION !")
    print("Assure-toi d'avoir extrait les fichiers .jsonl du .zip dans le même dossier que ce script.")
    sys.exit(1)
