import json
import os
import sys
from pymongo import MongoClient

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

client = MongoClient("mongodb://localhost:27017/")
db = client["shopnow"]

def import_jsonl(filename, collection_name):
    filename = os.path.join(SCRIPT_DIR, filename)
    if not os.path.exists(filename):
        print(f"❌ Fichier '{filename}' introuvable.")
        return False

    db[collection_name].drop()
    documents = []
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                documents.append(json.loads(line))

    if not documents:
        print(f"⚠️ Fichier '{filename}' vide.")
        return False

    db[collection_name].insert_many(documents)
    print(f"✅ {len(documents)} documents → '{collection_name}'")
    return True

print("Importation MongoDB...\n")

files = [
    ("products.jsonl", "products"),
    ("events.jsonl", "events"),
    ("orders.jsonl", "orders"),
    ("order_items.jsonl", "order_items"),
]

ok = all(import_jsonl(f, c) for f, c in files)

print("-" * 40)
if ok:
    print("✅ Base 'shopnow' prête.")
else:
    print("❌ Échec — vérifier que les .jsonl sont dans le même dossier.")
    sys.exit(1)
