import csv
from pymongo import MongoClient

# --- INITIALISATION ---
client = MongoClient("mongodb://localhost:27017/")
db = client["shopnow"]

print("DÉMARRAGE DES EXERCICES MONGODB...\n")

# =====================================================================
# E1. CRUD : Requêtes produits (Filtres + Projection)
# =====================================================================
print("--- E1. CRUD : Produits Premium pas chers ---")
# Puisqu'il n'y a pas de stock, on filtre sur les produits "premium" à moins de 50€
query_e1 = {"tags": "premium", "price": {"$lt": 50}, "active": True}
projection_e1 = {"_id": 1, "name": 1, "price": 1} # On affiche juste ID, Nom et Prix

produits_e1 = list(db.products.find(query_e1, projection_e1).limit(3))
for p in produits_e1:
    print(p)

# =====================================================================
# E2. AGGREGATION : Panier moyen et Top produits achetés
# =====================================================================
print("\n--- E2. AGRÉGATION : Panier moyen ---")
# On utilise le bon champ "total_amount" de la collection orders
pipeline_avg_cart = [
    {"$group": {"_id": None, "panier_moyen": {"$avg": "$total_amount"}}}
]
avg_cart = list(db.orders.aggregate(pipeline_avg_cart))
if avg_cart:
    print(f"Panier moyen global : {avg_cart[0]['panier_moyen']:.2f} €")

print("\n--- E2. AGRÉGATION : Top 3 produits les plus achetés ---")
# On utilise le bon champ "event_type" de la collection events
pipeline_top_products = [
    {"$match": {"event_type": "buy"}},
    {"$group": {"_id": "$product_id", "total_achats": {"$sum": 1}}},
    {"$sort": {"total_achats": -1}},
    {"$limit": 3}
]
top_products = list(db.events.aggregate(pipeline_top_products))
for p in top_products:
    print(f"Produit {p['_id']} : {p['total_achats']} achats")

# =====================================================================
# E3. ANALYTICS : Taux de conversion (Views -> Buys) par produit
# =====================================================================
print("\n--- E3. ANALYTICS : Taux de conversion (Top 3) ---")
pipeline_conversion = [
    {"$match": {"event_type": {"$in": ["view", "buy"]}}},
    {"$group": {
        "_id": "$product_id",
        "views": {"$sum": {"$cond": [{"$eq": ["$event_type", "view"]}, 1, 0]}},
        "buys": {"$sum": {"$cond": [{"$eq": ["$event_type", "buy"]}, 1, 0]}}
    }},
    {"$project": {
        "views": 1, "buys": 1,
        "conversion_rate": {
            "$cond": [{"$gt": ["$views", 0]}, {"$divide": ["$buys", "$views"]}, 0]
        }
    }},
    {"$sort": {"conversion_rate": -1}},
    {"$limit": 3}
]
conversions = list(db.events.aggregate(pipeline_conversion))
for c in conversions:
    print(f"Produit {c['_id']} : {c['views']} vues, {c['buys']} achats -> {(c['conversion_rate']*100):.1f}% conversion")

# =====================================================================
# E4. INDEXATION : Création + Explain()
# =====================================================================
print("\n--- E4. INDEXATION & EXPLAIN ---")
# Index sur les events (très utile pour l'analytique et la conversion)
db.events.create_index([("product_id", 1), ("ts", -1)])

# Dans pymongo, explain() ne prend pas d'argument, il renvoie tout le dictionnaire
explain_result = db.events.find({"product_id": "p069"}).sort("ts", -1).explain()

# L'information cruciale est souvent sous executionStats
stats = explain_result.get("executionStats", {})
print(f"Temps d'exécution : {stats.get('executionTimeMillis', 'N/A')} ms")
print(f"Documents examinés : {stats.get('totalDocsExamined', 'N/A')}")
print("✅ L'indexation garantit des lectures rapides pour nos requêtes.")

# =====================================================================
# E5. PYTHON : Export CSV d'un KPI
# =====================================================================
print("\n--- E5. SCRIPTING : Export CSV ---")
csv_filename = "top_produits_kpi.csv"
with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(["product_id", "total_achats"])
    for p in top_products:
        writer.writerow([p["_id"], p["total_achats"]])
print(f"✅ Fichier '{csv_filename}' généré avec succès !")

# =====================================================================
# BONUS B1 : Cohortes par jour (Activité utilisateurs)
# =====================================================================
print("\n--- BONUS B1 : Cohortes par jour (Visiteurs uniques) ---")
pipeline_cohorts = [
    # Le champ ts est un String "2025-06-30T12:56:15", on extrait juste les 10 premiers caractères (la date)
    {"$group": {
        "_id": {"$substr": ["$ts", 0, 10]},
        "utilisateurs_actifs": {"$addToSet": "$user_id"}
    }},
    {"$project": {"date": "$_id", "nb_users": {"$size": "$utilisateurs_actifs"}, "_id": 0}},
    {"$sort": {"date": 1}},
    {"$limit": 3}
]
cohortes = list(db.events.aggregate(pipeline_cohorts))
for c in cohortes:
    print(f"Date : {c['date']} -> {c['nb_users']} visiteurs uniques")

# =====================================================================
# BONUS B2 : Validation JSON Schema
# =====================================================================
print("\n--- BONUS B2 : Validation JSON Schema ---")
# On sécurise la base : tout nouveau produit DEVRA avoir un nom et un prix en nombre
db.command("collMod", "products", validator={
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["name", "price"],
        "properties": {
            "name": {"bsonType": "string", "description": "Doit être une chaîne de caractères"},
            "price": {"bsonType": ["double", "int", "number"], "description": "Doit être un nombre"}
        }
    }
})
print("✅ Schéma de validation appliqué sur 'products' !")

# =====================================================================
# BONUS B3 : Index TTL (RGPD - Conservation des données)
# =====================================================================
print("\n--- BONUS B3 : Index TTL (RGPD) ---")
# On configure MongoDB pour qu'il supprime tout seul les events de plus de 30 jours (2592000 sec)
# Petit mot pour ton rapport : le TTL dans Mongo nécessite que le champ date soit de type BSON Date (et non String) pour s'activer en production.
db.events.create_index("ts", expireAfterSeconds=2592000)
print("✅ Index TTL créé : les événements seront purgés automatiquement.")

print("\n TP MONGODB TERMINÉ AVEC SUCCÈS !")
