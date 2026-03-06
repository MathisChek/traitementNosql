import csv
import os
from pymongo import MongoClient

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

client = MongoClient("mongodb://localhost:27017/")
db = client["shopnow"]

# E1 — CRUD : Produits premium < 50€
print("--- E1 : CRUD (Produits premium < 50€) ---")
produits = list(db.products.find(
    {"tags": "premium", "price": {"$lt": 50}, "active": True},
    {"_id": 1, "name": 1, "price": 1}
).limit(3))
for p in produits:
    print(p)

# E2 — Agrégation : Panier moyen + Top 3 produits achetés
print("\n--- E2 : Panier moyen ---")
avg_cart = list(db.orders.aggregate([
    {"$group": {"_id": None, "panier_moyen": {"$avg": "$total_amount"}}}
]))
if avg_cart:
    print(f"Panier moyen : {avg_cart[0]['panier_moyen']:.2f} €")

print("\n--- E2 : Top 3 produits achetés ---")
top_products = list(db.events.aggregate([
    {"$match": {"event_type": "buy"}},
    {"$group": {"_id": "$product_id", "total_achats": {"$sum": 1}}},
    {"$sort": {"total_achats": -1}},
    {"$limit": 3}
]))
for p in top_products:
    print(f"Produit {p['_id']} : {p['total_achats']} achats")

# E3 — Analytics : Taux de conversion (views → buys)
print("\n--- E3 : Taux de conversion (Top 3) ---")
conversions = list(db.events.aggregate([
    {"$match": {"event_type": {"$in": ["view", "buy"]}}},
    {"$group": {
        "_id": "$product_id",
        "views": {"$sum": {"$cond": [{"$eq": ["$event_type", "view"]}, 1, 0]}},
        "buys":  {"$sum": {"$cond": [{"$eq": ["$event_type", "buy"]}, 1, 0]}}
    }},
    {"$project": {
        "views": 1, "buys": 1,
        "conversion_rate": {
            "$cond": [{"$gt": ["$views", 0]}, {"$divide": ["$buys", "$views"]}, 0]
        }
    }},
    {"$sort": {"conversion_rate": -1}},
    {"$limit": 3}
]))
for c in conversions:
    print(f"Produit {c['_id']} : {c['views']} vues, {c['buys']} achats → {c['conversion_rate']*100:.1f}%")

# E4 — Indexation + explain()
print("\n--- E4 : Indexation & Explain ---")
db.events.create_index([("product_id", 1), ("ts", -1)])
db.products.create_index([("category", 1), ("price", 1)])
print("Index 1 : events(product_id, ts) → filtre analytique + tri chronologique")
print("Index 2 : products(category, price) → recherche catalogue par catégorie/prix")

explain = db.events.find({"product_id": "p069"}).sort("ts", -1).explain()
stats = explain.get("executionStats", {})
print(f"Explain events(p069) : {stats.get('executionTimeMillis', 'N/A')} ms, {stats.get('totalDocsExamined', 'N/A')} docs examinés")

# E5 — Export CSV d'un KPI
print("\n--- E5 : Export CSV ---")
csv_path = os.path.join(SCRIPT_DIR, "top_produits_kpi.csv")
with open(csv_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["product_id", "total_achats"])
    for p in top_products:
        writer.writerow([p["_id"], p["total_achats"]])
print("✅ top_produits_kpi.csv généré")

# B1 — Cohortes par jour (visiteurs uniques)
print("\n--- B1 : Cohortes par jour ---")
cohortes = list(db.events.aggregate([
    {"$group": {
        "_id": {"$substr": ["$ts", 0, 10]},
        "users": {"$addToSet": "$user_id"}
    }},
    {"$project": {"date": "$_id", "nb_users": {"$size": "$users"}, "_id": 0}},
    {"$sort": {"date": 1}},
    {"$limit": 3}
]))
for c in cohortes:
    print(f"{c['date']} → {c['nb_users']} visiteurs uniques")

# B2 — Validation JSON Schema
print("\n--- B2 : JSON Schema ---")
db.command("collMod", "products", validator={
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["name", "price"],
        "properties": {
            "name":  {"bsonType": "string"},
            "price": {"bsonType": ["double", "int", "number"]}
        }
    }
})
print("✅ Schéma appliqué sur 'products'")

# B3 — Index TTL (purge events > 30 jours)
print("\n--- B3 : Index TTL ---")
db.events.create_index("ts", expireAfterSeconds=2592000)
print("✅ TTL 30j sur events.ts")

print("\n✅ TP MONGODB TERMINÉ")
