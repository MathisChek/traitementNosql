import json
import pandas as pd
from neo4j import GraphDatabase
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "platon01"


class Neo4jImporter:
    def __init__(self):
        try:
            self.driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
            self.driver.verify_connectivity()
            print("✅ Connexion Neo4j OK")
        except Exception as e:
            print(f"❌ Connexion impossible : {e}")
            sys.exit(1)

    def close(self):
        self.driver.close()

    def run_query(self, query, params=None):
        with self.driver.session() as session:
            return session.run(query, params)

    def import_data(self):
        print("🧹 Nettoyage du graphe...")
        self.run_query("MATCH (n) DETACH DELETE n")

        # Contraintes d'unicité
        self.run_query("CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE")
        self.run_query("CREATE CONSTRAINT product_id IF NOT EXISTS FOR (p:Product) REQUIRE p.id IS UNIQUE")

        # --- USERS ---
        df_users = pd.read_csv(os.path.join(SCRIPT_DIR, "neo4j_users.csv"))
        print(f"📥 {len(df_users)} utilisateurs...")
        for _, r in df_users.iterrows():
            self.run_query(
                "CREATE (:User {id: $id, city: $city, age_range: $age})",
                {"id": str(r["user_id"]), "city": r["city"], "age": r["age_range"]},
            )

        # --- PRODUCTS ---
        df_prods = pd.read_csv(os.path.join(SCRIPT_DIR, "neo4j_products.csv"))
        print(f"📥 {len(df_prods)} produits...")
        for _, r in df_prods.iterrows():
            self.run_query(
                "CREATE (:Product {id: $id, name: $name, category: $cat, price: toFloat($price)})",
                {"id": str(r["product_id"]), "name": r["name"], "cat": r["category"], "price": r["price"]},
            )

        # --- FOLLOWS ---
        df_fol = pd.read_csv(os.path.join(SCRIPT_DIR, "neo4j_follows.csv"))
        print(f"🔗 {len(df_fol)} relations FOLLOWS...")
        for _, r in df_fol.iterrows():
            self.run_query(
                "MATCH (a:User {id: $id1}), (b:User {id: $id2}) MERGE (a)-[:FOLLOWS]->(b)",
                {"id1": str(r["from_user"]), "id2": str(r["to_user"])},
            )

        # --- VIEWED ---
        df_view = pd.read_csv(os.path.join(SCRIPT_DIR, "neo4j_viewed.csv"))
        print(f"🔗 {len(df_view)} relations VIEWED...")
        for _, r in df_view.iterrows():
            self.run_query(
                "MATCH (u:User {id: $uid}), (p:Product {id: $pid}) MERGE (u)-[:VIEWED {count: $cnt}]->(p)",
                {"uid": str(r["user_id"]), "pid": str(r["product_id"]), "cnt": int(r["count"])},
            )

        # --- BOUGHT (dérivé des buy events dans events.jsonl) ---
        events_path = os.path.join(SCRIPT_DIR, "events.jsonl")
        if not os.path.exists(events_path):
            events_path = os.path.join(SCRIPT_DIR, "..", "MongoDB", "events.jsonl")
        if os.path.exists(events_path):
            buys = []
            with open(events_path, "r", encoding="utf-8") as f:
                for line in f:
                    evt = json.loads(line)
                    if evt.get("event_type") == "buy":
                        buys.append(evt)

            print(f"🔗 {len(buys)} relations BOUGHT (depuis events.jsonl)...")
            for b in buys:
                self.run_query(
                    "MATCH (u:User {id: $uid}), (p:Product {id: $pid}) MERGE (u)-[r:BOUGHT]->(p) "
                    "ON CREATE SET r.ts = $ts, r.count = 1 "
                    "ON MATCH SET r.count = r.count + 1",
                    {"uid": b["user_id"], "pid": b["product_id"], "ts": b["ts"]},
                )
        else:
            print("⚠️ events.jsonl introuvable (ni dans Neo4j/ ni dans ../MongoDB/)")

        # Résumé
        with self.driver.session() as session:
            counts = session.run(
                "MATCH (u:User) WITH count(u) AS users "
                "MATCH (p:Product) WITH users, count(p) AS products "
                "MATCH ()-[r]->() RETURN users, products, count(r) AS relations"
            ).single()
            print(f"\n✅ Graphe importé : {counts['users']} users, {counts['products']} produits, {counts['relations']} relations")


if __name__ == "__main__":
    importer = Neo4jImporter()
    importer.import_data()
    importer.close()
