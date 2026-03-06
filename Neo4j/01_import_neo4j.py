import pandas as pd
from neo4j import GraphDatabase
import os
import sys

# Configuration
URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "platon01"

class Neo4jImporter:
    def __init__(self):
        try:
            self.driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
            self.driver.verify_connectivity()
        except Exception as e:
            print(f"❌ ERREUR CONNEXION : {e}")
            sys.exit(1)

    def close(self):
        self.driver.close()

    def run_query(self, query, params=None):
        with self.driver.session() as session:
            session.run(query, params)

    def import_data(self):
        # 1. USERS
        df_users = pd.read_csv('neo4j_users.csv')
        u_col = 'user_id' if 'user_id' in df_users.columns else 'id'

        print(f"🧹 Nettoyage et Import de {len(df_users)} utilisateurs...")
        self.run_query("MATCH (n) DETACH DELETE n")

        for _, r in df_users.iterrows():
            self.run_query("CREATE (:User {id: $id, city: $city, age: $age})",
                          {"id": str(r[u_col]), "city": r['city'], "age": r['age_range']})

        # 2. PRODUCTS
        df_prods = pd.read_csv('neo4j_products.csv')
        p_col = 'product_id' if 'product_id' in df_prods.columns else 'id'
        print(f"📥 Import de {len(df_prods)} produits...")
        for _, r in df_prods.iterrows():
            # Correction : on utilise toFloatingPoint() pour le prix
            self.run_query("CREATE (:Product {id: $id, category: $cat, price: toFloatingPoint($price)})",
                          {"id": str(r[p_col]), "cat": r['category'], "price": r['price']})

        # 3. RELATIONS FOLLOWS
        if os.path.exists('neo4j_follows.csv'):
            df_fol = pd.read_csv('neo4j_follows.csv')
            print(f"🔗 Création de {len(df_fol)} relations FOLLOWS...")
            for _, r in df_fol.iterrows():
                self.run_query("""
                    MATCH (u1:User {id: $id1}), (u2:User {id: $id2})
                    MERGE (u1)-[:FOLLOWS]->(u2)
                """, {"id1": str(r['follower_id']), "id2": str(r['followed_id'])})

        # 4. RELATIONS VIEWED
        if os.path.exists('neo4j_viewed.csv'):
            df_view = pd.read_csv('neo4j_viewed.csv')
            print(f"🔗 Création de {len(df_view)} relations VIEWED...")
            for _, r in df_view.iterrows():
                self.run_query("""
                    MATCH (u:User {id: $uid}), (p:Product {id: $pid})
                    MERGE (u)-[:VIEWED {ts: $ts}]->(p)
                """, {"uid": str(r['user_id']), "pid": str(r['product_id']), "ts": r['ts']})

        print("\n✨ IMPORTATION RÉUSSIE ! Le graphe est complet.")

if __name__ == "__main__":
    importer = Neo4jImporter()
    importer.import_data()
    importer.close()
