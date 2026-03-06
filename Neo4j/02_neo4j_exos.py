from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "platon01"

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))


def query(cypher, params=None):
    with driver.session() as session:
        return list(session.run(cypher, params or {}))


def run_exercise(name, func):
    print(f"\n--- {name} ---")
    try:
        func()
    except Exception as e:
        print(f"❌ Erreur : {e}")


# E1 — Contraintes + vérification du graphe
def e1():
    constraints = query("SHOW CONSTRAINTS")
    for c in constraints:
        print(f"  {c['name']} → {c['labelsOrTypes']}")

    stats = query(
        "MATCH (u:User) WITH count(u) AS users "
        "MATCH (p:Product) WITH users, count(p) AS products "
        "MATCH ()-[r]->() RETURN users, products, count(r) AS relations"
    )[0]
    print(f"Graphe : {stats['users']} users, {stats['products']} produits, {stats['relations']} relations")

run_exercise("E1 : Contraintes & structure du graphe", e1)

# E2 — Top 5 produits par VIEWED et par BOUGHT
def e2():
    print("Top 5 vus :")
    for r in query("""
        MATCH (:User)-[v:VIEWED]->(p:Product)
        RETURN p.id AS pid, p.category AS cat, sum(v.count) AS views
        ORDER BY views DESC LIMIT 5
    """):
        print(f"  {r['pid']} ({r['cat']}) : {r['views']} vues")

    print("Top 5 achetés :")
    for r in query("""
        MATCH (:User)-[b:BOUGHT]->(p:Product)
        RETURN p.id AS pid, p.category AS cat, count(b) AS buys
        ORDER BY buys DESC LIMIT 5
    """):
        print(f"  {r['pid']} ({r['cat']}) : {r['buys']} achats")

run_exercise("E2 : Top 5 produits (vus + achetés)", e2)

# E3 — Co-achat : "souvent achetés ensemble"
def e3():
    results = query("""
        MATCH (u:User)-[:BOUGHT]->(p1:Product {id: $pid})
        MATCH (u)-[:BOUGHT]->(p2:Product)
        WHERE p2.id <> p1.id
        RETURN p2.id AS pid, p2.category AS cat, count(*) AS score
        ORDER BY score DESC LIMIT 5
    """, {"pid": "p076"})

    if not results:
        print("  Aucun co-achat pour p076, fallback global :")
        results = query("""
            MATCH (u:User)-[:BOUGHT]->(p1:Product)
            MATCH (u)-[:BOUGHT]->(p2:Product)
            WHERE p2.id <> p1.id
            RETURN p1.id AS source, p2.id AS pid, count(*) AS score
            ORDER BY score DESC LIMIT 5
        """)
        for r in results:
            print(f"  {r['source']} ↔ {r['pid']} : score {r['score']}")
    else:
        for r in results:
            print(f"  {r['pid']} ({r['cat']}) : score {r['score']}")

run_exercise("E3 : Co-achat (souvent achetés ensemble)", e3)

# E4 — Reco voisins : FOLLOWS → BOUGHT (produits non achetés)
def e4():
    for uid in ["u001", "u003"]:
        results = query("""
            MATCH (me:User {id: $uid})-[:FOLLOWS]->(friend:User)-[:BOUGHT]->(p:Product)
            WHERE NOT (me)-[:BOUGHT]->(p)
            RETURN p.id AS pid, p.category AS cat, p.price AS price, count(*) AS score
            ORDER BY score DESC LIMIT 5
        """, {"uid": uid})

        if results:
            print(f"  {uid} →", ", ".join(f"{r['pid']} ({r['cat']}, {r['price']}€)" for r in results))
        else:
            print(f"  {uid} → aucune reco (pas d'amis acheteurs)")

run_exercise("E4 : Reco voisins (FOLLOWS → BOUGHT)", e4)

# E5 — Script de recommandation hybride
def recommend(user_id, limit=5):
    reco_friends = query("""
        MATCH (me:User {id: $uid})-[:FOLLOWS]->(f:User)-[:BOUGHT]->(p:Product)
        WHERE NOT (me)-[:BOUGHT]->(p)
        RETURN p.id AS pid, p.category AS cat, p.price AS price, 'voisin' AS source, count(*) AS score
        ORDER BY score DESC LIMIT $lim
    """, {"uid": user_id, "lim": limit})

    reco_cobuy = query("""
        MATCH (me:User {id: $uid})-[:BOUGHT]->(myp:Product)<-[:BOUGHT]-(other:User)-[:BOUGHT]->(p:Product)
        WHERE NOT (me)-[:BOUGHT]->(p) AND p.id <> myp.id
        RETURN DISTINCT p.id AS pid, p.category AS cat, p.price AS price, 'co-achat' AS source, count(*) AS score
        ORDER BY score DESC LIMIT $lim
    """, {"uid": user_id, "lim": limit})

    combined = {}
    for r in reco_friends + reco_cobuy:
        pid = r["pid"]
        if pid not in combined or r["score"] > combined[pid]["score"]:
            combined[pid] = dict(r)
    return sorted(combined.values(), key=lambda x: x["score"], reverse=True)[:limit]


def e5():
    for uid in ["u001", "u003", "u020"]:
        recos = recommend(uid)
        if recos:
            print(f"  {uid} →", ", ".join(f"{r['pid']}({r['source']}, score={r['score']})" for r in recos))
        else:
            print(f"  {uid} → aucune reco")

run_exercise("E5 : Recommandation hybride (voisins + co-achat)", e5)

# B1 — shortestPath entre deux users
def b1():
    paths = query("""
        MATCH path = shortestPath((a:User {id: 'u001'})-[*..6]-(b:User {id: 'u020'}))
        RETURN [n IN nodes(path) | CASE WHEN n:User THEN n.id WHEN n:Product THEN n.id ELSE '' END] AS hops,
               length(path) AS dist
    """)
    if paths:
        print(f"  u001 → u020 : {paths[0]['dist']} hops via {' → '.join(paths[0]['hops'])}")
    else:
        print("  Aucun chemin trouvé")

run_exercise("B1 : Plus court chemin (shortestPath)", b1)

# B2 — Score combiné (views×1 + buys×5)
def b2():
    results = query("""
        MATCH (p:Product)
        OPTIONAL MATCH (:User)-[v:VIEWED]->(p)
        WITH p, coalesce(sum(v.count), 0) AS views
        OPTIONAL MATCH (:User)-[b:BOUGHT]->(p)
        WITH p, views, count(b) AS buys
        WITH p, views + buys * 5 AS score, views, buys
        WHERE score > 0
        RETURN p.id AS pid, p.category AS cat, views, buys, score
        ORDER BY score DESC LIMIT 5
    """)
    for r in results:
        print(f"  {r['pid']} ({r['cat']}) : {r['views']} vues + {r['buys']} achats = score {r['score']}")

run_exercise("B2 : Score combiné (vues×1 + achats×5)", b2)

# B3 — Détection de hubs (degree centrality)
def b3():
    results = query("""
        MATCH (u:User)
        OPTIONAL MATCH (u)-[out]->()
        WITH u, count(out) AS out_degree
        OPTIONAL MATCH (u)<-[inc]-()
        WITH u, out_degree, count(inc) AS in_degree
        WITH u, out_degree + in_degree AS total_degree, out_degree, in_degree
        ORDER BY total_degree DESC LIMIT 5
        RETURN u.id AS uid, u.city AS city, out_degree, in_degree, total_degree
    """)
    for r in results:
        print(f"  {r['uid']} ({r['city']}) : {r['total_degree']} connexions (out={r['out_degree']}, in={r['in_degree']})")

run_exercise("B3 : Hubs (degree centrality)", b3)

driver.close()
print("\n✅ TP NEO4J TERMINÉ")
