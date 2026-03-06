// Neo4j : chargement CSV (mettre les fichiers dans le dossier import Neo4j)
CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE;
CREATE CONSTRAINT product_id IF NOT EXISTS FOR (p:Product) REQUIRE p.id IS UNIQUE;

// Users
LOAD CSV WITH HEADERS FROM 'file:///neo4j_users.csv' AS row
MERGE (u:User {id: row.user_id})
SET u.city = row.city,
    u.age_range = row.age_range,
    u.segment = row.segment,
    u.consent_marketing = toInteger(row.consent_marketing),
    u.created_at = datetime(row.created_at);

// Products
LOAD CSV WITH HEADERS FROM 'file:///neo4j_products.csv' AS row
MERGE (p:Product {id: row.product_id})
SET p.name = row.name,
    p.category = row.category,
    p.price = toFloat(row.price),
    p.active = toInteger(row.active);

// FOLLOWS
LOAD CSV WITH HEADERS FROM 'file:///neo4j_follows.csv' AS row
MATCH (a:User {id: row.from_user}), (b:User {id: row.to_user})
MERGE (a)-[:FOLLOWS]->(b);

// VIEWED (count)
LOAD CSV WITH HEADERS FROM 'file:///neo4j_viewed.csv' AS row
MATCH (u:User {id: row.user_id}), (p:Product {id: row.product_id})
MERGE (u)-[r:VIEWED]->(p)
SET r.count = toInteger(row.count);
