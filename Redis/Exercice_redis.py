import redis
import time
import random

# Connexion à Redis (adapter si besoin, par défaut localhost:6379)
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

print("--- E1 : Sessions ---")
for i in range(1, 6):
    session_id = f"session:{i}"
    r.set(session_id, "active", ex=1800) # TTL 30 min (1800s)
    r.set(f"{session_id}:last_seen", time.time()) # Mise à jour last_seen
print(f"5 sessions créées. TTL session:1 : {r.ttl('session:1')}s")

print("\n--- E2 : Top-N (Vues) ---")
today = "2026-03-05"
key_top = f"top:views:{today}"
products = ["iPhone", "Pixel", "Galaxy", "Surface", "MacBook", "iPad"]
for p in products:
    r.zadd(key_top, {p: random.randint(10, 500)})

top_5 = r.zrevrange(key_top, 0, 4, withscores=True)
for rank, (prod, score) in enumerate(top_5, 1):
    print(f"{rank}. {prod} : {score} vues")

print("\n--- E3 : Visiteurs uniques ---")
key_uv = f"uv:{today}"
users = ["user_1", "user_2", "user_1", "user_3", "user_4"] # Note le doublon user_1
for u in users:
    r.sadd(key_uv, u)
print(f"Nombre de visiteurs uniques : {r.scard(key_uv)}")

print("\n--- E4 : Panier temporaire ---")
cart_id = "cart:session:1"
r.hset(cart_id, mapping={"prod_A": 2, "prod_B": 1})
r.expire(cart_id, 3600) # Expire dans 1h
print(f"Contenu du panier : {r.hgetall(cart_id)}")

print("\n--- E5 : Mesure Latence (Cache) ---")
def fake_db_call():
    time.sleep(0.1) # Simule une DB lente (100ms)
    return "data"

latencies_no_cache = []
latencies_cache = []

# Test sans cache
start = time.time()
for _ in range(10): # Réduit à 10 pour l'exemple
    fake_db_call()
latencies_no_cache = (time.time() - start) / 10

# Test avec cache
r.set("my_key", "data")
start = time.time()
for _ in range(10):
    r.get("my_key")
latencies_cache = (time.time() - start) / 10

print(f"Latence moyenne SANS cache : {latencies_no_cache:.4f}s")
print(f"Latence moyenne AVEC cache : {latencies_cache:.4f}s")
print(f"Gain : {latencies_no_cache / latencies_cache:.1f}x plus rapide")

def check_rate_limit(user_id, limit=10):
    # On crée une clé unique par utilisateur et par minute
    current_minute = int(time.time() / 60)
    key = f"rate:limit:{user_id}:{current_minute}"

    count = r.incr(key)

    if count == 1:
        # Première requête de la minute, on met un TTL de 60s
        r.expire(key, 60)

    if count > limit:
        return False, count
    return True, count

# Test du rate limit
user_test = "user_mathis"
for i in range(12):
    allowed, current = check_rate_limit(user_test)
    if allowed:
        print(f"Requête {i+1}: OK (Compteur: {current})")
    else:
        print(f"Requête {i+1}: BLOQUÉ ! (Trop de requêtes: {current})")
