import redis
import time
import random

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# E1 — Sessions (Hash + TTL 30 min)
print("--- E1 : Sessions ---")
for i in range(1, 6):
    sid = f"session:{i}"
    r.hset(sid, mapping={"user_id": f"u{i:03d}", "last_seen": str(time.time())})
    r.expire(sid, 1800)
print(f"5 sessions créées. TTL session:1 = {r.ttl('session:1')}s")
print(f"session:1 → {r.hgetall('session:1')}")

# E2 — Top-N produits vus (Sorted Set)
print("\n--- E2 : Top-N (Vues) ---")
today = time.strftime("%Y-%m-%d")
key_top = f"top:views:{today}"
products = ["iPhone", "Pixel", "Galaxy", "Surface", "MacBook", "iPad"]
for p in products:
    r.zadd(key_top, {p: random.randint(10, 500)})

top_5 = r.zrevrange(key_top, 0, 4, withscores=True)
for rank, (prod, score) in enumerate(top_5, 1):
    print(f"{rank}. {prod} : {int(score)} vues")

# E3 — Visiteurs uniques (Set + SCARD)
print("\n--- E3 : Visiteurs uniques ---")
key_uv = f"uv:{today}"
users = ["user_1", "user_2", "user_1", "user_3", "user_4"]
for u in users:
    r.sadd(key_uv, u)
print(f"Visiteurs uniques : {r.scard(key_uv)}")

# E4 — Panier temporaire (Hash + TTL 1h)
print("\n--- E4 : Panier temporaire ---")
cart_id = "cart:session:1"
r.hset(cart_id, mapping={"prod_A": 2, "prod_B": 1})
r.expire(cart_id, 3600)
print(f"Contenu : {r.hgetall(cart_id)}")

# E5 — Mesure latence : cache vs DB simulée
print("\n--- E5 : Mesure Latence (Cache) ---")
ITERATIONS = 100

def fake_db_call():
    time.sleep(0.01)
    return "data"

start = time.time()
for _ in range(ITERATIONS):
    fake_db_call()
avg_no_cache = (time.time() - start) / ITERATIONS

r.set("bench:key", "data")
start = time.time()
for _ in range(ITERATIONS):
    r.get("bench:key")
avg_cache = (time.time() - start) / ITERATIONS

print(f"Latence SANS cache : {avg_no_cache:.4f}s")
print(f"Latence AVEC cache : {avg_cache:.4f}s")
print(f"Gain : {avg_no_cache / avg_cache:.1f}x")

# E5 bis — Rate Limiting (INCR + EXPIRE par minute)
print("\n--- E5 bis : Rate Limiting ---")

def check_rate_limit(user_id, limit=10):
    current_minute = int(time.time() / 60)
    key = f"rate:limit:{user_id}:{current_minute}"
    count = r.incr(key)
    if count == 1:
        r.expire(key, 60)
    return count <= limit, count

for i in range(12):
    allowed, current = check_rate_limit("user_test")
    status = "OK" if allowed else "BLOQUÉ"
    print(f"Requête {i+1}: {status} (compteur: {current})")

# B1 — Pub/Sub : alerte "stock-low"
print("\n--- B1 : Pub/Sub (stock-low) ---")
import threading

received = []

def subscriber_loop():
    sub = redis.Redis(host='localhost', port=6379, decode_responses=True)
    ps = sub.pubsub()
    ps.subscribe("stock-low")
    for msg in ps.listen():
        if msg["type"] == "message":
            received.append(msg["data"])
            break
    ps.unsubscribe()

t = threading.Thread(target=subscriber_loop, daemon=True)
t.start()
time.sleep(0.2)

r.publish("stock-low", "p042:stock=2")
t.join(timeout=2)

if received:
    print(f"Message reçu sur 'stock-low' : {received[0]}")
else:
    print("Aucun message reçu (timeout)")

# B2 — Lua script atomique (INCR conditionnel)
print("\n--- B2 : Lua Script Atomique ---")
lua_script = """
local current = tonumber(redis.call('GET', KEYS[1]) or 0)
if current < tonumber(ARGV[1]) then
    return redis.call('INCR', KEYS[1])
else
    return -1
end
"""
incr_if_below = r.register_script(lua_script)

r.set("lua:counter", 8)
result1 = incr_if_below(keys=["lua:counter"], args=[10])
result2 = incr_if_below(keys=["lua:counter"], args=[10])
result3 = incr_if_below(keys=["lua:counter"], args=[10])
print(f"INCR si < 10 : {result1} → {result2} → {result3} (10 atteint = -1)")

print("\n✅ TP REDIS TERMINÉ")
