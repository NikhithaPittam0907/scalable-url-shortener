import os, sys, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from collections import Counter
from app import create_app
from cache import LRUCache, TokenBucket
from hash_ring import HashRing


def test_ring_is_balanced():
    ring = HashRing([f"n{i}" for i in range(4)])
    c = Counter(ring.get_node(f"key{i}") for i in range(20000))
    assert max(c.values()) / min(c.values()) < 1.5


def test_adding_node_moves_only_a_fraction_of_keys():
    keys = [f"key{i}" for i in range(10000)]
    ring = HashRing([f"n{i}" for i in range(4)])
    before = {k: ring.get_node(k) for k in keys}
    ring.add_node("n4")
    moved = sum(before[k] != ring.get_node(k) for k in keys) / len(keys)
    assert moved < 0.30  # ~1/5 expected, vs ~80% with hash(key) % N


def test_lru_evicts_oldest():
    c = LRUCache(2); c.put("a", 1); c.put("b", 2); c.get("a"); c.put("c", 3)
    assert c.get("b") is None and c.get("a") == 1


def test_token_bucket_blocks_burst_then_refills():
    tb = TokenBucket(rate=1, capacity=3)
    assert [tb.allow("x", now=0) for _ in range(4)] == [True, True, True, False]
    assert tb.allow("x", now=2)


def test_end_to_end_shorten_resolve_cache_and_rate_limit():
    app = create_app(tempfile.mkdtemp(), num_shards=3, rate=0.0001, burst=50)
    client = app.test_client()
    codes = [client.post("/shorten", json={"url": f"https://example.com/{i}"}).get_json()["code"] for i in range(30)]
    r = client.get(f"/{codes[0]}"); assert r.status_code == 302 and r.headers["Location"] == "https://example.com/0"
    client.get(f"/{codes[0]}")
    s = client.get("/stats").get_json()
    assert s["cache_hits"] == 1 and sum(s["shards"].values()) == 30
    assert client.get("/nope123").status_code == 404
    assert client.post("/shorten", json={"url": "ftp://x"}).status_code == 400
    limited = create_app(tempfile.mkdtemp(), rate=0.0001, burst=2).test_client()
    codes = [limited.post("/shorten", json={"url": "https://a.com"}).status_code for _ in range(3)]
    assert codes == [201, 201, 429]
