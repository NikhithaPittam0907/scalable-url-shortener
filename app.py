"""Sharded URL shortener: Flask gateway -> consistent hashing -> SQLite shards."""
import os
import secrets
import sqlite3
import string
import threading

from flask import Flask, jsonify, redirect, request

from cache import LRUCache, TokenBucket
from hash_ring import HashRing

ALPHABET = string.ascii_letters + string.digits


class ShardedStore:
    def __init__(self, data_dir, num_shards=4):
        os.makedirs(data_dir, exist_ok=True)
        self.data_dir = data_dir
        self.ring = HashRing()
        self.conns, self.locks = {}, {}
        for i in range(num_shards):
            self._open_shard(f"shard-{i}")

    def _open_shard(self, name):
        conn = sqlite3.connect(os.path.join(self.data_dir, f"{name}.db"), check_same_thread=False)
        conn.execute("CREATE TABLE IF NOT EXISTS urls (code TEXT PRIMARY KEY, url TEXT NOT NULL)")
        self.conns[name], self.locks[name] = conn, threading.Lock()
        self.ring.add_node(name)

    def put(self, code, url):
        shard = self.ring.get_node(code)
        with self.locks[shard]:
            cur = self.conns[shard].execute("INSERT OR IGNORE INTO urls VALUES (?, ?)", (code, url))
            self.conns[shard].commit()
            return cur.rowcount == 1  # False -> code collision

    def get(self, code):
        shard = self.ring.get_node(code)
        with self.locks[shard]:
            row = self.conns[shard].execute("SELECT url FROM urls WHERE code=?", (code,)).fetchone()
        return row[0] if row else None

    def counts(self):
        return {n: c.execute("SELECT COUNT(*) FROM urls").fetchone()[0] for n, c in self.conns.items()}


def create_app(data_dir="data", num_shards=4, cache_size=1000, rate=5, burst=10):
    app = Flask(__name__)
    store, cache, limiter = ShardedStore(data_dir, num_shards), LRUCache(cache_size), TokenBucket(rate, burst)
    app.store, app.cache = store, cache

    @app.post("/shorten")
    def shorten():
        if not limiter.allow(request.remote_addr):
            return jsonify(error="rate limit exceeded"), 429
        url = (request.get_json(silent=True) or {}).get("url", "")
        if not url.startswith(("http://", "https://")):
            return jsonify(error="url must start with http:// or https://"), 400
        for _ in range(5):  # retry on the rare code collision
            code = "".join(secrets.choice(ALPHABET) for _ in range(7))
            if store.put(code, url):
                return jsonify(code=code, short_url=request.host_url + code), 201
        return jsonify(error="could not allocate code"), 503

    @app.get("/<code>")
    def resolve(code):
        url = cache.get(code)
        if url is None:
            url = store.get(code)
            if url is None:
                return jsonify(error="not found"), 404
            cache.put(code, url)
        return redirect(url, 302)

    @app.get("/stats")
    def stats():
        total = cache.hits + cache.misses
        return jsonify(shards=store.counts(), cache_hits=cache.hits, cache_misses=cache.misses,
                       cache_hit_ratio=round(cache.hits / total, 3) if total else 0.0)

    return app


if __name__ == "__main__":
    create_app().run(port=5000, threaded=True)
