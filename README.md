# Scalable URL Shortener

A sharded URL shortener that demonstrates core distributed-systems ideas in a small, readable codebase.

## Architecture
Client -> Flask gateway (token-bucket rate limiter) -> LRU cache -> consistent-hash ring -> SQLite shards

- **Consistent hashing** (`hash_ring.py`): 100 virtual nodes per shard. Adding a shard remaps only ~1/N of keys, versus ~(N-1)/N with `hash(key) % N`.
- **Sharding** (`app.py`): each short code is routed to one of N SQLite databases by the ring.
- **LRU cache** (`cache.py`): thread-safe read cache in front of the shards; hit ratio is exposed on `/stats`.
- **Rate limiting** (`cache.py`): per-client token bucket returns HTTP 429 when exceeded.

## Run
```bash
pip install -r requirements.txt
python app.py
curl -X POST localhost:5000/shorten -H "Content-Type: application/json" -d '{"url":"https://example.com"}'
curl -i localhost:5000/<code>
curl localhost:5000/stats
```

## Test
```bash
pytest -q
```

## Limitations / future work
Shards are local SQLite files on one machine. Next steps: run shards as separate services, add replication and
failover, and rebalance data when nodes join or leave.
