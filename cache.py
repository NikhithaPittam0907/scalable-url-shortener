"""Thread-safe LRU cache and token-bucket rate limiter."""
import threading
import time
from collections import OrderedDict


class LRUCache:
    def __init__(self, capacity=1000):
        self.capacity, self._d, self._lock = capacity, OrderedDict(), threading.Lock()
        self.hits = self.misses = 0

    def get(self, key):
        with self._lock:
            if key in self._d:
                self._d.move_to_end(key)
                self.hits += 1
                return self._d[key]
            self.misses += 1
            return None

    def put(self, key, value):
        with self._lock:
            self._d[key] = value
            self._d.move_to_end(key)
            if len(self._d) > self.capacity:
                self._d.popitem(last=False)


class TokenBucket:
    """Per-client limiter: `rate` tokens/sec, bursts up to `capacity`."""

    def __init__(self, rate=5, capacity=10):
        self.rate, self.capacity = rate, capacity
        self._state, self._lock = {}, threading.Lock()

    def allow(self, client, now=None):
        now = time.monotonic() if now is None else now
        with self._lock:
            tokens, last = self._state.get(client, (self.capacity, now))
            tokens = min(self.capacity, tokens + (now - last) * self.rate)
            if tokens >= 1:
                self._state[client] = (tokens - 1, now)
                return True
            self._state[client] = (tokens, now)
            return False
