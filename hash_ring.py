"""Consistent hash ring with virtual nodes."""
import bisect
import hashlib


def _h(key: str) -> int:
    return int(hashlib.md5(key.encode()).hexdigest(), 16)


class HashRing:
    def __init__(self, nodes=None, vnodes=100):
        self.vnodes = vnodes
        self._ring = []   # sorted hash positions
        self._owner = {}  # position -> node
        for n in nodes or []:
            self.add_node(n)

    def add_node(self, node):
        for i in range(self.vnodes):
            pos = _h(f"{node}#{i}")
            self._owner[pos] = node
            bisect.insort(self._ring, pos)

    def remove_node(self, node):
        for i in range(self.vnodes):
            pos = _h(f"{node}#{i}")
            self._owner.pop(pos, None)
            self._ring.remove(pos)

    def get_node(self, key):
        if not self._ring:
            raise ValueError("ring is empty")
        idx = bisect.bisect(self._ring, _h(key)) % len(self._ring)
        return self._owner[self._ring[idx]]
