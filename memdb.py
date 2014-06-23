from keyvalue.cache import MISSING
from keyvalue.simple import SimpleKV


class MemoryDB(SimpleKV):
    def __init__(self):
        self.cache = {}

    def get(self, query):
        return self.cache.get(query, MISSING)

    def put(self, query, value):
        self.cache[query] = value
