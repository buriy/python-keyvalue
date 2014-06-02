from keyvalue.cache import NOTFOUND
from keyvalue.simple import SimpleKV

class MemoryDB(SimpleKV):
    def __init__(self):
        self.cache = {}

    def get(self, query):
        return self.cache.get(query, NOTFOUND)

    def put(self, query, value):
        self.cache[query] = value
