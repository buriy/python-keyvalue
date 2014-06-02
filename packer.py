from cPickle import HIGHEST_PROTOCOL, loads, dumps
from zlib import decompress, compress

from keyvalue.cache import Decorator


class Packer(Decorator):
    def get(self, query):
        val = self.db.get(query)
        if val is not None:
            val = loads(decompress(val))
        return val


    def get_many(self, keys):
        items = self.db.get_many(keys)
        for key in items.keys():
            val = items[key]
            if val is not None:
                items[key] = loads(decompress(val))
        return items


    def put(self, key, val):
        if val is not None:
            val = compress(dumps(val, protocol=HIGHEST_PROTOCOL))
        self.db.put(key, val)
        return val


    def put_many(self, queries):
        queries = queries.copy()
        for key in queries.keys():
            val = queries[key]
            if val is not None:
                queries[key] = compress(dumps(val, protocol=HIGHEST_PROTOCOL))
        self.db.put_many(queries)
