class RedisCache(Cache):
    def __init__(self, db, prefix, clean=False):
        super(RedisCache, self).__init__(db)
        self.prefix = prefix
        self.clean = clean

    def get(self, query):
        value = self.check(query)
        if value is NOTFOUND:
            value = self.db.get(query)
            self.save(query, value)
        return value

    def get_many(self, queries):
        results = {}
        misses = []
        for q in queries:
            v = self.check(q)
            if v is NOTFOUND:
                misses.append(q)
            else:
                results[q] = v
        for k, v in self.db.get_many(queries).iteritems():
            self.save(k, v)
            results[k] = v
        return results

    def __repr__(self):
        return "<%s for %r>" % (self.__class__.__name__, self.db)
