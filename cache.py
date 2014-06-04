class ERROR(Exception):
    pass


class NOTFOUND:
    pass


class Decorator(object):
    def __init__(self, db):
        self.db = db

        for name in dir(db):
            if callable(getattr(db, name)):
                if not hasattr(self, name) and hasattr(db, name):
                    setattr(self, name, getattr(db, name))

    def __repr__(self):
        return "<%s for %r>" % (self.__class__.__name__, self.db)


class Cache(Decorator):
    def __init__(self, db, cache):
        super(Cache, self).__init__(db)
        self.cache = cache

    def get(self, key):
        value = self.cache.get(key)
        if value is NOTFOUND or value is ERROR:
            value = self.db.get(key)
            self.cache.put(key, value)
        return value

    def get_many(self, keys):
        results = {}
        misses = []
        for key, value in self.cache.get_many(keys).iteritems():
            if value is NOTFOUND or value is ERROR:
                misses.append(key)
            else:
                results[key] = value

        pairs = self.db.get_many(keys)
        self.cache.put_many(pairs)
        for k, v in pairs.iteritems():
            results[k] = v
        return results

    def __repr__(self):
        return "<%s: %r with %s>" % (self.__class__.__name__, self.cache, self.db)
