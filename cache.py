class KVFailure(object):
    pass


class KVNotFound(KVFailure):
    def __repr__(self):
        return "<NOTFOUND>"


NOTFOUND = KVNotFound()


class KVReadError(Exception, KVFailure):
    pass


class KVDecorator(object):
    def __init__(self, db):
        self.db = db

        for name in dir(db):
            if callable(getattr(db, name)):
                if not hasattr(self, name) and hasattr(db, name):
                    setattr(self, name, getattr(db, name))

    def __repr__(self):
        return "<%s for %r>" % (self.__class__.__name__, self.db)


class Cache(KVDecorator):
    def __init__(self, db, cache, save_none=False):
        super(Cache, self).__init__(db)
        self.cache = cache
        self.save_none = save_none

    def get(self, key):
        value = self.cache.get(key)
        if isinstance(value, KVFailure) or (value is None and not self.save_none):
            value = self.db.get(key)
            self.cache.put(key, value)
        return value

    def get_many(self, keys):
        results = {}
        misses = []
        cached = self.cache.get_many(keys)
        for key, value in cached.iteritems():
            if isinstance(value, KVFailure) or (value is None and not self.save_none):
                misses.append(key)
            else:
                results[key] = value
        
        pairs = self.db.get_many(misses)
        
        self.cache.put_many(dict((k, v) for k, v in pairs.iteritems() if not isinstance(v, KVFailure)))
        
        for k, v in pairs.iteritems():
            results[k] = v
        return results

    def __repr__(self):
        return "<%s: %r with %s>" % (self.__class__.__name__, self.cache, self.db)
