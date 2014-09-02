class Failure(object):  # base class for all exceptions
    def __repr__(self):
        return "<A FAILURE>"


class Missing(Failure):  # singleton
    def __repr__(self):
        return "<KEY MISSING>"


class DBFailure(Failure):
    def __init__(self, e):
        self.e = e

    def __repr__(self):
        return "<DB FAILURE: %s>" % self.e


MISSING = Missing()


class Decorator(object):
    """
    A base class to fake subclassing using aggregation, which helps
    to make an instance-based "inheritance" implementation.
    Copies all methods from "db" *instance* to imitate inheritance,
    however it doesn't enforce class inheritance syntax,
    so you may subclass YourDecorator from Decorator,
    add your methods and then write decorated_db = YourDecorator(db)
    and have all methods of both YourDecorator and original db accessible.
    """
    def __init__(self, db):
        self.db = db

        for name in dir(db):
            if callable(getattr(db, name)):
                if not hasattr(self, name) and hasattr(db, name):
                    setattr(self, name, getattr(db, name))

    def __repr__(self):
        return "<%s for %r>" % (self.__class__.__name__, self.db)


class KVCache(Decorator):
    """
    A caching wrapper for any KV database.
    It decorates get() and get_many() in the instance
    but leaves other methods intact.

    It caches None and [] values by default.
    If you don't want to do this, change missing parameter
    or subclass and change is_empty function

    """
    def __init__(self, db, cache, missing=MISSING):
        super(KVCache, self).__init__(db)
        self.cache = cache
        self.missing = missing

    def is_empty(self, value):
        return isinstance(value, Failure) or (value is self.missing)

    def get(self, key):
        value = self.cache.get(key)
        if not self.is_empty(value):
            print "Cache %r has value for the key %s" % (self.cache, key)
            return value
        value = self.db.get(key)
        if self.is_empty(value):
            self.cache.delete(key)
        else:
            self.cache.put(key, value)
        return value

    def get_many(self, keys):
        results = {}
        misses = []
        cached = self.cache.get_many(keys)
        for key, value in cached.iteritems():
            if self.is_empty(value):
                misses.append(key)
            else:
                results[key] = value

        pairs = self.db.get_many(misses)

        to_cache = {}
        for k, v in pairs.iteritems():
            results[k] = v
            if not self.is_empty(v):
                to_cache[k] = v

        self.cache.put_many(to_cache)
        return results

    def __repr__(self):
        return "<%s: %r with %s>" % (self.__class__.__name__, self.cache, self.db)
