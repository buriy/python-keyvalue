import cPickle
from os.path import os
import urllib

from decorator import Decorator, NOTFOUND


DEBUG = False

class Cache(Decorator):
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


class FileCache(Cache):
    def __init__(self, db, path, clean=False):
        super(FileCache, self).__init__(db)
        self.path = path
        self.clean = clean

    def check(self, query):
        fn = self.path % self.fn(query)
        if self.clean and os.path.exists(fn):
            if DEBUG: print "Removing", fn
            os.remove(fn)
        if os.path.exists(fn):
            if DEBUG: print "Found", fn
            return cPickle.load(open(fn, 'rb'))
        return NOTFOUND

    def save(self, query, value):
        fn = self.path % self.fn(query)
        cPickle.dump(value, open(fn, 'wb'), protocol=cPickle.HIGHEST_PROTOCOL)


    def fn(self, query):
        query = str(query)
        query = urllib.quote(query, safe="/: \'")
        query = query.replace("/", '-').replace(':', '_').replace(' ', '_').replace("'", '_').decode('utf-8', 'ignore')
        # print "Using FN for caching:", query
        parts = query.split('_')
        r = []
        for p in parts:
            if p.upper() == p.lower():  # upper
                r.append(p)
            elif p.upper() == p:  # upper
                r.append('u' + p)
            elif p.title() == p:  # title
                r.append('t' + p)
            elif p.lower() == p:  # title
                r.append('l' + p)
            else:
                r.append('m' + p)
        return '_'.join(r)
