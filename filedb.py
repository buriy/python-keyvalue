import cPickle
import os
import urllib

from keyvalue.cache import NOTFOUND
from keyvalue.simple import SimpleKV


class FileDB(SimpleKV):
    def __init__(self, path, clean=False, missing=NOTFOUND):
        self.path = path
        self.clean = clean
        self.missing = missing

    def get(self, key):
        fn = self.path % self._fn(key)
        if self.clean and os.path.exists(fn):
            os.remove(fn)
        if os.path.exists(fn):
            return cPickle.load(open(fn, 'rb'))
        return self.missing

    def put(self, key, value):
        fn = self.path % self._fn(key)
        if value is self.missing:
            os.remove(fn)
        else:
            cPickle.dump(value, open(fn, 'wb'), protocol=cPickle.HIGHEST_PROTOCOL)

    def _fn(self, query):
        if not isinstance(query, (str, unicode)):
            query = unicode(query)
        if not isinstance(query, str):
            query = query.encode('utf-8', 'ignore')
        query = urllib.quote(query, safe="/: \'")
        query = query.replace("/", '_').replace(':', '_').replace(' ', '_').replace("'", '_')
        query = query.encode('utf-8')
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
