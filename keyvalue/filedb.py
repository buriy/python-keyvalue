import cPickle
from keyvalue.cache import MISSING, Failure
from keyvalue.simple import SimpleKV
import os
import time
import urllib


class FileDB(SimpleKV):
    """
    A poor man's local database.
    Saves entries into a directory.
    Path should contain a "%s" which will be replaced with the entry name,
    for example: e.g. cache/articles/%s.pickle
    Autocreates cache directories on initialization.
    Supports versioning: not-matching versions are discarded.
    Be careful to work around duplicates when using symbols
    "/", "_", "-", ":","'" which all are treated the same
    Returns special "MISSING" value if the key is not found, allowing to store None.
    """
    def __init__(self, path, version=1):
        self.path = path
        self.version = version
        dir = os.path.dirname(path)
        if not os.path.exists(dir) and not '%s' in dir:
            os.makedirs(dir, 0755)

    def get(self, key):
        fn = self.path % self._fn(key)
        if os.path.exists(fn):
            ver, val = cPickle.load(open(fn, 'rb'))
            if ver == self.version:
                return val
            else:
                # FIXME: do "upgrade" for older data
                # FIXME: and raise exception if newer version of data has been found
                os.remove(fn)  # remove incompatible version of the data
            #print "FileDB: Reading", key, 'as a', type(val)
        return MISSING

    def put(self, key, value):
        fn = self.path % self._fn(key)
        print "FileDB: Saving", key, 'as a', type(value)
        cPickle.dump((self.version, value), open(fn, 'wb'), protocol=cPickle.HIGHEST_PROTOCOL)

    def delete(self, key):
        fn = self.path % self._fn(key)
        if os.path.exists(fn):
            print "FileDB: Removing", key
            os.remove(fn)

    def _fn(self, query):
        if not isinstance(query, (str, unicode)):
            query = unicode(query)
        if not isinstance(query, str):
            query = query.encode('utf-8', 'ignore')
        query = urllib.quote(query, safe="/: '")
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

    def __repr__(self):
        return "<%s at %r>" % (self.__class__.__name__, self.path)


class TimedFileDB(FileDB):
    def __init__(self, path, version=1, refresh=86400):
        super(TimedFileDB, self).__init__(path, version)
        self.refresh = refresh

    def get(self, key):
        value = super(TimedFileDB, self).get(key)
        if isinstance(value, Failure): # missing
            return value
        load_time, data = value
        delta = time.time() - load_time
        if delta > self.refresh:
            return MISSING
        return data

    def put(self, key, value):
        super(TimedFileDB, self).put(key, (time.time(), value))
