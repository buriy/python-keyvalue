from keyvalue.cache import Decorator, MISSING, Failure
from django.db.models.query import QuerySet


def _iterobjvalues(obj):
    return obj.__dict__.itervalues()
#     for k in dir(obj):
#         v = getattr(obj, k)
#         if not callable(v):
#             yield v


def _iterobjitems(obj):
    return obj.__dict__.iteritems()
#     for k in dir(obj):
#         v = getattr(obj, k)
#         if not callable(v):
#             yield k, v


def itervalues(collection):
    if isinstance(collection, dict):
        return collection.itervalues()
    if isinstance(collection, (list, tuple, QuerySet)):
        return collection
    if hasattr(collection, '__dict__'):
        return _iterobjvalues(collection)
    raise Exception("Unknown kind of collection: %s" % type(collection))


def iteritems(collection):
    if isinstance(collection, dict):
        return collection.iteritems()
    if isinstance(collection, (list, tuple, QuerySet)):
        return enumerate(collection)
    if hasattr(collection, '__dict__'):
        return _iterobjitems(collection)
    raise Exception("Unknown kind of collection: %s" % type(collection))


def uniq(seq):
    s = set()
    for i in seq:
        if not i in s:
            yield i
            s.add(i)


def uniq_not_empty(seq):
    s = set()
    for i in seq:
        if not i in s and not isinstance(i, Failure):
            yield i
            s.add(i)


def dget(obj, key, *default):
    if isinstance(obj, dict):
        return obj.get(key, *default)
    return getattr(obj, key, *default)


def dput(obj, key, value):
    if isinstance(obj, (dict, list)):
        obj[key] = value
    else:
        setattr(obj, key, value)


def dupdate(collection, fmt):
    for k, v in iteritems(collection):
        collection[k] = fmt(v)
    return collection


def dupdate_fmt(data, key, fmt):
    results = {}
    for v in itervalues(data):
        v[key] = fmt(v.get(key, None))
    return results


def djoin(qs, key, name, db, fmt=None):
    keys = [dget(q, key, MISSING) for q in itervalues(qs)]
    print 'keys=', keys
    print 'qs=', qs
    updates = db.get_many(uniq_not_empty(keys))
    if fmt:
        dupdate(updates, fmt)
    #if not updates: return qs
    for q in itervalues(qs):
        fk = dget(q, key, MISSING)
        if isinstance(fk, Failure):
            raise Exception('key "%s" not found in %r' % (key, dict(iteritems(q))))
        print q, name, '=', updates[fk]
        dput(q, name, updates[fk])
    return qs


def dinject(qs, key, db, fmt=None):
    keys = [dget(q, key, MISSING) for q in itervalues(qs)]
    updates = db.get_many(uniq_not_empty(keys))
    if fmt:
        dupdate(updates, fmt)
    if not updates:
        return qs
    for q in itervalues(qs):
        fk = dget(q, key, MISSING)
        if isinstance(fk, Failure):
            raise Exception('key "%s" not found in %r' % (key, dict(iteritems(q))))
        for k, v in updates[fk].iteritems():
            dput(q, k, v)
    return qs


def dinclude(x, *includes):
    r = {}
    for k, v in iteritems(x):
        if k in includes:
            r[k] = v
    return r


def dexclude(x, *excludes):
    r = {}
    for k, v in iteritems(x):
        if not k in excludes:
            r[k] = v
    return r


def dmap_include(data, *includes):
    return dupdate(data, lambda x: dinclude(x, *includes))


def dmap_exclude(data, *excludes):
    return dupdate(data, lambda x: dexclude(x, *excludes))


class Formatter(Decorator):
    def __init__(self, db, format):
        self.db = db
        self.format = format

    def get(self, key):
        return self.format(self.db.get(key))

    def get_many(self, keys):
        return dupdate(self.format, self.db.get_many(keys))
