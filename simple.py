

def uniq(seq):
    s = set()
    for i in seq:
        if not i in s:
            yield i
            s.add(i)
from keyvalue.cache import DBFailure


class SimpleKV(object):
    trap_exceptions = False

    def get_many(self, keys):
        results = {}
        for q in uniq(keys):
            try:
                results[q] = self.get(q)
            except Exception, e:
                if not self.trap_exceptions:
                    raise
                results[q] = DBFailure(e)
        return results

    def put_many(self, pairs):
        for k, v in pairs.iteritems():
            try:
                self.put(k, v)
            except Exception:
                if not self.trap_exceptions:
                    raise
