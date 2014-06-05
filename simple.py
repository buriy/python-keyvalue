from keyvalue.cache import KVReadError


def uniq(seq):
    s = set()
    for i in seq:
        if not i in s:
            yield i
            s.add(i)


class SimpleKV(object):
    trap_exceptions = True

    def get_many(self, keys):
        results = {}
        for q in uniq(keys):
            try:
                results[q] = self.get(q)
            except Exception, e:
                if not self.trap_exceptions:
                    raise
                results[q] = KVReadError(e)
        return results

    def put_many(self, pairs):
        for k, v in pairs.iteritems():
            try:
                self.put(k, v)
            except Exception:
                if not self.trap_exceptions:
                    raise
