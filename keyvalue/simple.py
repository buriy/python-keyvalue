from keyvalue.cache import FailedWithException
from keyvalue.extras import uniq


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
                results[q] = FailedWithException(e)
        return results

    def put_many(self, pairs):
        for k, v in pairs.iteritems():
            try:
                self.put(k, v)
            except Exception:
                if not self.trap_exceptions:
                    raise
