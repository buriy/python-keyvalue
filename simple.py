from keyvalue.cache import ERROR


class SimpleKV(object):
    trap_exceptions = False
        

    def get_many(self, keys):
        results = {}
        for q in keys:
            try:
                results[q] = self.get(q)
            except Exception, e:
                if not self.trap_exceptions:
                    raise
                results[q] = ERROR(e)
        return results


    def put_many(self, pairs):
        for k,v in pairs.iteritems():
            try:
                self.put(k, v)
            except Exception, _e:
                if not self.trap_exceptions:
                    raise
