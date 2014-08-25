# coding: utf-8


class RedisTimedStore(object):
    def __init__(self, db, prefix, timeout=None):
        self.db = db
        self.prefix = prefix
        self.timeout = timeout  # in seconds

    def get(self, query):
        return self.db.get(self.prefix + query)

    def get_many(self, queries):
        if not queries:
            return {}
        return zip(queries, self.db.mget([self.prefix + q for q in queries]))

    def put(self, query):
        self.db.setex(self.prefix + query, ex=self.timeout)

    def put_many(self, queries):
        if not queries:
            return
        #_p = self.db.pipeline()
        #FIXME


class RedisHStore(object):
    def __init__(self, db, prefix):
        self.db = db
        self.prefix = prefix

    def get(self, query):
        return self.db.hget(self.prefix, query)

    def get_many(self, queries):
        if not queries:
            return {}
        return zip(queries, self.db.hmget(self.prefix, queries))

    def put(self, query):
        self.db.hput(self.prefix, query)

    def put_many(self, queries):
        if queries:
            self.db.hmset(self.prefix, queries)


class RedisZStore(object):
    def __init__(self, db, prefix, max_score=1e14, min_score=10):
        self.db = db
        self.prefix = prefix
        self.max_score = max_score
        self.min_score = min_score

    def get(self, query):
        v = self.db.zscore(self.prefix + query)
        if v is None or v < self.min_score:
            return None
        return v

    def put(self, query):
        v = self.db.zscore(self.prefix + query)
        if v is None or v < self.min_score:
            return None
        return v
