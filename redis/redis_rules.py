# coding: utf-8


class RulesSetStore(object):
    def __init__(self, db, prefix):
        self.db = db
        self.prefix = prefix

    def get(self, query):
        min_score, max_score, q = query
        data = self.db.zrevrangebyscore(self.prefix + str(query), max_score, min_score, withscores=True)
        return data
