from keyvalue.cache import MISSING


class RelModel(object):
    def __init__(self, table, missing=MISSING):
        self.table = table
        self.missing = missing

    def get(self, id):
        try:
            return self.table.objects.get(id=id)
        except self.table.DoesNotExist:
            return self.missing

    def get_many(self, ids):
        r = {}
        for k in ids:
            r[k] = self.missing
        for p in self.table.objects.filter(id__in=ids):
            r[p.id] = p
        return r

    def put(self, id, value):
        value.id = id
        value.save()

    def __repr__(self):
        return "<%s for %r>" % (self.__class__.__name__, self.table)


class RelKeyModel(object):
    def __init__(self, table, field='id', missing=MISSING):
        self.table = table
        self.field = field
        self.missing = missing

    def get(self, id):
        try:
            return self.table.objects.filter(**{self.field: id})
        except self.table.DoesNotExist:
            return self.missing

    def get_many(self, ids):
        r = {}
        for k in ids:
            r[k] = []
        for p in self.table.objects.filter(**{self.field + '__in': ids}):
            key = getattr(p, self.field)
            if r.get(key, self.missing) is self.missing:
                r[key] = [p]
            else:
                r[key].append(p)
        return r

    def __repr__(self):
        return "<%s for %s of %r>" % (self.__class__.__name__, self.field, self.table)
