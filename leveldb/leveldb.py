from newsdb.LevelDBClient import LevelDBPool, LevelDBError

DEBUG = False

class LevelDBStore(object):
    SEPARATOR = ':'
    def __init__(self, path, name):
        self.db = LevelDBPool(path)
        self.name = name
        self.key_prefix = self.name + self.SEPARATOR
        self.key_prefix_len = len(self.key_prefix)

    def EncodeKey(self, key):
        return key

    def DecodeKey(self, key):
        return key

    def EncodeValue(self, value):
        return value

    def DecodeValue(self, value):
        return value

    def WrapKey(self, key):
        return self.key_prefix + self.EncodeKey(key)

    def UnwrapKey(self, key):
        if not key.startswith(self.key_prefix):
            raise LevelDBError, 'Bad key %s does not start with mapping prefix %s' % (key, self.key_prefix)
        return self.DecodeKey(key[self.key_prefix_len:])

    def GetMany(self, keys):
        out = dict()
        for k, v in self.db.GetMany( self.WrapKey(k) for k in keys ).iteritems():
            if v != None:
                v = self.DecodeValue(v)
            out[self.UnwrapKey(k)] = v
        return out
    get_many = GetMany

    def SetMany(self, keys_and_values):
        if isinstance(keys_and_values, dict):
            iterobj = keys_and_values.iteritems()
        else:
            iterobj = iter(keys_and_values)
        self.db.SetMany( (self.WrapKey(k), self.EncodeValue(v)) for (k, v) in iterobj )
    put_many = SetMany

    def DeleteMany(self, keys):
        self.db.DeleteMany(keys)

    def Clear(self):
        self.db.DeleteByPrefix(self.key_prefix)

    def __getitem__(self, key):
        v = self.db.Get(self.WrapKey(key))
        if v == None:
            raise KeyError, key
        return self.DecodeValue(v)

    def __setitem__(self, key, value):
        self.db.Set(self.WrapKey(key), self.EncodeValue(value))
    put = __setitem__

    def __delitem__(self, key):
        self.db.Set(self.WrapKey(key), None)

    def __contains__(self, key):
        v = self.db.Get(self.WrapKey(key))
        return not (v == None)

    def __iter__(self):
        for batch in self.db.GetManyByPrefixBatched((self.key_prefix,), include_values=False, as_dict=False):
            for key in batch:
                yield self.UnwrapKey(key)

    iterkeys = __iter__

    def iteritems(self):
        for batch in self.db.GetManyByPrefixBatched((self.key_prefix,), include_values=True, as_dict=False):
            for key, value in batch:
                yield (self.UnwrapKey(key), self.DecodeValue(value))

    def items(self):
        return list(self.iteritems())

    def as_dict(self):
        return dict(self.iteritems())

    def __len__(self):
        return self.db.CountByPrefix(self.key_prefix)

    def get(self, key, default=None):
        v = self.db.Get(self.WrapKey(key))
        if v == None:
            return default
        else:
            return self.DecodeValue(v)
