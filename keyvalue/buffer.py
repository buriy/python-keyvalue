from keyvalue.cache import Decorator


class BufferedWriter(Decorator):
    def __init__(self, db, group_size=2000):
        super(BufferedWriter, self).__init__(db)
        self.cache = {}
        self.group_size = group_size

    def put(self, key, value):
        self.cache[key] = value
        if len(self.cache) >= self.group_size:
            self.sync()

    def sync(self):
        if self.cache:
            self.db.put_many(self.cache.iteritems())
            self.cache = {}

    def __del__(self):
        self.sync()
