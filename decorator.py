class NOTFOUND: pass
class ERROR(Exception): pass

class Decorator(object):
    def __init__(self, db):
        self.db = db

        for name in dir(db):
            if callable(getattr(db, name)):
                if not hasattr(self, name) and hasattr(db, name):
                    setattr(self, name, getattr(db, name))
