import concurrent.futures as futures
from keyvalue.cache import Decorator, DBFailure

def uniq(seq):
    s = set()
    for i in seq:
        if not i in s:
            yield i
            s.add(i)
import sys


def parallelize(func, queries, max_workers=5, timeout=None, trap_exceptions=False):
    # copied from example at https://code.google.com/p/pythonfutures/
    results = {}
    with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        tasks = dict((executor.submit(func, q), q) for q in uniq(queries))

        for future in futures.as_completed(tasks, timeout=timeout):
            q = tasks[future]
            if future.exception() is not None:
                if not trap_exceptions:
                    raise future.exception(), None, sys.exc_info()[2]
                results[q] = DBFailure(future.exception())
            else:
                results[q] = future.result()
    return results


class ParallelReader(Decorator):
    TRAP_EXCEPTIONS = False

    def __init__(self, db, workers=5):
        super(ParallelReader, self).__init__(db)
        self.workers = workers

    def get_many(self, keys):
        return parallelize(self.get, keys, max_workers=self.workers,
                           trap_exceptions=self.TRAP_EXCEPTIONS)


class ParallelWriter(Decorator):
    TRAP_EXCEPTIONS = False

    def __init__(self, db, workers=5):
        super(ParallelWriter, self).__init__(db)
        self.workers = workers

    def put_many(self, items):
        items = {}
        return parallelize(lambda (x, y): self.put(x, y), items.iteritems(),
                           max_workers=self.workers, trap_exceptions=self.TRAP_EXCEPTIONS)
