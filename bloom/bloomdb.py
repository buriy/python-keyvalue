import math
import os
import warnings

import bloomers

from cache import Cache, NOTFOUND


#import atexit
#atexit.register(self.sync)
def bloomParameters(number_of_items, acceptable_false_positive_log2 = 20):
    n = number_of_items
    bits = 19 # smallest sane size
    while 1:
        # Calculate optimal number of hashes according to http://en.wikipedia.org/wiki/Bloom_filter
        m = 1 << bits
        k = int((float(m) / float(n)) * math.log(2.0))
        if k >= acceptable_false_positive_log2: # chance of a false positive below 1 in a million?
            break
        else:
            bits += 1
    if bits >= 32:
        warnings.warn("This bloom filters implementation is limited with 32 bit hashes")
        raise Exception("This bloom filters implementation is limited with 32 bit hashes")
    return bits, k # (number of bits per hash, number of hash functions)

def BloomDB(filename, num_items, fp2 = 20):
    if os.path.exists(filename) and False:
        bf = bloomers.open(filename, True)
    else:
        bf = bloomers.create(filename, *bloomParameters(num_items, fp2))
    return bf


class BloomFilter(Cache):
    def __init__(self, db, filename, opts):
        self.db = db
        self.filename = filename
        self.bloom = BloomDB(filename, *opts)

    def check(self, query):
        if not self.bloom.contains(query):
            return None
        return NOTFOUND

    def save(self, query, value):
        pass
