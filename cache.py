import os
import time
from joblib import dump, load

class Cache(object):
    ''' Inheritable Object
    '''
    filename = 'cache.joblib'

    @property
    def path(self):
        return '.cache/%s' % self.filename

    def save(self):
        ''' Save class object to cache folder
        '''
        start = time.time()
        dump(self, self.path)
        print 'Cached %s at: %s in %.2fs' % (self, self.path, time.time() - start)

    @classmethod
    def files(cls):
        ''' List available files
        '''
        pass

    @classmethod
    def clear(cls):
        ''' Clear cached files
        '''
        pass

    def iscached(self):
        ''' Determine if there exists a file in the cache folder
        '''
        files = Cache.files()

    @classmethod
    def load(cls, symbol):
        ''' Load the random forest the saved file
        '''
        start = time.time()
        filename = 'forest[%s].joblib' % symbol
        model = load('models/%s' % filename)
        print 'Loaded %s in %.2fs' % (model, time.time() - start)
        return model
