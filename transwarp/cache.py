#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Michael Liao'

'''
A simple cache interface.
'''

import os, time, datetime, functools, logging

try:
    import cPickle as pickle
except ImportError:
    import pickle

class DummyClient(object):

    def set(self, key, value, expires=0):
        pass

    def setint(self, key, value, expires=0):
        pass

    def get(self, key, default=None):
        return default

    def gets(self, *keys):
        return [None] * len(keys)

    def getint(self, key, default=0):
        return default

    def getints(self, *keys):
        return [0] * len(keys)

    def delete(self, key):
        pass

    def incr(self, key):
        pass

    def decr(self, key):
        pass

class MemcacheClient(object):

    def __init__(self, servers, debug=False):
        import memcache
        if isinstance(servers, basestring):
            servers = [servers]
        self._client = memcache.Client(servers, debug)

    def set(self, key, value, expires=0):
        '''
        Set object with key.

        Args:
            key: cache key as str.
            value: object value.
            expires: cache time, default to 0 (using default expires time)

        >>> key = uuid.uuid4().hex
        >>> c = MemcacheClient('localhost:11211')
        >>> c.set(key, u'Python\u4e2d\u6587')
        >>> c.get(key)
        u'Python\u4e2d\u6587'
        >>> c.set(key, [1, 2, 3, 4, 5])
        >>> c.get(key)
        [1, 2, 3, 4, 5]
        >>> c.set(key, 'Expires after 1 sec', 1)
        >>> c.get(key)
        'Expires after 1 sec'
        >>> time.sleep(2)
        >>> c.get(key, 'Not Exist')
        'Not Exist'
        '''
        self._client.set(key, value, expires)

    setint = set

    def get(self, key, default=None):
        '''
        Get object by key.

        Args:
            key: cache key as str.
            default: default value if key not found. default to None.
        Returns:
            object or default value if not found.

        >>> key = uuid.uuid4().hex
        >>> c = MemcacheClient('localhost:11211')
        >>> c.get(key)
        >>> c.get(key, 'DEFAULT_MC')
        'DEFAULT_MC'
        >>> c.set(key, 'hello, mc')
        >>> c.get(key)
        'hello, mc'
        '''
        r = self._client.get(key)
        return default if r is None else r

    def getint(self, key, default=0):
        '''
        Get int.

        >>> key = uuid.uuid4().hex
        >>> c = MemcacheClient('localhost:11211')
        >>> c.getint(key)
        0
        >>> c.setint(key, 101)
        >>> c.getint(key)
        101
        '''
        r = self._client.get(key)
        return _safe_int(r, default)

    def gets(self, *keys):
        '''
        Get objects by keys.

        Args:
            keys: cache keys as str.
        Returns:
            list of object.

        >>> key1 = uuid.uuid4().hex
        >>> key2 = uuid.uuid4().hex
        >>> key3 = uuid.uuid4().hex
        >>> c = MemcacheClient('localhost:11211')
        >>> c.gets(key1, key2, key3)
        [None, None, None]
        >>> c.set(key1, 'Key1')
        >>> c.set(key3, 'Key3')
        >>> c.gets(key1, key2, key3)
        ['Key1', None, 'Key3']
        '''
        r = self._client.get_multi(keys)
        return map(lambda k: r.get(k), keys)

    def getints(self, *keys):
        '''
        Get ints by keys.

        >>> key1 = uuid.uuid4().hex
        >>> key2 = uuid.uuid4().hex
        >>> key3 = uuid.uuid4().hex
        >>> c = MemcacheClient('localhost:11211')
        >>> c.getints(key1, key2, key3)
        [0, 0, 0]
        >>> c.setint(key1, 11)
        >>> c.setint(key3, -99)
        >>> c.getints(key1, key2, key3)
        [11, 0, -99]
        '''
        r = self._client.get_multi(keys)
        return map(lambda k: _safe_int(r.get(k)), keys)

    def delete(self, key):
        '''
        Delete object from cache by key.

        Args:
            key: cache key as str.

        >>> key = uuid.uuid4().hex
        >>> c = MemcacheClient('localhost:11211')
        >>> c.set(key, 'delete from mc')
        >>> c.get(key)
        'delete from mc'
        >>> c.delete(key)
        >>> c.get(key)
        '''
        self._client.delete(key)

    def incr(self, key):
        '''
        Increase counter.

        Args:
            key: cache key as str.

        >>> key = uuid.uuid4().hex
        >>> c = MemcacheClient('localhost:11211')
        >>> c.incr(key)
        1
        >>> c.incr(key)
        2
        >>> c.set(key, 100)
        >>> c.incr(key)
        101
        '''
        r = self._client.incr(key)
        if r is None:
            self._client.set(key, 1)
            r = 1
        return r

    def decr(self, key):
        '''
        Decrease counter. NOTE the memcache does not allow negative number, 
        so decr key = 0 will still return 0

        Args:
            key: cache key as str.

        >>> key = uuid.uuid4().hex
        >>> c = MemcacheClient('localhost:11211')
        >>> c.decr(key)
        0
        >>> c.decr(key)
        0
        >>> c.set(key, 100)
        >>> c.decr(key)
        99
        '''
        r = self._client.decr(key)
        if r is None:
            self._client.set(key, 0)
            r = 0
        return r

def _safe_pickle_loads(r):
    if r is None:
        return None
    try:
        return pickle.loads(r)
    except pickle.UnpicklingError:
        pass
    return None

def _safe_int(r, default=0):
    if r is None:
        return default
    try:
        return int(r)
    except ValueError:
        return default

class RedisClient(object):

    def __init__(self, servers, debug=False):
        import redis
        self._client = redis.StrictRedis(host=servers)

    def setint(self, key, value, expires=0):
        self._set(key, value, expires, use_pickle=False)

    def set(self, key, value, expires=0):
        '''
        Set object with key.

        Args:
            key: cache key as str.
            value: object value.
            expires: cache time, default to 0 (using default expires time)

        >>> key = uuid.uuid4().hex
        >>> c = RedisClient('localhost')
        >>> c.set(key, u'Python\u4e2d\u6587')
        >>> c.get(key)
        u'Python\u4e2d\u6587'
        >>> c.set(key, ['A', 'B', 'C'])
        >>> c.get(key)
        ['A', 'B', 'C']
        >>> c.set(key, 'Expires after 1 sec', 1)
        >>> c.get(key)
        'Expires after 1 sec'
        >>> time.sleep(2)
        >>> c.get(key, 'Not Exist')
        'Not Exist'
        '''
        logging.debug('set cache: key = %s' % key)
        self._set(key, value, expires, use_pickle=True)

    def _set(self, key, value, expires, use_pickle):
        self._client.set(key, pickle.dumps(value) if use_pickle else value)
        if expires:
            self._client.expire(key, expires)

    def get(self, key, default=None):
        '''
        Get object by key.

        Args:
            key: cache key as str.
            default: default value if key not found. default to None.
        Returns:
            object or default value if not found.

        >>> key = uuid.uuid4().hex
        >>> c = RedisClient('localhost')
        >>> c.get(key)
        >>> c.get(key, 'DEFAULT_REDIS')
        'DEFAULT_REDIS'
        >>> c.set(key, u'hello redis')
        >>> c.get(key)
        u'hello redis'
        >>> c.set(key, 12345)
        >>> c.get(key)
        12345
        '''
        logging.debug('get cache: key = %s' % key)
        r = self._client.get(key)
        if r is None:
            return default
        return _safe_pickle_loads(r)

    def gets(self, *keys):
        '''
        Get objects by keys.

        Args:
            keys: cache keys as str.
        Returns:
            list of object.

        >>> key1 = uuid.uuid4().hex
        >>> key2 = uuid.uuid4().hex
        >>> key3 = uuid.uuid4().hex
        >>> c = RedisClient('localhost')
        >>> c.gets(key1, key2, key3)
        [None, None, None]
        >>> c.set(key1, 'Key1')
        >>> c.set(key3, 'Key3')
        >>> c.gets(key1, key2, key3)
        ['Key1', None, 'Key3']
        '''
        return map(_safe_pickle_loads, self._client.mget(keys))

    def delete(self, key):
        '''
        Delete object from cache by key.

        Args:
            key: cache key as str.

        >>> key = uuid.uuid4().hex
        >>> c = RedisClient('localhost')
        >>> c.set(key, 'delete from redis')
        >>> c.get(key)
        'delete from redis'
        >>> c.delete(key)
        >>> c.get(key)
        '''
        self._client.delete(key)

    def getints(self, *keys):
        '''
        get ints by keys.

        >>> key1 = uuid.uuid4().hex
        >>> key2 = uuid.uuid4().hex
        >>> key3 = uuid.uuid4().hex
        >>> c = RedisClient('localhost')
        >>> c.getints(key1, key2, key3)
        [0, 0, 0]
        >>> c.setint(key1, 100)
        >>> c.setint(key2, -200)
        >>> c.incr(key3)
        1
        >>> c.getints(key1, key2, key3)
        [100, -200, 1]
        '''
        return map(_safe_int, self._client.mget(keys))

    def getint(self, key, default=0):
        return _safe_int(self._client.get(key), default)

    def incr(self, key):
        '''
        Increase counter.

        Args:
            key: cache key as str.

        >>> key = uuid.uuid4().hex
        >>> c = RedisClient('localhost')
        >>> c.incr(key)
        1
        >>> c.incr(key)
        2
        >>> c.setint(key, 100)
        >>> c.incr(key)
        101
        >>> c.getint(key)
        101
        >>> c.getint(key + '-no', 10)
        10
        '''
        return self._client.incr(key)

    def decr(self, key):
        '''
        Decrease counter.

        Args:
            key: cache key as str.

        >>> key = uuid.uuid4().hex
        >>> c = RedisClient('localhost')
        >>> c.decr(key)
        -1
        >>> c.decr(key)
        -2
        >>> c.setint(key, 100)
        >>> c.decr(key)
        99
        '''
        return self._client.decr(key)

client = DummyClient()

if __name__=='__main__':
    import uuid, doctest
    doctest.testmod()
