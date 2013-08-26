#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Michael Liao'

'''
Utils
'''

class Dict(dict):
    '''
    Simple dict but support access as x.y style.

    >>> d1 = Dict()
    >>> d1['x'] = 100
    >>> d1.x
    100
    >>> d1.y = 200
    >>> d1['y']
    200
    >>> d2 = Dict(a=1, b=2, c='3')
    >>> d2.c
    '3'
    >>> d2['empty']
    Traceback (most recent call last):
        ...
    KeyError: 'empty'
    >>> d2.empty
    Traceback (most recent call last):
        ...
    AttributeError: 'Dict' object has no attribute 'empty'
    >>> d3 = Dict(('a', 'b', 'c'), (1, 2, 3))
    >>> d3.a
    1
    >>> d3.b
    2
    >>> d3.c
    3
    '''
    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

def load_module(modname):
    '''
    Load module as object.

    >>> m1 = load_module('time')
    >>> type(m1)
    <type 'module'>
    >>> m1.__name__
    'time'
    >>> m2 = load_module('xml.dom')
    >>> type(m2)
    <type 'module'>
    >>> m2.__name__
    'xml.dom'
    >>> m3 = load_module('xml.sax.handler')
    >>> type(m3)
    <type 'module'>
    >>> m3.__name__
    'xml.sax.handler'
    >>> load_module('base64.b64encode')
    Traceback (most recent call last):
      ...
    ImportError: No module named b64encode
    '''
    last = modname.rfind('.')
    name = modname if last==(-1) else modname[:last]
    return __import__(modname, globals(), locals(), [name])

if __name__=='__main__':
    import doctest
    doctest.testmod()
