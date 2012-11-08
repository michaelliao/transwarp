from distutils.core import setup
import sys

kw = dict(
    name = 'transwarp',
    version = '1.0.1',
    description = 'Transwarp web framework',
    long_description = open('README', 'r').read(),
    author = 'Michael Liao',
    author_email = 'askxuefeng@gmail.com',
    url = 'https://github.com/michaelliao/transwarp',
    download_url = 'https://github.com/michaelliao/transwarp',
    license = 'Apache License',
    packages = ['transwarp'],
    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ])

setup(**kw)
