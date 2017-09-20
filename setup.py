#!/usr/bin/env python

import os
from setuptools import setup

setup(name='zict',
      version='0.1.3',
      description='Mutable mapping tools',
      url='http://zict.readthedocs.io/en/latest/',
      maintainer='Matthew Rocklin',
      maintainer_email='mrocklin@gmail.com',
      license='BSD',
      keywords='mutable mapping,dict,dask',
      packages=['zict'],
      install_requires=[open('requirements.txt').read().strip().split('\n')],
      long_description=(open('README.rst').read() if os.path.exists('README.rst')
                        else ''),
      zip_safe=False)
