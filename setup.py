#!/usr/bin/env python

import os
from setuptools import setup

setup(name='zhip',
      version='0.0.1',
      description='Mutable mapping interface for zip files',
      url='http://github.com/mrocklin/zhip/',
      maintainer='Matthew Rocklin',
      maintainer_email='mrocklin@gmail.com',
      license='BSD',
      keywords='zip',
      packages=['zhip'],
      install_requires=[],
      long_description=(open('README.rst').read() if os.path.exists('README.rst')
                        else ''),
      zip_safe=False)
