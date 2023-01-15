Zict: Composable Mutable Mappings
=================================

The dictionary / mutable mapping interface is powerful and multi-faceted.

*   We store data in different locations such as in-memory, on disk, in archive
    files, etc..
*   We manage old data with different policies like LRU, random eviction, etc..
*   We might encode or transform data as it arrives or departs the dictionary
    through compression, encoding, etc..

To this end we build abstract ``MutableMapping`` classes that consume and build
on other ``MutableMappings``.  We can compose several of these with each other
to form intuitive interfaces over complex storage systems policies.

Example
-------

In the following example we create an LRU dictionary backed by pickle-encoded,
zlib-compressed, directory of files.

.. code-block:: python

   import pickle
   import zlib

   from zict import File, Func, LRU

   a = File('mydir/')
   b = Func(zlib.compress, zlib.decompress, a)
   c = Func(pickle.dumps, pickle.loads, b)
   d = LRU(100, c)

   >>> d['x'] = [1, 2, 3]
   >>> d['x']
   [1, 2, 3]

API
---

.. currentmodule:: zict

.. autoclass:: Buffer
   :members:
.. autoclass:: Cache
   :members:
.. autoclass:: WeakValueMapping
   :members:
.. autoclass:: File
   :members:
.. autoclass:: Func
   :members:
.. autoclass:: LMDB
   :members:
.. autoclass:: LRU
   :members:
.. autoclass:: Sieve
   :members:
.. autoclass:: Zip
   :members:


Changelog
---------

Release notes can be found :doc:`here <changelog>`.
