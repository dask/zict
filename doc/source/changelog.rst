Changelog
=========
.. currentmodule:: zict

2.3.0 - Unreleased
------------------
- Dropped support for Python 3.7 (:pr:`84`) `Guido Imperiale`_
- ``File.__getitem__`` now returns bytearray instead of bytes. This prevents a memcpy
  when deserializing numpy arrays with dask. (:pr:`74`) `Guido Imperiale`_
- Removed dependency from ``heapdict``; sped up ``LRU``. (:pr:`77`) `Guido Imperiale`_
- Fixed broken ``LRU`` state when the underlying mapping starts non-empty.
  (:pr:`77`) `Guido Imperiale`_
- ``File`` and ``LMDB`` now support :class:`pathlib.Path` and pytest's ``tmpdir``.
  (:pr:`78`) `Guido Imperiale`_
- ``LMDB`` now uses memory-mapped I/O on MacOSX and is usable on Windows.
  (:pr:`78`) `Guido Imperiale`_
- The library is now almost completely thread-safe.
  (:pr:`82`, :pr:`90`, :pr:`92`, :pr:`93`) `Guido Imperiale`_
- :class:`LRU` and :class:`Buffer` now support delayed eviction.
  New object :class:`InsertionSortedSet`.
  (:pr:`87`) `Guido Imperiale`_
- All mappings now return proper KeysView, ItemsView, and ValuesView objects from their
  keys(), items(), and values() methods (:pr:`93`) `Guido Imperiale`_
- :class:`File`, :class:`LMDB`, and :class:`Zip` now behave coherently with unexpected
  key/value types (:pr:`95`) `Guido Imperiale`_
- ``Zip.__contains__`` no longer reads the value from disk (:pr:`95`) `Guido Imperiale`_
- ``Zip.__setitem__`` will now raise when updating an already-existing key instead of
  quietly corrupting the mapping (:pr:`95`) `Guido Imperiale`_
- New object :class:`AsyncBuffer`; new method :meth:`LRU.get_all_or_nothing`
  (:pr:`88`) `Guido Imperiale`_


2.2.0 - 2022-04-28
------------------
- Added ``python_requires`` to ``setup.py`` (:pr:`60`) `Carlos Cordoba`_
- Added type annotations (:pr:`62`) `Guido Imperiale`_
- If you call ``Func.update()`` and ``Func`` wraps around ``File``, do not store all dump outputs in
  memory (:pr:`64`) `Guido Imperiale`_
- Added new classes ``zict.Cache`` and ``zict.WeakRefCache``
  (:pr:`65`) `Guido Imperiale`_


2.1.0 - 2022-02-25
------------------
- LRU and Buffer now deal with exceptions raised by the callbacks - namely, OSError
  raised when the disk is full (:pr:`48`) `Naty Clementi`_, `Guido Imperiale`_
- Dropped support for Python 3.6; added support for Python 3.9 and 3.10 (:pr:`55`)
  `Guido Imperiale`_
- Migrate to GitHub actions (:pr:`40`) `Thomas J. Fan`_
- Allow file mmaping (:pr:`51`) `jakirkham`_


2.0.0 - 2020-02-28
------------------

- Create ``CONTRIBUTING.md`` (:pr:`28`) `Jacob Tomlinson`_
- Import ABC from ``collections.abc`` instead of ``collections`` for Python 3.9
  compatibility (:pr:`31`) `Karthikeyan Singaravelan`_
- Drop Python 2 / 3.5 and add Python 3.7 / 3.8 support (:pr:`34`) `James Bourbeau`_
- Duplicate keys fast slow (:pr:`32`) `Florian Jetter`_
- Fix dask cuda worker's race condition failure (:pr:`33`) `Pradipta Ghosh`_
- Changed default ``lmdb`` encoding to ``utf-8`` (:pr:`36`) `Alex Davies`_
- Add code linting and style check (:pr:`35`) `James Bourbeau`_

.. _`Jacob Tomlinson`: https://github.com/jacobtomlinson
.. _`Karthikeyan Singaravelan`: https://github.com/tirkarthi
.. _`James Bourbeau`: https://github.com/jrbourbeau
.. _`Florian Jetter`: https://github.com/fjetter
.. _`Pradipta Ghosh`: https://github.com/pradghos
.. _`Alex Davies`: https://github.com/traverseda
.. _`Naty Clementi`: https://github.com/ncclementi
.. _`Guido Imperiale`: https://github.com/crusaderky
.. _`Thomas J. Fan`: https://github.com/thomasjpfan
.. _`jakirkham`: https://github.com/jakirkham
.. _`Carlos Cordoba`: https://github.com/ccordoba12