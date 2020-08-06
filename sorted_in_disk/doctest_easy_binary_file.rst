============================
Test for easy_binary_file.py
============================

This can be executed with command:
``python -m doctest -v doctest_easy_binary_file.rst``

::

    >>> from easy_binary_file import EasyBinaryFile, EasyBinaryFileHelper

open_file
=========

Example dump data.
::

    >>> fw = EasyBinaryFileHelper.open_file("test_file.tmp", "wb")
    >>> import pickle
    >>> pickle.dump("test_value", fw, pickle.HIGHEST_PROTOCOL)
    >>> fw.close()

Example load data.
::

    >>> fr = EasyBinaryFileHelper.open_file("test_file.tmp", "rb")
    >>> import pickle
    >>> value = pickle.load(fr)
    >>> fr.close()
    >>> print(value)
    test_value

Clean after test.
::

    >>> from pathlib import Path
    >>> Path("test_file.tmp").unlink()
