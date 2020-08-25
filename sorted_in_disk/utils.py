#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# @autor: Ramón Invarato Menéndez


__test__ = {'import_test': """
                           >>> from sorted_in_disk.utils import *

                           """}


def write_iter_in_file(path_to_file_write, iterable, fun_prepline=None, mode="w"):
    """
    Write a iterable as text line in file

    >>> mi_iterable = ["line1", "line2", "line3"]
    >>> write_iter_in_file("file.txt", mi_iterable)
    3

    :param path_to_file_write: path file where write
    :param iterable: Iterable where each element is a text line to write in disk
    :param fun_prepline: function with args count and line, to return a line to write in file.
    If None then apply this format each line: "{}\n".format(line.rstrip()).
    By default: None
    :param mode: mode w or a. By default: w
    :return: number of write lines
    """
    count = 0
    with open(path_to_file_write, mode) as f:
        if fun_prepline:
            for count, el in enumerate(iterable, 1):
                f.write(fun_prepline(count, el))
        else:
            for count, el in enumerate(iterable, 1):
                f.write("{}\n".format(el.rstrip()))

    return count


def read_iter_from_file(path_to_file_read):
    """
    Read a iterable where each element is a text line in file

    >>> mi_iterable = read_iter_from_file("file.txt")
    >>> for line in mi_iterable:
    ...     print(line)
    line1
    line2
    line3

    :param path_to_file_write: path file where read
    :return:
    """
    with open(path_to_file_read, "r") as fichero:
        line = fichero.readline().strip()
        while line:
            yield line
            line = fichero.readline().strip()


def human_size(size_bytes):
    """
    Return a human size readable from bytes

    >>> human_size(906607633)
    '864.61 MB'

    :param size_bytes: bytes to transform
    :return: string in human readable format.
    """
    if size_bytes is 0:
        return "0B"

    def ln(x):
        n = 99999999
        return n * ((x ** (1/n)) - 1)

    def log(x, base):
        result = ln(x)/ln(base)
        return result

    exp = int(log(size_bytes, 1024))
    try:
        unit = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")[exp]
    except KeyError:
        unit = "YB"
    return "{} {}".format(round(size_bytes / (1024 ** exp), 2), unit)


__test__ = {
    'clean_test_files': """
                        >>> from pathlib import Path
                        >>> Path("file.txt").unlink()

                        """}
