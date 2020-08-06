# -*- coding: utf-8 -*-
#
# @autor: Ramón Invarato Menéndez
# @version 1.0
import logging
from datetime import datetime
from pathlib import Path

from sorted_in_disk.sorted_in_disk import sorted_in_disk

"""
Execute this script to see sorted result in console or in a result file

path_to_file_read: You can use generate_test_file.py to generate sample files with unsorted data
path_to_file_write: If None only simulate read in for loop and discard result
tmp_dir: temporal folder to sort in disk
"""
path_to_file_read = Path("path/example_file_10mb.txt")
path_to_file_write = None
tmp_dir = Path("tmp_test_folder")


def iter_big_file(path_to_file_read):
    with open(path_to_file_read, "r") as fichero:
        line = fichero.readline()
        while line:
            yield line
            line = fichero.readline().strip()


def save_iter_in_file(path_to_file_write, iterable):
    with open(path_to_file_write, "w") as f:
        for count, el in enumerate(iterable, 1):
            f.write("{}\n".format(el))
            if count % 100000 == 0:
                print("Lines readed: {}".format(count))


if __name__ == "__main__":

    start = datetime.now()
    print("[injecting] start: {}".format(start))
    gs = sorted_in_disk(iter_big_file(path_to_file_read),
                        key=lambda line: line.split("|")[2],
                        tmp_dir=tmp_dir)
    finish = datetime.now()

    print("[injecting] finish: {} | diff finish-start: {}".format(finish, finish-start))

    print("Counter lines injected: {}".format(len(gs)))

    start = datetime.now()
    print("[reading] start: {}".format(start))

    if path_to_file_write is None:
        for count, el in enumerate(gs, 1):
            if count % 100000 == 0:
                print("[{}] Lines read so far: {}".format(datetime.now(), count))
    else:
        save_iter_in_file(path_to_file_write, gs)

    finish = datetime.now()
    print("[reading] finish: {} | diff finish-start: {}".format(finish, finish-start))
