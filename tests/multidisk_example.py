# -*- coding: utf-8 -*-
#
# @autor: Ramón Invarato Menéndez
# @version 1.0
import logging
from datetime import datetime
from pathlib import Path

from sorted_in_disk import sorted_in_disk
from sorted_in_disk.utils import read_iter_from_file, write_iter_in_file

"""
Execute this script to see sorted result in console or in a result file

path_to_file_read: You can use generate_test_file.py to generate sample files with unsorted data
path_to_file_write: If None only simulate read in for loop and discard result
tmp_dir: temporal folder to sort in disk
"""
path_to_file_read = Path("path/example_file_10mb.txt")
path_to_file_write = None

tmp_dir = Path("tmp_test_folder_controller")
write_processes_dirs = [Path("C:\\tmp_test_folder_process_1"),
                        Path("D:\\tmp_test_folder_process_2"),
                        Path("D:\\tmp_test_folder_process_2"),
                        Path("J:\\tmp_test_folder_process_3")]


def fun_prepline(count, line):
    if count % 100000 == 0:
        print("Lines readed: {}".format(count))
    return "{}\n".format(line.rstrip())


if __name__ == "__main__":

    start = datetime.now()
    print("[injecting] start: {}".format(start))
    sid = sorted_in_disk(read_iter_from_file(path_to_file_read),
                         key=lambda line: line.split("|")[2],
                         tmp_dir=tmp_dir,
                         only_one_read=True,
                         append=True,
                         write_processes=write_processes_dirs,
                         read_process=False,
                         logging_level=logging.DEBUG)
    finish = datetime.now()

    print("[injecting] finish: {} | diff finish-start: {}".format(finish, finish-start))

    print("Counter lines injected: {}".format(len(sid)))

    sid.visor()

    start = datetime.now()
    print("[reading] start: {}".format(start))

    if path_to_file_write is None:
        count = 0
        for count, el in enumerate(sid, 1):
            if count % 100000 == 0:
                print("[{}] Lines read so far: {}".format(datetime.now(), count))
    else:
        count = write_iter_in_file(path_to_file_write, sid, fun_prepline=fun_prepline)

    print("Total lines readed: {}".format(count))

    finish = datetime.now()
    print("[reading] finish: {} | diff finish-start: {}".format(finish, finish-start))
