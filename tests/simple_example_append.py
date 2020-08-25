# -*- coding: utf-8 -*-
#
# @autor: Ramón Invarato Menéndez
# @version 1.0
from datetime import datetime
from pathlib import Path

from sorted_in_disk import sorted_in_disk

"""
Execute this script to see sorted result in console

iterator_with_data_to_sort: any iterable with data to sort
tmp_dir: temporal folder to sort in disk
"""
iterator_with_data_to_sort = [
    "key3|value3",
    "key1|value1",
    "key2|value2"
]
second_iterator_with_data_to_sort = [
    "key4|value4",
    "key6|value6",
    "key5|value5"
]
third_iterator_with_data_to_sort = [
    "key9|value9",
    "key7|value7",
    "key8|value8"
]
tmp_dir = Path("tmp_test_folder")

max_process = None


if __name__ == "__main__":
    start = datetime.now()
    print("[injecting] start: {}".format(start))
    gs = sorted_in_disk(iterator_with_data_to_sort,
                        key=lambda line: line.split("|")[0],
                        tmp_dir=tmp_dir,
                        write_processes=max_process,
                        append=False,
                        only_one_read=False)
    finish = datetime.now()
    print("[injecting] finish: {} | diff finish-start: {}".format(finish, finish-start))
    print("Counter lines injected: {}".format(len(gs)))

    print("[injecting 2] start: {}".format(start))
    gs = sorted_in_disk(second_iterator_with_data_to_sort,
                        key=lambda line: line.split("|")[0],
                        tmp_dir=tmp_dir,
                        write_processes=max_process,
                        append=True,
                        only_one_read=False)
    finish = datetime.now()
    print("[injecting 2] finish: {} | diff finish-start: {}".format(finish, finish-start))
    print("Counter lines injected 2: {}".format(len(gs)))

    print("[injecting 3] start: {}".format(start))
    gs = sorted_in_disk(third_iterator_with_data_to_sort,
                        key=lambda line: line.split("|")[0],
                        tmp_dir=tmp_dir,
                        write_processes=max_process,
                        append=True,
                        only_one_read=True)
    finish = datetime.now()
    print("[injecting 3] finish: {} | diff finish-start: {}".format(finish, finish-start))
    print("Counter lines injected 3: {}".format(len(gs)))

    start = datetime.now()
    print("[reading] start: {}".format(start))

    for count, el in enumerate(gs, 1):
        print(el)

    finish = datetime.now()
    print("[reading] finish: {} | diff finish-start: {}".format(finish, finish-start))
