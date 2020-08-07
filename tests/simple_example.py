# -*- coding: utf-8 -*-
#
# @autor: Ramón Invarato Menéndez
# @version 1.0
from datetime import datetime
from pathlib import Path

from sorted_in_disk import sorted_in_disk

"""
Execute this script to see sorted result in console

iterable_with_data_to_sort: any iterable with data to sort
tmp_dir: temporal folder to sort in disk
"""
iterable_with_data_to_sort = [
    "key3|value3",
    "key1|value1",
    "key2|value2"
]
tmp_dir = Path("tmp_test_folder")


if __name__ == "__main__":
    start = datetime.now()
    print("[injecting] start: {}".format(start))
    gs = sorted_in_disk(iterable_with_data_to_sort,
                        key=lambda line: line.split("|")[0],
                        tmp_dir=tmp_dir)
    finish = datetime.now()

    print("[injecting] finish: {} | diff finish-start: {}".format(finish, finish-start))
    print("Counter lines injected: {}".format(len(gs)))

    start = datetime.now()
    print("[reading] start: {}".format(start))

    for count, el in enumerate(gs, 1):
        print(el)

    finish = datetime.now()
    print("[reading] finish: {} | diff finish-start: {}".format(finish, finish-start))
