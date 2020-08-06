#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# @autor: Ramón Invarato Menéndez
# @version 1.0
from pathlib import Path
import hashlib
from random import random

"""
Script to generate random lines separated with a delimiter in a sample file of unsorted data

path_to_file: File to generate one file with the sample data
delimiter: delimiter between each part of line
columns: number of columns to create in a line (counter column of line generated always is added as first column)
file_size: when file size is reached, then end of generate
"""
path_to_file = Path("example_file_10mb.txt")
delimiter = "|"
columns = 10
file_size = 10 * 1024 * 1024

if __name__ == "__main__":

    with open(path_to_file, "a") as f:
        num = 0
        while True:
            num += 1
            line = delimiter.join([hashlib.sha256(str(random()).encode()).hexdigest()[:15] for _ in range(0, columns)])
            f.write("{}{}{}\n".format(num, delimiter, line))
            if num % 10000 == 0 and f.tell() > file_size:
                break

    print("{} lines wrote in file: {}\nExample of line generated: {}".format(num, path_to_file, line))
