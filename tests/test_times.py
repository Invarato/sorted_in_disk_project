# -*- coding: utf-8 -*-
#
# @autor: Ramón Invarato Menéndez
# @version 1.0
from datetime import datetime

"""
Several tests
"""
count = 20000000



if __name__ == "__main__":

    start = datetime.now()
    print("[if] start: {}".format(start))

    val = True
    for _ in range(1, count):
        if val:
            v = "aaa|bbb".split("|")
        else:
            v = "ccc|ddd".split("|")

    finish = datetime.now()
    print("[if] finish: {} | diff finish-start: {}".format(finish, finish-start))

    # ===============================================

    start = datetime.now()
    print("[function] start: {}".format(start))
    def mi_func():
        "aaa|bbb".split("|")

    for _ in range(1, count):
        mi_func()

    finish = datetime.now()
    print("[function] finish: {} | diff finish-start: {}".format(finish, finish-start))

    # ===============================================

    start = datetime.now()
    print("[function arg] start: {}".format(start))
    def mi_func(ar):
        del ar
        "aaa|bbb".split("|")

    for _ in range(1, count):
        mi_func("ccc")

    finish = datetime.now()
    print("[function arg] finish: {} | diff finish-start: {}".format(finish, finish-start))

    # ===============================================

    start = datetime.now()
    print("[function return] start: {}".format(start))
    def mi_func(ar):
        return "aaa|bbb".split("|")

    for _ in range(1, count):
        mi_func("ccc")

    finish = datetime.now()
    print("[function return] finish: {} | diff finish-start: {}".format(finish, finish-start))