#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# @autor: Ramón Invarato Menéndez

import os
import sys
import time
import multiprocessing
import gc
from pathlib import Path
import logging

from easy_binary_file import EasyBinaryFile, load_single_value, dump_single_value, quick_dump_items, quick_load_items


try:
    import queue
except ImportError:
    # python 3.x
    import Queue as queue


__test__ = {'import_test': """
                           >>> from sorted_in_disk.sorted_in_disk import *

                           """}


def sorted_in_disk(iterable,
                   key=None,
                   reverse=False,

                   tmp_dir=Path("sortInDiskTmps"),
                   ensure_different_dirs=False,

                   append=False,
                   only_one_read=True,

                   count_insert_to_check=1000000,
                   max_process_size=1024 * 1024 * 1024,
                   ensure_space=False,

                   max_process=0,
                   queue_max_size=1000,

                   iter_multiprocessing=False,
                   iter_m_queue_max_size=1000,

                   logging_level=logging.WARNING):
    """
    Return a new sorted object SortedInDisk from the items in iterable

    A sort is stable if it guarantees not to change the relative order of elements that compare equal —
    this is helpful for sorting in multiple passes (for example, sort by department, then by salary grade).

    It is important have one tmp_dir for each instance of SortedInDisk, if you need to use several times and in same
    time more than one SortedInDisk is important to define one tmp_dir different to each instance. If you want to use
    more than one time the work in one tmp_dir, then you need to sure that only one instance at time access to tmp_dir
    folder.

    Moments to delete temporaly directory (clean temporaly files):
        1) Before sort (auto if append=False). Because previous execution of sorted_in_disk not was cleaned.
        2) After sort (auto if only_one_read=True and return iterable is full consumed)
        3) After end SortInDisk work (Manually call to clear())

    Why do not clean tmp files after execution:
        A) You want to append more data to previously sorted work
        B) You want to reuse sorted result

    Reuse sorted data in sorted_in_disk is quickly than inject unsorted data other time, but slowly than save result
    in a normal file and reuse from this one. For this reason, it is strongly recomend to save result in other structure
    or file if you planed to reuse more than one or two times.

    Note if max_process != 0: Multiprocess improve hard sorted work in other/s processes and get free the main process
                              after end of injection. It is necesary to improve a join to ensure and save sorted states.
                              For auto-join you can iterate sorted result (you not need to do nothing). But if you only
                              want inject data to future use, then you need to call explicit join call to:
                              join_multiprocess()

    Example of quick use:
    # >>> iterable_unsorted = ["valA|key3|valD", "valB|key1|valE", "valC|key2|valF"]
    # >>> sid = sorted_in_disk(iterable_unsorted, key=lambda line: line.split("|")[1])
    # >>> list(sid)
    ['valB|key1|valE', 'valC|key2|valF', 'valA|key3|valD']

    Example to define folder for temp files (necesary to reuse sorted files):
    # >>> iterable_unsorted = ["valA|key3|valD", "valB|key1|valE", "valC|key2|valF"]
    # >>> sid = sorted_in_disk(iterable_unsorted, key=lambda line: line.split("|")[1], tmp_dir="my_tmp_folder")
    # >>> list(sid)
    ['valB|key1|valE', 'valC|key2|valF', 'valA|key3|valD']

    Example to define multiprocess (max_process=None to auto-get the number of max physical processors):
    # >>> iterable_unsorted = ["valA|key3|valD", "valB|key1|valE", "valC|key2|valF"]
    # >>> sid = sorted_in_disk(iterable_unsorted, key=lambda line: line.split("|")[1], max_process=4)
    # >>> list(sid)
    ['valB|key1|valE', 'valC|key2|valF', 'valA|key3|valD']

    Example to append more data to same structure:
    >>> iterable_unsorted = ["valA|key3|valD", "valB|key1|valE", "valC|key2|valF"]
    >>> sid = sorted_in_disk(iterable_unsorted, key=lambda line: line.split("|")[1], only_one_read=False)
    >>> iterable_unsorted = ["valG|key4|valJ", "valH|key0|valK"]
    >>> sid = sorted_in_disk(iterable_unsorted, key=lambda line: line.split("|")[1], append=True)
    >>> list(sid)
    ['valH|key0|valK', 'valB|key1|valE', 'valC|key2|valF', 'valA|key3|valD', 'valG|key4|valJ']

    Example to remove tmp files if not full iterate (or if only_one_read=False):
    >>> iterable_unsorted = ["valA|key3|valD", "valB|key1|valE", "valC|key2|valF"]
    >>> sid = sorted_in_disk(iterable_unsorted, key=lambda line: line.split("|")[1], only_one_read=False)
    >>> sid.clear()

    :param iterable: iterable to sort in disk
        This iterable should by a generator for big data due to memory RAM limitation.
    :param key: key specifies a function of one argument that is used to extract a comparison key from each element in
        iterable (for example, key=str.lower or key=lambda e: e.split(",")[3]).
        The default value is None (compare the elements directly).
    :param reverse: reverse is a boolean value.
        If set to True, then the list elements are sorted as if each comparison were reversed.
    :param tmp_dir: Path to dir where save temporal files. If None this create a folder and overwrite if
        exist previously. By default: create a sortInDiskTmps folder in current directory.
    :param ensure_different_dirs: True to add incremental counter to folder if exists previously.
        Useful if you use several instances of SortedInDisk at same time.
        Note: conflict if `append` is `True`, because this creates a new name and not delete
        previously file (example if True: if exist /path/folder/ it create a new /paht/folder(1)/). By default: False
    :param append: True to clean folder tmp_dir if existe previously. By default: False
    :param only_one_read: True to clean folder tmp_dir when you consume all data.
        If it is True only works if you read all returned data, if you not read all, then you need to clear instance
        to auto. By default: True
    :param count_insert_to_check: counter to check if process have more size in memory than max_process_size.
        By default: 1000000
    :param max_process_size: max size in bytes to dump cache memory values to disk
        (only execute when count_insert_to_check is reached). If None, then not import psutil and then only
        check with count_insert_to_check. By default: 1024*1024*1024  # 1Gib
    :param ensure_space: True to ensure disk space but is slowly. If not space then process launch warning message
        and wait for space. If False, then get and IOException if not enough space. By defatul: False
    :param max_process: number of process to execute. If None then it is number of CPUs. By default: 0
    :param queue_max_size: (only if max_process!=0) max number of elements in queue. If None then is the max by default.
        By default: 1000
    :param iter_multiprocessing: True to get and prepare data in other process, False to use this one.
        By default: False
    :param iter_m_queue_max_size: (only if enable_multiprocessing is True) max number of elements in queue. If None
        then is the max by default. By default: 1000
    :param logging_level: Level of log. Only to debug or to remove psutil warning. By default: logging.WARNING

    :return: SortedInDisk object (you can iterate directly in for structure in same way as list)

    Note: This is a modification of oficial documentation from https://docs.python.org/3/library/functions.html#sorted
    """
    return SortedInDisk(tmp_dir,
                        delete_to_end=only_one_read,
                        delete_previous=not append,
                        ensure_different_dirs=ensure_different_dirs,
                        iter_multiprocessing=iter_multiprocessing,
                        iter_m_queue_max_size=iter_m_queue_max_size,
                        logging_level=logging_level,
                        ).save_and_sort(iterable,
                                        func_key_sort=key,
                                        reverse=reverse,
                                        max_process=max_process,
                                        count_insert_to_check=count_insert_to_check,
                                        max_process_size=max_process_size,
                                        queue_max_size=queue_max_size,
                                        ensure_space=ensure_space)


def _is_parent_process_killed():
    """
    Return if parent process was killed
    :return: True if parent process was killed
    """
    return os.getppid() == 1


def _get_func_process_memory(enable_consumption_check=True):
    """
    Get a function to calculate current mount size of memory has consume the current process in the moment
    to call the function
    :param enable_consumption_check: True to get a function to check memory consumption, False to return -1 in the
                                     function. If True but not installed psutil, then it will be returned -1 in the
                                     same way.
    :return: function to calculate memory consumption.
             If function return -1 then get current mount of memory is not available
    """
    if enable_consumption_check:
        try:
            import psutil

            def get_process_memory():
                """
                :return: bites of this process size in memory
                """
                process = psutil.Process(os.getpid())
                return process.memory_info().rss

            return get_process_memory
        except ModuleNotFoundError:
            logging.warning("psutil module not found, auto-disabled memory check "
                            "(only check by counter count_insert_to_check). "
                            "To use auto memory check, install psutil "
                            "(PIP installation: pip install psutil)")

    def get_process_memory():
        return -1

    return get_process_memory


def _mprocess(proxy_queue,
              proxy_start_event,
              proxy_end_event,
              ipid,

              dir_tmp_path,
              proxy_dict,

              count_insert_to_check,
              max_process_size,
              reverse,

              next_id_path_to_keys_sorted,

              ensure_space,
              logging_level):
    """
    Process to inject data.

    :param proxy_queue: queue to work
    :param proxy_start_event: start flag process notification
    :param proxy_end_event: end flag process notification
    :param ipid: pid of this process
    :param dir_tmp_path: path to tmp directories
    :param proxy_dict: dict of sorted indexation
    :param count_insert_to_check: counter to check if process have more size in memory than max_process_size.
    :param max_process_size: max size in bytes to dump cache memory values to disk.
    :param reverse: True to reverse sort. By default: False
    :param ensure_space: True to ensure disk space but is slowly. If not space then process launch warning message
        and wait for space. If False, then get and IOException if not enough space
    :return: None
    """
    logging.basicConfig(stream=sys.stderr, level=logging_level)

    get_process_memory = _get_func_process_memory(max_process_size is not None)

    logging.debug("[START -> id:{} | ppid:{} | pid:{}]".format(ipid, os.getppid(), os.getpid()))

    def sort_cache_and_save(dir_tmp_path, ipid, key_file, dict_to_save, reverse):
        path_to_keys_sorted = None
        if dict_to_save:
            def gen_key_value_sorted(mdict_to_save, mreverse):
                # When it is sorted, it assign each key to his value
                for key in sorted(mdict_to_save.keys(), reverse=mreverse):
                    yield key, mdict_to_save[key]

            path_to_keys_sorted = Path(dir_tmp_path, "keys_sorted_{}_{}.db".format(ipid, key_file))
            quick_dump_items(path_to_keys_sorted, gen_key_value_sorted(dict_to_save, reverse))

            del dict_to_save
            gc.collect()
        return path_to_keys_sorted

    count_key_file = next_id_path_to_keys_sorted
    cache_bulk_counter = 0
    total_bulk_counter = 0
    times_waiting = 0

    dict_keysortable_fpositions = {}
    list_paths_to_keys_sorted = list()

    def evt_err_space_dump(_, time_to_retry, err):
        logging.error("[NOT SPACE ON DEVICE (WAITING TO CONTINUE {} SECONDS) -> "
                      "id:{} | ppid:{} | pid:{}]: {}".format(time_to_retry,
                                                             ipid,
                                                             os.getppid(),
                                                             os.getpid(), err))

    path_full_data = Path(dir_tmp_path, "full_data_{}.db".format(ipid))
    with EasyBinaryFile(path_full_data, mode='ab') as f_full_data:
        loop_enable = True
        gc.collect()
        while loop_enable:
            try:
                sort_key, value = proxy_queue.get(timeout=0.1)
                times_waiting = 0

                start_cursor_pos = f_full_data.get_cursor_position()

                f_full_data.dump(value,
                                 ensure_space=ensure_space,
                                 fun_err_space=evt_err_space_dump)

                try:
                    fpositions = dict_keysortable_fpositions[sort_key]
                    fpositions.append(start_cursor_pos)
                    dict_keysortable_fpositions[sort_key] = fpositions
                except KeyError:
                    dict_keysortable_fpositions[sort_key] = [start_cursor_pos]

                if count_insert_to_check is not None:
                    cache_bulk_counter += 1

                    if count_insert_to_check < cache_bulk_counter:
                        process_memory = get_process_memory()
                        total_bulk_counter += cache_bulk_counter
                        cache_bulk_counter = 0
                        logging.debug("[MEMORY CHECK -> id:{} | ppid:{} | pid:{}]: "
                                      "mem<{}>, els<{}>, ~qsize<{}>".format(ipid,
                                                                            os.getppid(),
                                                                            os.getpid(),
                                                                            process_memory,
                                                                            total_bulk_counter,
                                                                            proxy_queue.qsize()))
                        if process_memory == -1 is None or max_process_size < process_memory:
                            # If process have more size than limit, then cache is saved to disk and set cache to empty
                            count_key_file += 1
                            logging.debug("[SAVING MEMORY -> id:{} | ppid:{} | pid:{}]: key<{}>".format(ipid,
                                                                                                        os.getppid(),
                                                                                                        os.getpid(),
                                                                                                        count_key_file))
                            paths_to_keys_sorted = sort_cache_and_save(dir_tmp_path,
                                                                       ipid,
                                                                       count_key_file,
                                                                       dict_keysortable_fpositions,
                                                                       reverse)
                            list_paths_to_keys_sorted.append(paths_to_keys_sorted)
                            del dict_keysortable_fpositions
                            gc.collect()
                            dict_keysortable_fpositions = {}
            except queue.Empty:
                loop_enable = not (proxy_end_event.is_set() and proxy_queue.empty())
                if loop_enable:
                    times_waiting += 1
                    if proxy_start_event.is_set():
                        time_to_retry = 0.1 * times_waiting

                        if _is_parent_process_killed():
                            logging.debug("[PARENT KILLED (TERMINATE) -> "
                                          "id:{} | ppid:{} | pid:{}]".format(ipid,
                                                                             os.getppid(),
                                                                                 os.getpid()))
                            loop_enable = False
                        else:
                            gc.collect()
                            time.sleep(time_to_retry)
                    else:
                        logging.debug("[WAIT -> id:{} | ppid:{} | pid:{}]".format(ipid,
                                                                                  os.getppid(),
                                                                                  os.getpid()))
                        gc.collect()
                        proxy_start_event.wait()

                        logging.debug("[RESUME WAIT -> id:{} | ppid:{} | pid:{}]".format(ipid,
                                                                                         os.getppid(),
                                                                                         os.getpid()))
            except Exception as err:
                logging.error("[ERROR -> id:{} | ppid:{} | pid:{}]: {}".format(ipid, os.getppid(), os.getpid(), err))

    total_bulk_counter += cache_bulk_counter

    logging.debug("[LOOP STOP -> id:{} | ppid:{} | pid:{}]".format(ipid, os.getppid(), os.getpid()))
    gc.collect()

    paths_to_keys_sorted = sort_cache_and_save(dir_tmp_path,
                                               ipid,
                                               count_key_file + 1,
                                               dict_keysortable_fpositions,
                                               reverse)
    if paths_to_keys_sorted is not None:
        list_paths_to_keys_sorted.append(paths_to_keys_sorted)

    next_id_path_to_keys_sorted = count_key_file + 1
    if len(list_paths_to_keys_sorted) > 0:
        proxy_dict[ipid] = (path_full_data,
                            list_paths_to_keys_sorted,
                            next_id_path_to_keys_sorted,
                            total_bulk_counter)
    else:
        path_full_data.unlink()

    gc.collect()
    logging.debug("[END -> id:{} | ppid:{} | pid:{}]".format(ipid, os.getppid(), os.getpid()))


def _get_next(iter_f):
    """
    Get function than get next of iterable in each call
    :param iter_f: iterable to wrap in next function
    :return: function to get next of iterable
    """
    def f_next():
        try:
            return next(iter_f)
        except RuntimeError:
            return None
        except StopIteration:
            return None

    return f_next


def _iter_get_data_from_files(dict_ipid_tup_full_list_parts, reverse=False):
    """
    Generator to read first line in all sorted buckets and sort all obtained lines, then obtain first until to consume
    buckets all.

    :param dict_ipid_tup_full_list_parts: dict with information about temporal files
    :param reverse: True to reverse sort. By default: False
    :return: Generator to return tuples key and line after sort.
    """
    full_data_counter = dict()
    l_get = list()
    for ipid, tup in dict_ipid_tup_full_list_parts.items():
        f_full_data_open = EasyBinaryFile(tup[0], mode='rb')

        for path_to_keys_sorted in tup[1]:
            f_next = _get_next(quick_load_items(path_to_keys_sorted))
            l_get.append(f_next() + (f_next, f_full_data_open))
            try:
                full_data_counter[f_full_data_open] += 1
            except KeyError:
                full_data_counter[f_full_data_open] = 1

    l_get = sorted(l_get, key=lambda mtup: mtup[0], reverse=reverse)

    # To several sorted files
    while len(l_get) > 1:
        key, fpositions, f_next, f_full_data = l_get[0]
        for f_pos in fpositions:
            f_full_data.seek(f_pos)
            yield key, f_full_data.load()

        new_tup = f_next()
        if new_tup is None:
            l_get.pop(0)
            full_data_counter[f_full_data] -= 1
            if full_data_counter[f_full_data] == 0:
                f_full_data.close()
                del full_data_counter[f_full_data]
        else:
            l_get[0] = new_tup + (f_next, f_full_data)
            if (not reverse and l_get[0][0] > l_get[1][0]) or (reverse and l_get[0][0] < l_get[1][0]):
                l_get = sorted(l_get, key=lambda mtup: mtup[0], reverse=reverse)

    # To one sorted file
    if len(l_get) == 1:
        el_get = l_get[0]
        while el_get is not None:
            key, fpositions, f_next, f_full_data = el_get

            for f_pos in fpositions:
                yield key, f_full_data.get_by_cursor_position(f_pos)

            new_tup = f_next()
            if new_tup is None:
                el_get = None
                full_data_counter[f_full_data] -= 1
                if full_data_counter[f_full_data] == 0:
                    f_full_data.close()
                    del full_data_counter[f_full_data]
            else:
                el_get = new_tup + (f_next, f_full_data)

    for f in full_data_counter.keys():
        f.close()

    del full_data_counter


def _mprocess_iter(proxy_queue_iter,
                   proxy_start_event_iter,
                   proxy_end_event_iter,

                   dict_ipid_tup_full_list_parts,
                   reverse,

                   logging_level):
    """
    Consumer process of sorted data

    :param proxy_queue_iter: queue to work
    :param proxy_start_event_iter: start flag process notification
    :param proxy_end_event_iter: end flag process notification
    :param dict_ipid_tup_full_list_parts: dict with information about temporal files
    :param reverse: True to reverse sort. By default: False
    :param logging_level:
    :return: None
    """
    logging.basicConfig(stream=sys.stderr, level=logging_level)

    logging.debug("[START GETTER -> ppid:{} | pid:{}]".format(os.getppid(), os.getpid()))
    proxy_start_event_iter.set()
    gc.collect()

    if dict_ipid_tup_full_list_parts is not None:
        for tup_key_loadpickle in _iter_get_data_from_files(dict_ipid_tup_full_list_parts, reverse):
            loop_enable = True
            while loop_enable:
                try:
                    proxy_queue_iter.put(tup_key_loadpickle, timeout=1)
                    loop_enable = False
                except queue.Full:
                    if _is_parent_process_killed():
                        logging.debug("[GETTER PARENT KILLED (TERMINATE) -> ppid:{} | pid:{}]".format(os.getppid(),
                                                                                                      os.getpid()))
                        exit()

        logging.debug("[LOOP GETTER STOP -> ppid:{} | pid:{}]".format(os.getppid(), os.getpid()))

    proxy_end_event_iter.set()
    gc.collect()
    logging.debug("[END GETTER -> ppid:{} | pid:{}]".format(os.getppid(), os.getpid()))


def create_tmp_folder(dir_tmp_path, ensure_different_dirs=False):
    """
    Helper to create a temporal folder

    :param dir_tmp_path: directory where create a temporal folder
        (if ensure_different_dirs=True then this will be changed)
    :param ensure_different_dirs: True to add incremental counter to folder if exists previously.
        Useful if you use several instances of SortedInDisk at same time.
        Note: conflict if delete_previous is True in save_and_sort(), because this create a new name and not delete
        previously file (example if True: if exist /path/folder/ it create a new /paht/folder(1)/). By default: False
    :return: path to final path to temporal folder
    """
    dir_tmp_path = Path(dir_tmp_path)

    if ensure_different_dirs:
        aux_dir_tmp_path = Path(dir_tmp_path)
        count = 0

        while aux_dir_tmp_path.exists():
            count += 1
            aux_dir_tmp_path = Path(dir_tmp_path.parent, "{}_{}".format(dir_tmp_path.name, count))

        dir_tmp_path = aux_dir_tmp_path

    wait_for_files_idle_times = 10
    error = None
    while wait_for_files_idle_times > 0:
        try:
            if not dir_tmp_path.exists():
                try:
                    dir_tmp_path.mkdir()
                except Exception:
                    pass
            wait_for_files_idle_times = 0
            error = None
        except Exception as err:
            time.sleep(0.2)
            wait_for_files_idle_times -= 1
            error = err

    if error:
        raise error

    return dir_tmp_path


def delete_tmp_folder(dir_tmp_path, secure_paths_to_del=None):
    """
    Delete temporal files created

    :param dir_tmp_path: parent dir to delete
    :param secure_paths_to_del: iterable of paths to delete. Safe check with dir_tmp_path parent.
    :exception FileNotFoundError: raise if one path of secure_paths_to_del is not a dir_tmp_path child.
    :return: None
    """
    def del_file(path_file_to_del):
        try:
            if path_file_to_del.exists():
                if path_file_to_del.is_file():
                    path_file_to_del.unlink()
                else:
                    path_file_to_del.rmdir()
        except OSError:
            pass

    if secure_paths_to_del:
        for path_to_del in secure_paths_to_del:
            if dir_tmp_path in path_to_del.parents or dir_tmp_path is path_to_del:
                del_file(path_to_del)
            else:
                raise FileNotFoundError("{} is not a path of {}".format(path_to_del,
                                                                        dir_tmp_path))


class SortedInDisk(object):
    """
    Clase SortedInDisk
    """
    def __init__(self,
                 path_to_tmp_dir=Path("sortInDiskTmps"),
                 ensure_different_dirs=False,

                 delete_previous=True,
                 delete_to_end=True,

                 iter_multiprocessing=False,
                 iter_m_queue_max_size=1000,

                 logging_level=logging.WARNING):
        """
        Sort in disk mono-thread or multiprocess.
        By default is multiprocess with the max CPUs in system (defined by max_process parameter)

        Example of use for one example iterator:
            iterator_with_data_to_sort=[
                "key3|value3",
                "key1|value1",
                "key2|value2"
            ]

        Quick simple use (Multiprocessor):
            sid = SortedInDisk().save_and_sort(iterator_with_data_to_sort,
                                             func_key_sort=lambda line: line.split("|")[0])
            for line_sorted in sid:
                print(line_sorted)

        Multiprocessor declaration:
            sid = SortedInDisk(Path("C:\tmp_folder\"))\
                  .save_and_sort(iterator_with_data_to_sort,
                                 func_key_sort=lambda line: line.split()[0])

            Note: This is not blocking the current thread flow. Join when call to iter_with_key().
                  To explicit join call to: sid.join_multiprocess()

        Mono thread (current) declaration:
            sid = SortedInDisk(Path("C:\tmp_folder\"))\
                  .save_and_sort(iterator_with_data_to_sort,
                                 func_key_sort=lambda line: line.split()[0],
                                 max_process=0)

            Note: This is blocking the current thread flow until end.
            Note: If path_to_tmp_dir is not defined, then it is in current execution script folder

        Iter by lines in any case (multiprocess join here, previously to iterate)
            for line_sorted in sid:
                print(line_sorted)

        :param path_to_tmp_dir: Path to dir where save temporal files. By default: create a sortInDiskTmps folder
                                in current directory.
        :param ensure_different_dirs: True to add incremental counter to folder if exists previously.
            Useful if you use several instances of SortedInDisk at same time.
            Note: conflict if delete_previous is True in save_and_sort(), because this create a new name and not
            delete previously file
            (example if True: if exist /path/folder/ it create a new /paht/folder(1)/). By default: False
        :param delete_previous: True to delete previous tmps files, False to append data to previous files.
                                By default: True
        :param delete_to_end: True to delete tmps files in the end of consumption of sorted data. If False or
                              if you not consume full returned iterable, then you may to delete tmps files by hand
                              (you can carry out with clear() method). By default: True
        :param iter_multiprocessing: True to get and prepare data in other process, False to use this one.
            By default: False
        :param iter_m_queue_max_size: (only if enable_multiprocessing is True) max number of elements in queue. If None
            then is the max by default. By default: 1000
        """
        self.logging_level = logging_level
        logging.basicConfig(stream=sys.stderr, level=self.logging_level)

        if sys.version_info[0] < 3:
            raise IOError("Solo compatible con Python >= 3")

        if delete_previous:
            self.dir_tmp_path = path_to_tmp_dir
            self.delete_tmp(remove_tmp_folder=True)

        self.dir_tmp_path = create_tmp_folder(path_to_tmp_dir, ensure_different_dirs)

        self.delete_to_end = delete_to_end

        self.dict_num_procceses = dict()
        self.manager = multiprocessing.Manager()
        self.proxy_dict = None

        self.iter_multiprocessing = iter_multiprocessing
        self.iter_m_queue_max_size = iter_m_queue_max_size

    def tmp_paths(self, include_tmp_folder=True):
        dict_info = self.get_dict_saved_info()
        yield Path(self.dir_tmp_path, "dict_info.db")
        if not dict_info["empty"]:
            if dict_info["dict_ipid_tup_full_list_parts"] is not None:
                for ipid, tup in dict_info["dict_ipid_tup_full_list_parts"].items():
                    yield tup[0]
                    for path_to_keys_sorted in tup[1]:
                        yield path_to_keys_sorted

            if include_tmp_folder and self.dir_tmp_path is not None:
                try:
                    yield self.dir_tmp_path
                    self.dir_tmp_path = None
                except OSError:
                    pass

    def delete_tmp(self, remove_tmp_folder=True):
        """
        Delete temporal files created

        :param remove_tmp_folder: True to delete tmp folder. By default: True
        :return: None
        """
        delete_tmp_folder(self.dir_tmp_path,
                          secure_paths_to_del=self.tmp_paths(include_tmp_folder=remove_tmp_folder))

    def clear(self, remove_tmp_folder=True):
        """
        Clear file and delete temporal files (use `clear` to use instance state)

        :param remove_tmp_folder: True to delete tmp folder. By default: True
        :return: None
        """
        self.delete_tmp(remove_tmp_folder=remove_tmp_folder)

    def get_dict_saved_info(self):
        """
        Get dict with general information. If not exist, create a new empty

        :return: dict info of data saved, if not exist create one dict new with default data
        """
        try:
            return load_single_value(Path(self.dir_tmp_path, "dict_info.db"))
        except FileNotFoundError:
            # Create a new dict_info
            return {
                "dict_ipid_tup_full_list_parts": None,
                "reverse": False,
                "empty": True,
                "multiprocessing": False,
                "total_counter": 0
            }

    def set_dict_saved_info(self, dict_to_save):
        """
        Save in disk a new dict with general information.

        :param dict_to_save: dict to save
        :return: None
        """
        path_to_dict_info = Path(self.dir_tmp_path, "dict_info.db")
        try:
            path_to_dict_info.unlink()
        except FileNotFoundError:
            pass
        dump_single_value(path_to_dict_info, dict_to_save)

    def save_and_sort_mono(self,
                           it_values,
                           func_key_sort=None,
                           func_value=None,
                           reverse=False,

                           count_insert_to_check=1000000,
                           max_process_size=1024*1024*1024,

                           ensure_space=False):
        """
        Consume an iterable to be sorted. Take analysis in this iterable and save to disk (in temporal files).
        Mono thread, this one execute in current thread.

        :param it_values: iterator of values
        :param func_key_sort: function to extract the key of each value of it_values.
            If None is full value of it_values. By default: None
        :param func_value: function to extract the value of each value of it_values.
            If None is full value of it_values. By default: None
        :param reverse: True to reverse sort. By default: False
        :param count_insert_to_check: counter to check if process have more size in memory than max_process_size.
            By default: 1000000
        :param max_process_size: max size in bytes to dump cache memory values to disk
            (only execute when count_insert_to_check is reached). If None, then not import psutil and then only
            check with count_insert_to_check. By default: 1024*1024*1024  # 1Gib
        :param ensure_space: True to ensure disk space but is slowly. If not space then process launch warning message
            and wait for space. If False, then get and IOException if not enough space
        :return: self
        """
        if func_key_sort is None:
            def func_key_sort_default(key):
                return key

            func_key_sort = func_key_sort_default

        if func_value is None:
            def func_value_default(value):
                return value

            func_value = func_value_default

        get_process_memory = _get_func_process_memory(max_process_size is not None)

        dict_info = self.get_dict_saved_info()

        def sort_cache_and_save(key_file, dict_to_save, reverse):
            if dict_to_save:
                def gen_key_value_sorted(dict_to_save, reverse):
                    # When it is sorted, it assign each key to his value
                    for key in sorted(dict_to_save.keys(), reverse=reverse):
                        yield key, dict_to_save[key]

                mpath_to_keys_sorted = Path(self.dir_tmp_path, "keys_sorted_{}.db".format(key_file))
                quick_dump_items(mpath_to_keys_sorted, gen_key_value_sorted(dict_to_save, reverse))

                del dict_to_save

                return mpath_to_keys_sorted

            return None

        list_paths_to_keys_sorted = list()

        def evt_err_space_dump(_, time_to_retry, err):
            logging.error("[NOT SPACE ON DEVICE (WAITING TO CONTINUE {} SECONDS) -> "
                          "ppid:{} | pid:{}]: {}".format(time_to_retry,
                                                         os.getppid(),
                                                         os.getpid(), err))

        path_full_data = Path(self.dir_tmp_path, "full_data.db")
        with EasyBinaryFile(path_full_data, mode='ab') as f_full_data_open:
            dict_keysortable_fpositions = {}

            if dict_info["dict_ipid_tup_full_list_parts"] is None:
                count_key_file = 0
            else:
                count_key_file = dict_info["dict_ipid_tup_full_list_parts"][-1][2]

            cache_bulk_counter = 0
            total_bulk_counter = 0

            dict_info["reverse"] = reverse
            dict_info["empty"] = False
            dict_info["multiprocessing"] = False

            for value in it_values:
                mkey = func_key_sort(value)
                value = func_value(value)

                start_cursor_pos = f_full_data_open.get_cursor_position()
                f_full_data_open.dump(value,
                                      ensure_space=ensure_space,
                                      fun_err_space=evt_err_space_dump)

                try:
                    fpositions = dict_keysortable_fpositions[mkey]
                    fpositions.append(start_cursor_pos)
                    dict_keysortable_fpositions[mkey] = fpositions
                except KeyError:
                    dict_keysortable_fpositions[mkey] = [start_cursor_pos]

                cache_bulk_counter += 1
                if count_insert_to_check is not None \
                        and count_insert_to_check < cache_bulk_counter:
                    process_memory = get_process_memory()
                    total_bulk_counter += cache_bulk_counter
                    cache_bulk_counter = 0
                    logging.debug("[MEMORY CHECK -> ppid:{} | pid:{}]: mem<{}>, els<{}>".format(os.getppid(),
                                                                                                os.getpid(),
                                                                                                process_memory,
                                                                                                total_bulk_counter))

                    if process_memory == -1 or max_process_size < process_memory:
                        # If process have more size than limit, then cache is saved to disk and set cache to empty
                        count_key_file += 1
                        logging.debug("[SAVING MEMORY -> ppid:{} | pid:{}]: key<{}>".format(os.getppid(),
                                                                                            os.getpid(),
                                                                                            count_key_file))
                        path_to_keys_sorted = sort_cache_and_save(count_key_file, dict_keysortable_fpositions, reverse)
                        if path_to_keys_sorted is not None:
                            list_paths_to_keys_sorted.append(path_to_keys_sorted)
                        dict_keysortable_fpositions = {}

            total_bulk_counter += cache_bulk_counter

        path_to_keys_sorted = sort_cache_and_save(count_key_file+1, dict_keysortable_fpositions, reverse)
        if path_to_keys_sorted is not None:
            list_paths_to_keys_sorted.append(path_to_keys_sorted)

        next_id_path_to_keys_sorted = count_key_file + 1
        if dict_info["dict_ipid_tup_full_list_parts"] is None:
            new_total_bulk_counter = total_bulk_counter
            dict_info["dict_ipid_tup_full_list_parts"] = {-1: (path_full_data,
                                                               list_paths_to_keys_sorted,
                                                               next_id_path_to_keys_sorted,
                                                               new_total_bulk_counter)}
        else:
            prev_list_paths_keys_sorted = dict_info["dict_ipid_tup_full_list_parts"][-1][1]
            prev_bulk_counter = dict_info["dict_ipid_tup_full_list_parts"][-1][3]
            new_total_bulk_counter = prev_bulk_counter+total_bulk_counter
            dict_info["dict_ipid_tup_full_list_parts"][-1] = (path_full_data,
                                                              prev_list_paths_keys_sorted+list_paths_to_keys_sorted,
                                                              next_id_path_to_keys_sorted,
                                                              prev_bulk_counter+total_bulk_counter)

        dict_info["total_counter"] = new_total_bulk_counter

        self.set_dict_saved_info(dict_info)

        return self

    def __iter__(self):
        """
        :return: Sorted iterable of lines
        """
        return self.values()

    def items(self):
        """
        Get a sorted iterable from disk to return sorted tuples of key and line, in each petition this get one sorted
        tuple.

        Note: If you consume full iterable then and delete_to_end is False, then you can to iterate more than one
        time and you need to delete temporal files by hand with clean.

        Note: This is a wrapper of iter_with_key().

        :return: Sorted iterable of tuples key and value
        """
        return self.iter_with_key(delete_to_end=self.delete_to_end,
                                  enable_multiprocessing=self.iter_multiprocessing,
                                  queue_max_size=self.iter_m_queue_max_size)

    def values(self):
        """
        Get a sorted iterable from disk to return sorted lines, in each petition this get one sorted line.

        Note: If you consume full iterable then and delete_to_end is False, then you can to iterate more than one
        time and you need to delete temporal files by hand with clean.

        Note: This is a wrapper of iter_with_key().

        :return: Sorted iterable of values
        """
        def _iter_values(self):
            for _, value in self.iter_with_key(delete_to_end=self.delete_to_end,
                                               enable_multiprocessing=self.iter_multiprocessing,
                                               queue_max_size=self.iter_m_queue_max_size):
                yield value
        return _iter_values(self)

    def keys(self):
        """
        Get a sorted iterable from disk to return sorted keys of lines, in each petition this get one sorted key.

        Note: If you consume full iterable then and delete_to_end is False, then you can to iterate more than one
        time and you need to delete temporal files by hand with clean.

        Note: This is a wrapper of iter_with_key().

        :return: Sorted iterable of keys sorted
        """
        def _iter_keys(self):
            for key, _ in self.iter_with_key(delete_to_end=self.delete_to_end,
                                             enable_multiprocessing=self.iter_multiprocessing,
                                             queue_max_size=self.iter_m_queue_max_size):
                yield key
        return _iter_keys(self)

    def join_multiprocess(self):
        """
        Wait to end of all processes.

        This call is necessary if multiprocess is enable,
        because ensure end of all processes and need to update dict_info

        :return: dict of updated dict_info
        """
        for p in self.dict_num_procceses.values():
            p.join()

        self.dict_num_procceses = dict()

        dict_info = self.get_dict_saved_info()

        if self.proxy_dict is None:
            return dict_info
        else:
            proxy_dict = dict(self.proxy_dict)
            self.proxy_dict = None

            total_counter = dict_info["total_counter"]
            for ipid in proxy_dict.keys():
                total_counter += proxy_dict[ipid][3]

                if dict_info["dict_ipid_tup_full_list_parts"]:
                    try:
                        prev_list_paths_keys_sorted = dict_info["dict_ipid_tup_full_list_parts"][ipid][1]
                        prev_total_bulk_counter = dict_info["dict_ipid_tup_full_list_parts"][ipid][3]
                        proxy_dict[ipid] = (proxy_dict[ipid][0],
                                            prev_list_paths_keys_sorted + proxy_dict[ipid][1],
                                            proxy_dict[ipid][2],
                                            prev_total_bulk_counter + proxy_dict[ipid][3])
                    except KeyError:
                        pass

            dict_info["dict_ipid_tup_full_list_parts"] = proxy_dict
            dict_info["total_counter"] = total_counter

            self.set_dict_saved_info(dict_info)

            gc.collect()

            return dict_info

    def save_and_sort_multiprocess(self,
                                   it_values,
                                   func_key_sort=None,
                                   func_value=None,
                                   reverse=False,

                                   count_insert_to_check=1000000,
                                   max_process_size=1024*1024*1024,

                                   max_process=None,
                                   queue_max_size=1000,

                                   ensure_space=False):
        """
        Consume a iterable to be sorted in multiprocess way.
        Take analysis in this iterable and save to disk (in temporal files).

        :param it_values: iterator of values
        :param func_key_sort: function to extract the key of each value of it_values.
            If None is full value of it_values. By default: None
        :param func_value: function to extract the value of each value of it_values.
            If None is full value of it_values. By default: None
        :param reverse: True to reverse sort. By default: False
        :param count_insert_to_check: counter to check if process have more size in memory than max_process_size.
            By default: 1000000
        :param max_process_size: max size in bytes to dump cache memory values to disk
            (only execute when count_insert_to_check is reached). If None, then not import psutil and then only
            check with count_insert_to_check. By default: 1024*1024*1024  # 1Gib
        :param max_process: number of process to execute. If None then it is number of CPUs. By default: None
        :param queue_max_size: max number of elements in queue. If None then is the max by default. By default: 1000
        :param ensure_space: True to ensure disk space but is slowly. If not space then process launch warning message
            and wait for space. If False, then get and IOException if not enough space
        :return: self
        """
        logging.debug("[ROOT START -> ppid:{} | pid:{}]".format(os.getppid(), os.getpid()))

        if func_key_sort is None:
            def func_key_sort_default(key):
                return key

            func_key_sort = func_key_sort_default

        if func_value is None:
            def func_value_default(value):
                return value

            func_value = func_value_default

        dict_info = self.get_dict_saved_info()

        dict_info["reverse"] = reverse
        dict_info["empty"] = False
        dict_info["multiprocessing"] = True

        self.set_dict_saved_info(dict_info)

        if queue_max_size is None:
            proxy_queue = multiprocessing.Queue()
        else:
            proxy_queue = multiprocessing.Queue(queue_max_size)

        self.join_multiprocess()

        proxy_end_event = multiprocessing.Event()
        proxy_end_event.clear()

        proxy_start_event = multiprocessing.Event()
        proxy_start_event.clear()

        try:
            try:
                first_value = next(it_values)
            except TypeError:
                it_values = (v for v in it_values)
                first_value = next(it_values)

            logging.debug("[ROOT INITIALIZE CHILDS -> ppid:{} | pid:{}]".format(os.getppid(), os.getpid()))

            self.proxy_dict = self.manager.dict()
            for procesnum in range(0, multiprocessing.cpu_count() if max_process is None else max_process):

                if dict_info["dict_ipid_tup_full_list_parts"] is None:
                    next_id_path_to_keys_sorted = 0
                else:
                    try:
                        next_id_path_to_keys_sorted = dict_info["dict_ipid_tup_full_list_parts"][procesnum][2]
                    except KeyError:
                        next_id_path_to_keys_sorted = 0

                process = multiprocessing.Process(target=_mprocess, args=(proxy_queue,
                                                                          proxy_start_event,
                                                                          proxy_end_event,
                                                                          procesnum,

                                                                          self.dir_tmp_path,
                                                                          self.proxy_dict,

                                                                          count_insert_to_check,
                                                                          max_process_size,
                                                                          reverse,
                                                                          next_id_path_to_keys_sorted,

                                                                          ensure_space,
                                                                          self.logging_level))
                process.daemon = True
                process.start()
                self.dict_num_procceses[procesnum] = process

            logging.debug("[ROOT START DATA ITERATION -> ppid:{} | pid:{}]".format(os.getppid(), os.getpid()))

            gc.collect()
            proxy_start_event.set()

            count = 1
            proxy_queue.put((func_key_sort(first_value), func_value(first_value)))
            for count, value in enumerate(it_values, 2):
                proxy_queue.put((func_key_sort(value), func_value(value)))

            logging.debug("[ROOT LINES PROCESED: {} -> ppid:{} | pid:{}]".format(count, os.getppid(), os.getpid()))

            proxy_queue.close()
            proxy_end_event.set()
        except StopIteration:
            pass

        gc.collect()
        logging.debug("[ROOT END -> ppid:{} | pid:{}]".format(os.getppid(), os.getpid()))

        return self

    def save_and_sort(self,
                      it_values,
                      func_key_sort=None,
                      func_value=None,
                      reverse=False,

                      count_insert_to_check=1000000,
                      max_process_size=1024 * 1024 * 1024,

                      max_process=None,
                      queue_max_size=1000,

                      ensure_space=False):
        """
        Choose save_and_sort_multiprocess of save_and_sort_mono depend on max_process

        :param it_values: iterator of values
        :param func_key_sort: function to extract the key of each value of it_values.
            If None is full value of it_values. By default: None
        :param func_value: function to extract the value of each value of it_values.
            If None is full value of it_values. By default: None
        :param reverse: True to reverse sort. By default: False
        :param count_insert_to_check: counter to check if process have more size in memory than max_process_size.
            By default: 1000000
        :param max_process_size: max size in bytes to dump cache memory values to disk
            (only execute when count_insert_to_check is reached). If None, then not import psutil and then only
            check with count_insert_to_check. By default: 1024*1024*1024  # 1Gib
        :param max_process: number of process to execute. If None then it is number of CPUs. By default: None
        :param queue_max_size: max number of elements in queue. If None then is the max by default. By default: 1000
        :param ensure_space: True to ensure disk space but is slowly. If not space then process launch warning message
            and wait for space. If False, then get and IOException if not enough space
        :return:
        """
        if max_process is 0:
            self.save_and_sort_mono(it_values=it_values,
                                    func_key_sort=func_key_sort,
                                    func_value=func_value,
                                    reverse=reverse,
                                    count_insert_to_check=count_insert_to_check,
                                    max_process_size=max_process_size,
                                    ensure_space=ensure_space)
        else:
            self.save_and_sort_multiprocess(it_values=it_values,
                                            func_key_sort=func_key_sort,
                                            func_value=func_value,
                                            reverse=reverse,
                                            count_insert_to_check=count_insert_to_check,
                                            max_process_size=max_process_size,
                                            max_process=max_process,
                                            queue_max_size=queue_max_size,
                                            ensure_space=ensure_space)
        return self

    def iter_with_key(self,
                      delete_to_end=True,
                      enable_multiprocessing=False,
                      queue_max_size=1000):
        """
        Get a sorted iterable from disk to return tuples of key and sorted line, in each petition this get one
        sorted line.

        If you consume full iterable then and delete_to_end is False, then you can to iterate more than one times.

        Note: you can use a wrappers pre-build that remove key and only return the sorted line in iter()

        :param delete_to_end: True to delete tmps files in the end of consumption of sorted data. If False or
                              if you not consume full returned iterable, then you may to delete tmps files by hand
                              (you can carry out with clear() method). By default: True
        :param enable_multiprocessing: True to get and prepare data in other process, False to use this one.
            By default: False
        :param queue_max_size: (only if enable_multiprocessing is True) max number of elements in queue. If None
            then is the max by default. By default: 1000
        :return None
        """
        dict_info = self.get_dict_saved_info()

        if dict_info["empty"]:
            raise StopIteration

        reverse = dict_info["reverse"]
        if dict_info["multiprocessing"]:
            gc.collect()
            dict_info = self.join_multiprocess()

        dict_ipid_tup_full_list_parts = dict_info["dict_ipid_tup_full_list_parts"]

        if enable_multiprocessing:
            if queue_max_size is None:
                proxy_queue_iter = multiprocessing.Queue()
            else:
                proxy_queue_iter = multiprocessing.Queue(queue_max_size)

            proxy_start_event_iter = multiprocessing.Event()
            proxy_start_event_iter.clear()

            proxy_end_event_iter = multiprocessing.Event()
            proxy_end_event_iter.clear()

            logging.debug("[ROOTG INITIALIZE CHILD -> ppid:{} | pid:{}]".format(os.getppid(), os.getpid()))

            process = multiprocessing.Process(target=_mprocess_iter, args=(proxy_queue_iter,
                                                                           proxy_start_event_iter,
                                                                           proxy_end_event_iter,
                                                                           dict_ipid_tup_full_list_parts,
                                                                           reverse,
                                                                           self.logging_level))

            process.daemon = True
            process.start()
            loop_enable = True
            times_waiting = 0

            logging.debug("[ROOTG START -> ppid:{} | pid:{}]".format(os.getppid(), os.getpid()))

            while loop_enable:
                try:
                    yield proxy_queue_iter.get(timeout=0.1)
                except queue.Empty:
                    loop_enable = not (proxy_end_event_iter.is_set() and proxy_queue_iter.empty())
                    if loop_enable:
                        times_waiting += 1
                        if proxy_start_event_iter.is_set():
                            time_to_retry = 0.1 * times_waiting
                            gc.collect()
                            time.sleep(time_to_retry)
                        else:
                            logging.debug("[ROOTG WAIT GETTER -> ppid:{} | pid:{}]".format(os.getppid(),
                                                                                           os.getpid()))
                            gc.collect()
                            proxy_start_event_iter.wait()
                            logging.debug("[ROOTG RESUME WAIT -> ppid:{} | pid:{}]".format(os.getppid(),
                                                                                           os.getpid()))

            if process.is_alive():
                logging.debug("[ROOTG FORCE TO TERMINATE LIVE CHILD -> ppid:{} | pid:{}]".format(os.getppid(),
                                                                                                 os.getpid()))
                process.terminate()

            logging.debug("[ROOTG LOOP STOP -> ppid:{} | pid:{}]".format(os.getppid(),
                                                                         os.getpid()))
        else:
            for tup_key_loadpickle in _iter_get_data_from_files(dict_ipid_tup_full_list_parts, reverse):
                yield tup_key_loadpickle

        if delete_to_end:
            self.delete_tmp(remove_tmp_folder=True)

    def __len__(self):
        """
        Get number of elements in this structure
        :return: length
        """
        dict_info = self.get_dict_saved_info()
        if dict_info["multiprocessing"]:
            gc.collect()
            dict_info = self.join_multiprocess()
        return dict_info["total_counter"]
