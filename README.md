# Sort in disk
Sort a bulk of data in disk (RAM memory free) and optionally in parallel (Use available resources).

It is recomendable use `sorted_in_disk` to sort bulk of data to disk, because `sorted` save data RAM memory that is 
pretty limited.

This is a good way to work with big data without RAM memory limits (hard disk have more size and cheaper than 
RAM memory).

`sorted_in_disk` algorithm return data as soon as posible and only work the minimum necessary, that mean sorted is in 
real time while return sorted data to you (in injection time perform a quick and minimum sorted work).


## Installation from PYPI
You can find last version of project in: https://pypi.org/project/sort-in-disk/

Command to install:
```
pip install sort-in-disk
```

This project has one dependency (this is auto-imported with PIP): `easy_binary_file`

You can use but it is not mandatory `psutil` package. Only to check current memory of process and save to disk (if you
do not have `psutil`, then when certain number of items will be reached, then save to disk).


## Quick use
Simply import and use.

Example of inject a list of string lines with pipe `|` delimiter where key is first part:
```python
from sorted_in_disk import sorted_in_disk

unsorted_data = [
    "key3|value3",
    "key1|value1",
    "key2|value2"
]

sid = sorted_in_disk(unsorted_data,
                     key=lambda line: line.split("|")[0])

for element in sid:
    print(element)
```

You can read from `sorted_in_disk` like a dict style with `values`, `keys`, `items` methods. Example:
```python
for key, value in sorted_in_disk(...).items():
    pass
```

`SortInDisk` implements `__iter__` (with `values`). Example:
```python
for value in sorted_in_disk(...):
    pass
```


## Quick how to use in comparison of sorted method
`sorted_in_disk` is similar to oficial `sorted` method (https://docs.python.org/3/library/functions.html#sorted), with
`iterable`, `key` and `reverse` args. 

Example for comparison of common `sorted` use: 
```python
unsorted_data = [
    "key3|value3",
    "key1|value1",
    "key2|value2"
]

for element in sorted(unsorted_data,
                      key=lambda line: line.split("|")[0]):
    print(element)
```

Example of `sorted_in_disk` use:
```python
for element in sorted_in_disk(unsorted_data,
                              key=lambda line: line.split("|")[0]):
    print(element)
```


## Import
Import functions, class or all.
```python
from sorted_in_disk import sorted_in_disk
```

## Main
This is a review of class and functions content inside.

**You have complete docstring documentation in code and more examples/tests in doctest format.**


### Function:
 * `sorted_in_disk`: Main method to create a SortedInDisk object configured

Helpers methods public to take advantage of this package (those are not main package use): 
 * `create_tmp_folder`: Helper to create a temporal folder
 * `delete_tmp_folder`: Delete temporal files created
 
 
#### sorted_in_disk args
 * `iterable`: iterable to sort in disk.
        This iterable should by a generator for big data due to RAM memory limitation.
 * `key`: key specifies a function of one argument that is used to extract a comparison key from each element in
        iterable (for example, `key=str.lower` or key=lambda e: `e.split(",")[3]`).
        The default value is None (compare the elements directly).
 * `reverse`: reverse is a boolean value.
        If set to `True`, then the list elements sorted as if each comparison were reversed.
 * `tmp_dir`: Path to dir where save temporal files. If None this creates a folder and overwrite if
        exist previously. By default: create a sortInDiskTmps folder in the current directory.
 * `ensure_different_dirs`: True to add incremental counter to folder if exists previously.
        Useful if you use several instances of SortedInDisk at same time.
        Note: conflict if `append` is `True`, because this creates a new name and not delete
        previously file (example if True: if exist `/path/folder/` it create a new `/paht/folder(1)/`). 
        By default: `False`
 * `append`: True to clean folder tmp_dir if existe previously. By default: `False`
 * `only_one_read`: True to clean folder tmp_dir when you consume all data. If it is True only works if you read all 
        returned data, if you not read all, then you need to clear instance to auto. By default: `True`
 * `count_insert_to_check`: counter to check if process have more size in memory than max_process_size.
        By default: `1000000`
 * `max_process_size`: max size in bytes to dump cache memory values to disk
        (only execute when `count_insert_to_check` is reached). If None, then not import psutil and then only
        check with `count_insert_to_check`. By default: `1024*1024*1024`   # 1Gib
 * `max_process`: number of process to execute. If None then it is number of CPUs. By default: `0`
 * `queue_max_size`: (only if `max_process!=0`) max number of elements in queue. If None then is the max by default.
        By default: `1000`
 * `iter_multiprocessing`: `True` to get and prepare data in other process, `False` to use this one.
        By default: `False`
 * `iter_m_queue_max_size`: (only if `enable_multiprocessing` is `True`) max number of elements in queue. If None
        then is the max by default. By default: `1000`


### Class:
 * `SortedInDisk`: Instance an object to work with data in a specific temporal folder.
    * `save_and_sort`: Choose `save_and_sort_multiprocess` of `save_and_sort_mono` depend on max_process
    * `__iter__`: Sorted iterable of lines (same as `values` method).
    * `__len__`: Get number of elements in this structure.
    * `items`: Get a sorted iterable from disk to return sorted tuples of key and line, in each petition this get 
               one sorted
    * `values`: Get a sorted iterable from disk to return sorted lines, in each petition this get one sorted line.
    * `keys`: Get a sorted iterable from disk to return sorted keys of lines, in each petition this get one sorted key.
    * `join_multiprocess`: Wait to end of all processes (only it is important if multiprocess injection is enable).
    * `clear`: Clear file and delete temporal files
    * Other methods invoked in previous methods (public for package extension proposals): 
        * `delete_tmp`: Delete temporal files created (use `clear` to use instance state)
        * `get_dict_saved_info`: Get dict with general information. If not exist, create a new empty.
        * `set_dict_saved_info`: Save in disk a new dict with general information.
        * `save_and_sort_multiprocess`: Consume an iterable to be sorted in multiprocess way. Take analysis in this 
                                        iterable and save to disk (in temporal files).
        * `save_and_sort_mono`: Consume an iterable to be sorted. Take analysis in this iterable and save to disk 
                                (in temporal files). Mono-thread, this one execute in the current thread.


## Algorithm and background work
`sort_in_disk` method creates one instance of `SortInDisk` object.

The sort in disk method have a life of cicle:
 * **Clean before**: Check if not exist previous structure in disk and clean if it is necessary
 * **Injection**: Data unsorted is inject in this structure
 * **Reader**: Data sorted is read from this structure
 * **Clean after**: clean if it is necessary


### Clean before
One live instance of `SortInDisk` own a temporal dir path to create temporal files.

No more than one instance of `SortInDisk` at time may own to same temporal dir path.

For this reason is important check if not exist previous temporal files generated 
for `sort_in_disk` in same folder in disk.

One instance of `SortInDisk can:
 * **Create** a new temporal dir: Not exist other temporal dir with same name. This is a clean state.
 * **Append** more data to previous temporal dir: If exist a temporal dir with same name and not other instance that
    pointing to this dir are live, then this new instance can own previous temporal dir to append more data. 
    This is enable if `sorted_in_disk` have arg `append` to `True`.
 * **Delete** previous temporal dir: delete all, similar to clean state and begin.
    This is enable if `sorted_in_disk` have arg `append` to `False`.
 * **Rename** a new temporal dir: in case of exist a previous temporal dir with same name, create a new temporal dir
    with an increment number and use this new as clean state.
    This is enable if `sorted_in_disk` have arg `ensure_different_dirs` to `True`.


### Injection
Data can be injected in multiprocess or mono-process (in the main thread) way.

Each thread to create a new raw file with all injected data.

Also, each thread create a cache in RAM memory with index of keys to point data and extra information.

By other hand, it is created one file more with general information.

If you have `psutil` installed, then if this cache has more size than `max_process_size` and `count_insert_to_check` 
is reached (to reduce check and increase injection speed), then keys cached are pre-sorted and saved to disk and 
create a new empty cache.

In other way, if you do not have `psutil` installed (or if `max_process_size` is `None`), then if 
`count_insert_to_check` is reached, then keys cached are pre-sorted and saved to disk and create a new empty cache.

You can enable mono-process if `sorted_in_disk` have arg `max_process` to `0`. To start and normal use I recommend you
this one.

You can enable multiprocess if `sorted_in_disk` have arg `max_process` to `None` to auto-determinate physical 
processors in current system or define a number of processors. Depend on your velocity of main-thread reading data you 
take advantage of multiprocess or not. If multiprocess is enable, then when main-thread end of inject data next code 
will be not blocked; it is necessary join with `join_multiprocess()`, some methods call to `join_multiprocess()` as
`__iter__`, `values`, `keys`, `items` or `__len__`. Multiprocess have a queue determine with `queue_max_size` to work 
with data between processes (it can be regulated, but it is better not to put neither too much to have enough 
RAM memory nor too little to do not have idle process).

To sum up process control:
 * `max_process = 0` is mono-process injection.
 * `max_process = None` is multi-process injection, one process per physical processor detected in system.
 * `max_process = 4` is multi-process injection, 4 process to inject data.

To sum up memory control:
 * `max_process_size = None` or not `psutil`, and `count_insert_to_check = 1000000` then save index to disk when 
 1000000 values binjected, clean and continue.
 * `max_process_size = 1024*1024*1024` or not `psutil`, and `count_insert_to_check = 1000000` then check if 1 GB are
 reached each 100000 values inject, if precess reach 1 GB, then index save to disk, clean and continue.
 * `queue_max_size` only if multi-process injection is enable. If `queue_max_size=1000` then main process put in the 
 queue max 1000 values, those values will be taking by consumption processes. If data does not fit in the queue then 
 main  process will go to idle until the queue have space.


### Reader
Data can read in multiprocess or mono-process (in the main thread) way.

In one thread, the header of data read from all files and sorted these headers. Then it return the first header,
choose next header from file and sort other time all headers and repeat.

In this point have data in real time (data streaming). Data is not necessary full sorted to sure a correct sort.

If full data read, then file will be closed (and go to after clean step in life cycle).

You can enable mono-process if `sorted_in_disk` have arg `iter_multiprocessing` to `False`.

You can enable multiprocess if `sorted_in_disk` have arg `iter_multiprocessing` to `True`. In this case, data is 
sorted and enqueue until these sorted data will read in main-thread. This is useful if we want to take advantage of
the data preparation between data consumption. Multiprocess have a queue determine with `iter_m_queue_max_size` to 
work with data between processes (it can be regulated, but it is better not to put neither too much to have enough 
RAM memory nor too little to do not have idle process).

To sum up:
 * `iter_multiprocessing = False` is mono-process reader.
 * `iter_multiprocessing = True` is multi-process reader (one process more to prepare a bulk of sorted data).
 * `iter_m_queue_max_size` only if multi-process reader is enable. If `iter_m_queue_max_size=1000` then second process 
 have a queue of 1000 positions to put data, if data does not fit in the queue then process will go to idle until queue 
 main process take data from the queue and leave space.

### Clean after
Depend on we want append more data or maintain sorted work in disk, maybe we do not want clean anything.

In the end on one instance of `SortInDisk can:
 * **Maintain** the temporal dir: This not delete temporal files to use other times in the future. If we want clear 
    data in the future, we will be able to delete the temporal folder from disk or use `sorted_in_disk` other time 
    and change the `only_one_read` to `True` or use `clear` method by hand. 
    To enable maintain the temporal dir if `sorted_in_disk` have arg `only_one_read` to `False`.
 * **Delete** the temporal dir: delete all temporal data, clean state.
    This is enable if `sorted_in_disk` have arg `only_one_read` to `True`.


## About performance:
You can use many times one sorted work from disk (if `only_one_read` is `False`), but this is not a data base. When 
data is in disk have a minimum sorted work, but it is not finally sort. When you read data perform complete sort in 
real time (to have sorted data as soon as posible). Due to, if you want use several times sorted work, maybe is good
idea save result to file and read from this one (if you want to take advantage of same read iteration, with Python 
generators in streaming configuration you can save data to disk while you use data at same time).


## To prevent bottleneck
I recommend to start testing with mono-process in injection and read. After, you can test with multiprocess,
because depend on system (hardware and software) multiprocess is quicker or slower than mono-process due to process
idle, extra processor to enqueue data, etc.

It is better save and read data in SSD disk due to velocity. If you read file it is better to read from other 
disk or SSD disk.

If you think velocity of disk is enough, you can enable multiprocess and test with different configurations. If
disk velocity is not enough then is quickly and easy use mono-process.

To read, if you need to perform hard operations with each returned data, it is better to enable multi-process in read. 
In other case, it is quick enough mono-process.


## Note about this algorithm
This is an own algorithm create with my own experience in big data. This is invented by me and my experience, I do not 
investigated thirds or used any others to create this one.


## Limitations
Not mix mono-process and multiprocess configuration in same temporal file (only if you use `only_one_read=False` 
and `append=True`). Each mode (mono-process or multiprocess) work similar but save data in different ways to improve
performance of each thread.
