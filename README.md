# Sort in disk
Sort a bulk of data in disk (RAM memory free) and optionally in parallel (Use available resources).

It is recomendable use `sorted_in_disk` to sort bulk of data to disk, because `sorted` save data RAM memory that is 
pretty limited.

This is a good way to work with big data without RAM memory limits (hard disk have more size and cheaper than 
RAM memory).

`sorted_in_disk` algorithm return data as soon as posible and only work the minimum necessary, that mean sorted is in 
real time while return sorted data to you (in injection time perform a quick and minimum sorted work).


## Table of contents
 * [Technologies](#technologies)
 * [Setup](#setup)
 * [Quick use](#quick-use)
 * [Algorithm lifecycle](#algorithm-lifecycle)
 * [Performance](#performance)
 * [Documentation](#documentation)
 * [Limitations](#limitations)
 * [About this algorithm](#about-this-algorithm)
 * [Is useful for you?](#is-useful-for-you)
 * [Contact](#contact)


## Technologies
Project create with:
 * Python 3


## Setup
Install it locally using PyPI (last release in https://pypi.org/project/sorted-in-disk/):

```
pip install sorted-in-disk
```

### Information about dependencies
Mandatory dependencies (auto-import with PIP):
 * `easy_binary_file`: to manage binary files
 * `quick_queue`: to pass quick values between processes

Optional dependencies (only hand-installable):
 * `psutil`: to check current memory of process and save to disk (if you do not have `psutil`, then when certain 
             number of items will be reached, then save to disk). Not mandatory because I detected some 
             incompatibilities issues in some systems (`psutil` need extra permissions). Optionally you can install 
             `psutil` with:
```
pip install psutil
```


## Quick use
Import:
```python
from sorted_in_disk import sorted_in_disk
```

Example of inject a list of string lines with pipe `|` delimiter where key is first part:
```python
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

If you have a big text file where each line has a key to sort, you can read file line to line quickly with
`read_iter_from_file` in `sorted_in_disk.utils` package to pass an iterable for lines in the file (this way not read 
full file in one time, read line per line in a generator; only consume one line size in RAM memory). 
Example (key supposes file have lines similar to "key1|value1"):
```python
from sorted_in_disk.utils import read_iter_from_file

iterable_with_unsorted_data = read_iter_from_file("path/to/your/file/to/read")

sid = sorted_in_disk(iterable_with_unsorted_data,
                     key=lambda line: line.split("|")[0])

for sorted_line in sid:
    print(sorted_line)
```


### In comparison with sorted method
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


## Algorithm lifecycle
`sort_in_disk` method creates one instance of `SortInDisk` object.

The sort in disk method have a lifecycle:
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

If you have `psutil` installed, then if this cache has more size than `max_write_process_size` and 
`count_insert_to_check` is reached (to reduce check and increase injection speed), then keys cached are pre-sorted 
and saved to disk and create a new empty cache.

In other way, if you do not have `psutil` installed (or if `max_write_process_size` is `None`), then if
`count_insert_to_check` is reached, then keys cached are pre-sorted and saved to disk and create a new empty cache.

You can enable mono-process if `sorted_in_disk` have arg `write_processes` to `0`.

You can enable multiprocess if `sorted_in_disk` have arg `write_processes` to `None` to auto-determinate physical
processors in current system or define a number of processors. Depend on your velocity of main-thread reading data you 
take advantage of multiprocess or not. If multiprocess is enable, then when main-thread end of inject data next code 
will be not blocked; it is necessary join with `join_multiprocess()`, some methods call to `join_multiprocess()` as
`__iter__`, `values`, `keys`, `items` or `__len__`. Multiprocess have a queue determine with `queue_max_size` to work 
with data between processes (it can be regulated, but it is better not to put neither too much to have enough 
RAM memory nor too little to do not have idle process).

To sum up process control:
 * `write_processes = 0` is mono-process injection.
 * `write_processes = None` is multi-process injection, one process per physical processor detected in system.
 * `write_processes = 4` is multi-process injection, 4 process to inject data.
 * `write_processes = ["path/tmp_process_1", "path/tmp_process_2", "path/tmp_process_3"]` is multi-process injection, 
    3 process to inject data, one per directory.

To sum up memory control:
 * `max_write_process_size = None` or not `psutil`, and `count_insert_to_check = 1000000` then save index to disk when
 1000000 values binjected, clean and continue.
 * `max_write_process_size = 1024*1024*1024` or not `psutil`, and `count_insert_to_check = 1000000` then check if 1 GB are
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

You can enable mono-process if `sorted_in_disk` have arg `read_process` to `False`.

You can enable multiprocess if `sorted_in_disk` have arg `read_process` to `True`. In this case, data is
sorted and enqueue until these sorted data will read in main-thread. This is useful if we want to take advantage of
the data preparation between data consumption. Multiprocess have a queue determine with `iter_m_queue_max_size` to 
work with data between processes (it can be regulated, but it is better not to put neither too much to have enough 
RAM memory nor too little to do not have idle process).

To sum up:
 * `read_process = False` is mono-process reader.
 * `read_process = True` is multi-process reader (one process more to prepare a bulk of sorted data).
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


## Performance

### Hardware
`sorted_in_disk` take advantage of you Hardware, then this is more quickly if it is well configured and run in best
hardware.

You need to know:
* **RAM memory**: it is used to accelerate injection, multiprocess queues and pre-sorted, when condition of variables 
`count_insert_to_check` and `queue_max_size` are met, then will dump to disk to liberate RAM memory. In conclusion,
if you have more RAM memory you can configure more `count_insert_to_check` and `queue_max_size` to work more in 
faster RAM memory. Example:
```python
sid = sorted_in_disk(...,
                     count_insert_to_check=1000000,
                     max_write_process_size=1024 * 1024 * 1024)
```

* **Processes**: To inject/write is a good idea divide work in processes one per processor 
(with `write_processes` variable) if it is possible to increase the velocity of sorted in disk. But, you need to
calibrate the best number of processes in your system if you want to reach the max performance; because, maybe more
processes are no more quickly if you have RAM memory or disk limitations. To read, if `read_process=True` you
have a dedicated precess to read while you consume data and have a buffer of prepared sorted data.

* **Disk**: It is better work in SSD disk than HHD disk to prevent a bottleneck of processes. To increase the 
disk performance you can read data from different disk (depend on `iterable` read data) where the sorted is 
taking place. Configure main path with `tmp_dir` (or/and define one list with paths to other disks with 
`write_processes`). Example:
```python
sid = sorted_in_disk(...,
                     tmp_dir="/path/to/tmp_dir")
```

To start I recommend you test first with mono-process (`write_processes=0`) in injection and read
(`read_process=False`).
```python
sid = sorted_in_disk(...,
                     write_processes=0)
```

After, you can test with multiprocess (example for 4 inject processes `write_processes=4`,
or create one per physical processor with `write_processes=None`). If you think velocity of disk is enough,
you can enable multiprocess and test with different configurations. If disk velocity is not enough then is 
quickly and easy use mono-process.
```python
sid = sorted_in_disk(...,
                     write_processes=4)
```

If you think your bottleneck is disk, and you want to enable multiprocess, then you can try to pass a list with 
paths to `write_processes`. Each element of this list create a process than save data in each assigned path 
(`tmp_dir` continue saving a small file with state, but not bulk of data). For example, you have tree disks, 
"disk_HDD1" and "disk_HDD2" are HDD and "disk_SSD" is a SSD, then you can define one process per HDD disk and two 
process to SSD because it is quickly (in total 4 injection process; similar to `write_processes=4` but each
process have one path defined to save data):
```python
sid = sorted_in_disk(...,
                     tmp_dir="/path/to/tmp_dir",
                     write_processes = ["disk_HDD1/path/tmp_HDD1", 
                                        "disk_SSD/path/tmp_SSD", 
                                        "disk_SSD/path/tmp_SSD", 
                                        "disk_HDD2/path/tmp_HDD2"])
```

To read, if you need to perform hard operations with each returned data, it is better to enable multi-process in read.
```python
sid = sorted_in_disk(...,
                     read_process=True)
```
 
In other case, it is quick enough mono-process.
```python
sid = sorted_in_disk(...,
                     read_process=False)
```

More advanced performance settings in `sorted_in_disk` if you use multi-process, then you can configure the queue 
to read or write (both queues are independents). 

For example to precise fit configuration of write-queue (if `write_processes=0` write-queue not work):
```python
sid = sorted_in_disk(...,
                     write_processes=None,  # Write multiprocess enabled if this is different to 0 (None, 1, 2, 3, etc...)
                     queue_max_size=1000,
                     size_bucket_list=None,
                     min_size_bucket_list=10,
                     max_size_bucket_list=None)
```

Or an example to precise fit configuration of read-queue (if `read_process=False` read-queue not work):
```python
sid = sorted_in_disk(...,
                     read_process=True,  # Read multiprocess enabled if this is True
                     iter_m_queue_max_size=1000,
                     iter_min_size_bucket_list=10,
                     iter_max_size_bucket_list=None)
```

### Reuse pre-sorted work
You can use many times one sorted work from disk (if `only_one_read=False`), but this is not a data base. Example:
```python
sid = sorted_in_disk(...,
                     only_one_read=False)
```

When data is in disk have a minimum sorted work, but it is not finally sort. When you read data perform complete sort 
in real time (to have sorted data as soon as posible). Due to, if you want use several times sorted work, maybe is good
idea save result to file and read from this one (if you want to take advantage of same read iteration, with Python 
generators in streaming configuration you can save data to disk while you use data at same time).


### Performance test
Hardware where the tests have been done:
 * Processor: Intel i5 3.2GHz 4-core
 * Operating System: Windows 10 x64
 * RAM Memory: 8 GB

Use different configurations in `python3 tests\complex_example.py`

Inject N elements in `sorted_in_disk`.


#### 6,360,077 elements (1 GB of data) <All data sorted in same HHD disk using 1,16 GB>

**Mono-Process**: main process (injection in same main process):
```
sorted_in_disk write and pre-sort: 0:01:41.586510
```
Example to instance this execution:
```python
sid = sorted_in_disk(iterable_1gb,
                     key=lambda line: line.split("|")[2],
                     write_processes=0)
```

**+1 Process**: main process and 1 process for injection 
(time: Mono-Process = 1 Process x 3.3 faster):
```
sorted_in_disk write and pre-sort: 0:00:30.138000
```
Example to instance this execution:
```python
sid = sorted_in_disk(iterable_1gb,
                     key=lambda line: line.split("|")[2],
                     write_processes=1)
```

**+4 Processes**: main process and 4 processes for injection 
(time: Mono-Process = 4 Processes x 3.5 faster):
```
sorted_in_disk write and pre-sort: 0:00:28.697512
```
```python
sid = sorted_in_disk(iterable_1gb,
                     key=lambda line: line.split("|")[2],
                     write_processes=None)
```


#### 314,610,000 elements (50 GB of data) <All data sorted in same HHD disk using 66 GB>

**Mono-Process**: main process (injection in same main process; max 2 GB RAM memory consumption):
```
sorted_in_disk write and pre-sort: 2:01:26.222081
```
Example to instance this execution:
```python
sid = sorted_in_disk(iterable_50gb,
                     key=lambda line: line.split("|")[2],
                     write_processes=0)
```

**+4 Processes**: main process and 4 processes for injection (max 5 GB RAM memory consumption)
(time: Mono-Process = 4 Processes x 2 faster):
```
sorted_in_disk write and pre-sort: 1:16:38.016659
```
```python
sid = sorted_in_disk(iterable_50gb,
                     key=lambda line: line.split("|")[2],
                     write_processes=None)
```

### Readed
**Reminder**: `sorted_in_disk` write and do pre-sort operations while consume all data, after read in realtime when data 
is consumed by third code. In the following individual performance tests it is only indicated "write and pre-sort" 
heavy work require consume all data and block main process until all data will inject.

In all cases, **read sorted data in realtime** (reading in same main process from all files): 
```
100,000 elements each 4 seconds approximately
```

To configure read in same process (with block while process of sort improved):
```python
sid = sorted_in_disk(iterable,
                     ...
                     read_process=False)
```

To configure read in different process (without block while process of sort improved):
```python
sid = sorted_in_disk(iterable,
                     ...
                     read_process=True)
```


## Documentation
This is a review of class and functions content inside.

**You have complete docstring documentation in code and more examples/tests in doctest format.**


### Function:
 * `sorted_in_disk`: Main method to create a SortedInDisk object configured

Helpers methods public to take advantage of this package (those are not main package use): 
 * `create_tmp_folder`: Helper to create a temporal folder
 * `delete_tmp_folder`: Delete temporal files created
 
 
#### sorted_in_disk args
Args to configure **common sorted args**:
 * `iterable`: iterable to sort in disk.
        This iterable should by a generator for big data due to RAM memory limitation.
 * `key`: key specifies a function of one argument that is used to extract a comparison key from each element in
        iterable (for example, `key=str.lower` or `key=lambda e: e.split(",")[3]`).
        The default value is None (compare the elements directly).
 * `value`: value specifies a function of one argument that is used to extract a value from each element in
            iterable (for example, `key=lambda e: e.split(",")[1:]`).
 * `reverse`: reverse is a boolean value.
 
Args to configure **temporal directories**:
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
        
Args to configure **write/injection**:
 * `count_insert_to_check`: counter to check if process have more size in memory than max_write_process_size.
        By default: `1000000`
 * `max_write_process_size`: max size in bytes to dump cache memory values to disk
        (only execute when `count_insert_to_check` is reached). If None, then not import psutil and then only
        check with `count_insert_to_check`. By default: `1024*1024*1024`   # 1Gib
 * `ensure_space`: True to ensure disk space but is slowly. If not space then process launch warning message
           and wait for space. If False, then get and IOException if not enough space. By defatul: `False`
 * `write_processes`: number of process to execute. If None then it is number of CPUs. If you pass one list 
                     with paths pointing to folders, then each path implements one process (each process save data in 
                     its own path; you can use one path to several processes if you define same path several times in 
                     the list) and these paths are managed by `sorted_in_disk` (`tmp_dir` continues to be used for 
                     save general state information). By default: `0`
 * `queue_max_size`: (only if `write_processes!=0`) max number of elements in queue. If None then is the max by default.
        By default: `1000`
 * `size_bucket_list`: None to enable sensor size bucket list (require `maxsize>0`). If a number is defined
                              here then use this number to size_bucket_list and disable sensor. If `maxsize<=0`
                              and `size_bucket_list==None` then size_bucket_list is default to `1000`; other wise,
                              if maxsize<=0 and size_bucket_list is defined, then use this number. By default: `None`
 * `min_size_bucket_list`: (only if sensor is enabled) min size bucket list.
                                  `Min == 1` and `max == max_size_bucket_list - 1`. By default: `10`
 * `max_size_bucket_list`: (only if sensor is enabled) max size bucket list. If `None` is infinite.
                                  By defatult: `None`
Args to configure **read**:
 * `read_process`: `True` to get and prepare data in other process, `False` to use this one.
        By default: `False`
 * `iter_m_queue_max_size`: (only if `enable_multiprocessing` is `True`) max number of elements in queue. If None
        then is the max by default. By default: `1000`
 * `iter_min_size_bucket_list`: (only if sensor is enabled) min size bucket list.
                         `Min == 1` and `max == iter_max_size_bucket_list` - 1. By default: `10`
 * `iter_max_size_bucket_list`: (only if sensor is enabled) max size bucket list. If `None` is infinite.
                                 By default: `None`
Args to debug:
 * `logging_level`: Level of log. Only to debug or to remove psutil warning. By default: `logging.WARNING`

### Class:
 * `SortedInDisk`: Instance an object to work with data in a specific temporal folder.
    * `save_and_sort`: Choose `save_and_sort_multiprocess` of `save_and_sort_mono` depend on `write_processes`
    * `__iter__`: Sorted iterable of lines (same as `values` method).
    * `__len__`: Get number of elements in this structure.
    * `items`: Get a sorted iterable from disk to return sorted tuples of key and line, in each petition this get 
               one sorted
    * `values`: Get a sorted iterable from disk to return sorted lines, in each petition this get one sorted line.
    * `keys`: Get a sorted iterable from disk to return sorted keys of lines, in each petition this get one sorted key.
    * `join_multiprocess`: Wait to end of all processes (only it is important if multiprocess injection is enable).
    * `clear`: Clear file and delete temporal files
    * `visor`: Visor of information in state file.
    * Other methods invoked in previous methods (public for package extension proposals): 
        * `delete_tmp`: Delete temporal files created (use `clear` to use instance state)
        * `get_dict_saved_info`: Get dict with general information. If not exist, create a new empty.
        * `set_dict_saved_info`: Save in disk a new dict with general information.
        * `save_and_sort_multiprocess`: Consume an iterable to be sorted in multiprocess way. Take analysis in this 
                                        iterable and save to disk (in temporal files).
        * `save_and_sort_mono`: Consume an iterable to be sorted. Take analysis in this iterable and save to disk 
                                (in temporal files). Mono-thread, this one execute in the current thread.

### Utils functions:
Some tools to make work easier to read a file from disk to use `sorted_in_disk` and others.
 * `write_iter_in_file`: Write a iterable as text line in file
 * `read_iter_from_file`: Read a iterable where each element is a text line in file
 * `human_size`: Return a human size readable from bytes
 
#### How to read a file and sort quickly
You have a file similar to this content, and you want to sort with "keyN":
```
valA,key3,valB
valC,key1,valD
valE,key2,valF
```

You can inject use util `read_iter_from_file` in this way:
```python
from sorted_in_disk.utils import read_iter_from_file

sid = sorted_in_disk(read_iter_from_file("path/to/file/to/read"),
                     key=lambda line: line.split(",")[1])
```

And to write sorted content in a different file:
```python
from sorted_in_disk.utils import write_iter_in_file

count = write_iter_in_file("path/to/file/to/write", sid)

print("Total sorted lines: {}".format(count))
```



## Limitations
Not mix mono-process and multiprocess configuration in same temporal file (only if you use `only_one_read=False` 
and `append=True`). Each mode (mono-process or multiprocess) work similar but save data in different ways to improve
performance of each thread.


## About this algorithm
This is an own algorithm create with my own experience in big data. This is invented by me and my experience, I do not 
investigated thirds or used any others to create this one. In addition, I use QuickQueue for Python than I invented and
developed too.


## Is useful for you?
Maybe you would be so kind to consider the amount of hours puts in, the great effort and the resources expended in 
doing this project. Thank you.

[![paypal](https://www.paypalobjects.com/en_US/i/btn/btn_donateCC_LG.gif)](https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=PWRRXZ2HETVG8&source=url)


## Contact
Contact me via: r.invarato@gmail.com
