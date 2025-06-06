# MemcLoad
### Reworked memc_load.py to process TSV logs and load them into memcached concurrently
#### The project includes three versions:
* single-threaded (`memc_load.py`)
* multi-threaded (`memc_load_concurrency.py`)
* multiprocessing (`memc_load_multiprocessing.py`)

### Run Examples
#### Single-threaded version:
```shell
python memc_load.py --pattern "logs/*.tsv.gz" --dry --log "logs/memc_load_single.log"
```

#### Multi-threaded version:
```shell
python memc_load_concurrency.py --pattern "logs/*.tsv.gz" --dry --log "logs/memc_load_concurrency.log"
```

#### Multiprocessing version:
```shell
python memc_load_multiprocessing.py --pattern "logs/*.tsv.gz" --dry --log "logs/memc_load_multiprocessing.log"
```

### Options Parser:
```
--pattern — path to .tsv.gz files (glob pattern)
--dry — if set, no data is sent to Memcached, only logged
--log — path to the log file
```

### Example Input File
```
idfa e7e1a50c0ec2747ca56cd9e1558c0d7c 67.7835424444 -22.8044005471 7942,8519,4232,3
idfa f5ae5fe6122bb20d08ff2c2ec43fb4c4 -104.68583244 -51.24448376 4877,7862,7181,6
gaid 3261cf44cbe6a00839c574336fdf49f6 137.790839567 56.8403675248 7462,1115,5205,6
``` 

### Logging
##### Single-threaded version:
```
[2025.04.10 00:37:48] I Memc loader started with options: {'test': False, 'log': 'logs/memc_load_single.log', 'dry': True, 'pattern': 'logs/*.tsv.gz', 'idfa': '127.0.0.1:33013', 'gaid': '127.0.0.1:33014', 'adid': '127.0.0.1:33015', 'dvid': '127.0.0.1:33016'}
[2025.04.10 00:37:48] I Processing logs/20170929000000.tsv.gz
...
...
[2025.04.10 00:51:33] I Acceptable error rate (0.0). Successfull load
[2025.04.10 00:51:33] I Total processed: 10269498, total errors: 0, total execution time: 824.402271 sec
```

##### Multi-threaded version:
```
[2025.04.09 20:34:23] I Memc loader started with options: {'test': False, 'log': 'logs/memc_load_concurrency.log', 'dry': True, 'pattern': 'logs/*.tsv.gz', 'idfa': '127.0.0.1:33013', 'gaid': '127.0.0.1:33014', 'adid': '127.0.0.1:33015', 'dvid': '127.0.0.1:33016'}
[2025.04.09 20:34:23] I Processing logs/20170929000000.tsv.gz
...
...
[2025.04.09 20:49:30] I Acceptable error rate (0.0). Successfull load
[2025.04.09 20:49:30] I Finished processing <Future at 0x100da7800 state=finished returned tuple>: processed=3422026, errors=0, execution time: 379.600811 sec
[2025.04.09 20:49:30] I Total processed: 10269498, total errors: 0, total execution time: 379.601812 sec
```

##### Multiprocessing version:
```
[2025.04.14 14:36:02] I Memc loader started with options: {'test': False, 'log': 'logs/memc_load_multiprocessing.log', 'dry': True, 'pattern': 'logs/*.tsv.gz', 'idfa': '127.0.0.1:33013', 'gaid': '127.0.0.1:33014', 'adid': '127.0.0.1:33015', 'dvid': '127.0.0.1:33016'}
[2025.04.14 14:38:09] I Finished processing <Future at 0x1035a6e10 state=finished returned tuple>: processed=3422995, errors=0, execution time: 127.044695 sec
[2025.04.14 14:38:09] I Finished processing <Future at 0x1035a7410 state=finished returned tuple>: processed=3424477, errors=0, execution time: 126.893555 sec
[2025.04.14 14:38:09] I Finished processing <Future at 0x1035a7a70 state=finished returned tuple>: processed=3422026, errors=0, execution time: 126.974656 sec
[2025.04.14 14:38:09] I Total processed: 10269498, total errors: 0, total execution time: 127.054865 sec
```

### Links to Test Files
- [File 1](https://cloud.mail.ru/public/2hZL/Ko9s8R9TA)
- [File 2](https://cloud.mail.ru/public/DzSX/oj8RxGX1A)
- [File 3](https://cloud.mail.ru/public/LoDo)



