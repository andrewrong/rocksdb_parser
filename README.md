### 目的

主要是为了分析目前 rocksdb 底层文件的分布情况，主要是依赖 rocksdb 提供的工具，然后对产生的结果进行分析

### 使用

```
-ldb: rocksdb的ldb的工具路径
-sstdump: rocksdb的sstdump的工具路径
-dbPath： rocksdb的数据文件的路径
-manifest: rocksdb的manifest文件的路径
-level: 只分析某一level的文件分布
-numLevel: level的范围，比如0-numLevel的分析

-level/numLevel 二选一
```

运行的需要注意的几个问题：

1. 需要依赖一些库文件，因为 rocksdb 的工具需要依赖这些库，主要有：

    * librocksdb.so
    * libsnappy.so
2. 需要和 mongodb 一样，运行之前将上面的库导入到 LD_LIBRARY_PATH 中

### 3. 例子

```
nohup ./rocksdb_parser -dbPath /disk5/db -ldb /home/xxxx/rockstool/ldb -manifest /xxx/db/MANIFEST-5881114 -sstdump /home/xxxx/rockstool/sst_dump &
```
