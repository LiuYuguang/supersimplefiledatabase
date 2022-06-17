# Super Simple File Database  
base on B-tree, interface is simple  
基于b树的kv文件数据库，同时还实现了文件块的分配与回收。接口简单易调用，核心代码简明易读，适合新人学习b树。  

## Demo  
**Note:** You need to install uuid for bench!
#### CentOS
```
yum install -y libuuid-devel
```

```shell
make
./bench_string
./bench_bytes
./bench_int
```

# Crash Consistency 如何解决崩溃一致性  
1. 如何发现崩溃后不一致
2. 如何恢复一致性