# Super Simple File Database  
base on B-tree, interface is simple  

# 超简单的文件数据库  
- 基于B-tree实现的Key–value文件数据库。  
- 接口简单，一共六个接口，创建数据库、打开数据库、关闭数据库、插入操作、查询操作、删除操作。  
- 创建文件数据库时，可指定关键字为string、bytes、int32、int64类型之一。  
- 只有一个数据库文件，并且实现了文件块的分配与回收，删除操作将文件块标记为空闲，插入操作优先分配空闲文件块。  

## Demo  
```shell
make
./filedb
```

# 如何解决崩溃一致性 Crash Consistency  
1. 如何发现不一致
2. 如何恢复一致性