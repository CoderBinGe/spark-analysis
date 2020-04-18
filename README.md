**PySpark_exercise：对RDD进行相关操作**
- 文件载入
- 采样
- 清洗
- 统计
- 排序
- 过滤
```
import findspark
findspark.init()
import os
import pyspark
from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setAppName("miniProject").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf) 
```
