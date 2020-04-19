**pyspark_rdd_exercise：对RDD进行相关操作**
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
**pyspark_rdd_exercise2：对RDD进行相关操作**

顾客消费分析：
- 统计每位顾客的消费总额
- 取出消费最高的5位顾客
- 消费超过5000的顾客有多少位
- 消费超过5200的顾客，平均消费是多少
- 消费最高的20名顾客比最低的20位顾客，平均消费高多少

电影评分分析：
-  统计不同打分的数量
-  统计影评数量最多的3部电影
-  找到影评数量最多的3位user
-  统计每个user的平均影评电影数量
-  统计全局的平均分
-  高分电影(超过3分)的平均分
