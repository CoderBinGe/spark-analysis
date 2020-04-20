**pyspark_rdd_exercise：对RDD进行相关操作**
- 文件载入
- 采样
- 清洗
- 统计
- 排序
- 过滤
```
 import re
 def remove_symbol(s):
     reg = re.compile(r'[\s#,%]')
     s = reg.sub('-',s)
     return s.split('-')
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

**pyspark_dataframe_exercise：**
- 格式调整
- 生成新字段
- 排序
- 均值
- 最值

```
from pyspark.sql.functions import format_number

# 格式调整
desc.select(desc['summary'], format_number(desc['Open'].cast('float'), 2).alias('Open'), 
            format_number(desc['High'].cast('float'), 2).alias('High'),
            format_number(desc['Low'].cast('float'), 2).alias('Low'), 
            format_number(desc['Close'].cast('float'), 2).alias('Close'),
            format_number(desc['Volume'].cast('float'), 2).alias('Adj Close')).show()

# 生成新字段HV Ratio
df.withColumn('HV Ratio', df['High'] / df['Volume']).select('HV Ratio').show()
```
