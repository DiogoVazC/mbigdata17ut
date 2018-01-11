"""
This computes the asins of products containing a certain word in title across the 
amazon products in the HDFS file /data/doina/UCSD-Amazon-Data/meta_Electronics.json.gz

To execute on a Farm machine:
time spark-submit apple.py 2> /dev/null
Cluster:
spark-submit --master yarn --deploy-mode cluster apple.py
hdfs dfs -cat /user/s*/project/data/part-00000 | head -5
"""

from pyspark import SparkContext
from pyspark.sql import SQLContext
filename = '/data/doina/UCSD-Amazon-Data/meta_Electronics.json.gz'
sc = SparkContext(appName="Amazon Products")
sqlc = SQLContext(sc)
df = sqlc.read.json(filename)

products = df.select("title", "asin")
apple = products.filter(products.title.rlike('(?i).*apple.*'))

apple.printSchema()

"""Farm"""
example = apple.take(100)
print example

"""Cluster"""
"""
apple.rdd.saveAsTextFile("/user/s1997319/project/data/")
"""