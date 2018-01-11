"""
This computes the reviews summary of products with the asin found across the 
amazon products in the HDFS file /data/doina/UCSD-Amazon-Data/meta_Electronics.json.gz

To execute on a Farm machine:
time spark-submit reviews.py 2> /dev/null
Cluster:
spark-submit --master yarn --deploy-mode cluster reviews.py
hdfs dfs -cat /user/s*/project/data/* | head -5
"""

from pyspark import SparkContext
from pyspark.sql import SQLContext
filename = '/data/doina/UCSD-Amazon-Data/reviews_Electronics.json.gz'
sc = SparkContext(appName="Amazon reviews")
sqlc = SQLContext(sc)
df = sqlc.read.json(filename)

apple = df.select("asin", "overall", "summary", "reviewTime")

""" Change the filter to join the asins of amazon meta_Electronics
apple = apple.filter(apple.asin.rlike('(?i).*apple.*'))
"""

apple.printSchema()

"""Uncomment for farm"""
"""
exampe = apple.collect()
print example

"""

"""Cluster"""
apple.rdd.saveAsTextFile("/user/s1997319/project/data/")
