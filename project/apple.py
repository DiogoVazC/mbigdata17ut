"""
This computes the asins of products containing a certain word in title across the 
amazon products in the HDFS file /data/doina/UCSD-Amazon-Data/meta_Electronics.json.gz

To execute on a Farm machine:
time spark-submit apple.py 2> /dev/null
"""

import string
from pyspark import SparkContext
from pyspark.sql import SQLContext
filename = '/data/doina/UCSD-Amazon-Data/meta_Electronics.json.gz'
sc = SparkContext("local", "Electronics")
sqlc = SQLContext(sc)
df = sqlc.read.json(filename)

products = df.select("title", "asin")
apple = products.filter(products.title.rlike('(?i).*apple.*')).map(lambda contents: contents).sortBy(lambda record: record[1], ascending=True)

apple.printSchema()

example = apple.take(50)

print example
