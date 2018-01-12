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
reviewsfile = '/data/doina/UCSD-Amazon-Data/reviews_Electronics.json.gz'
sc = SparkContext(appName="Amazon Products")
sqlc = SQLContext(sc)
df = sqlc.read.json(filename)

"""Farm"""
"""
products = df.select("asin", "title", "price")
apple = products.filter(products.title.rlike('(?i).*apple.*')) 	\
	.filter(products.price > 100)

price = df.select("asin", "price")
price = price.filter(price.price > 500)

apple50 = apple.join(price, apple.asin == price.asin)

price.printSchema()
apple50.printSchema()

apple.printSchema()
example = apple.take(50)
print example
"""

"""Cluster"""
products = df.select("asin", "title", "price")
apple = products.filter(products.title.rlike('(?i).*apple.*')) 	\
	.filter(products.price > 100)

"""apple.rdd.saveAsTextFile("/user/s1997319/project/data/")"""

df2 = sqlc.read.json(reviewsfile)
reviews = df2.select("asin", "overall", "summary", "unixReviewTime", "reviewTime") \
	.filter(df2.unixReviewTime > 1356998400)
reviews = reviews.join(apple, apple.asin == reviews.asin)
"""reviews.rdd.flatMap(lambda (file, contents): contents.lower().split())"""

""".sortBy(lambda record: record.reviewTime, ascending=True)"""

reviews.rdd.saveAsTextFile("/user/s1997319/project/data/unix2013")
