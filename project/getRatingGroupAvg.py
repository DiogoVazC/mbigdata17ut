"""
This computes the asins of products containing a certain word in title across the
amazon products in the HDFS file /data/doina/UCSD-Amazon-Data/meta_Electronics.json.gz

To execute on a Farm machine:
time spark-submit apple.py [user] [folder] [companyName] [unixBeginTime] [unixEndTime] 2> /dev/null
Cluster:
spark-submit --master yarn --deploy-mode cluster apple.py [user] [folder] [companyName] [unixBeginTime] [unixEndTime]
hdfs dfs -cat /user/s*/project/data/part-00000 | head -5
"""

import sys
user = sys.argv[1]
folder = sys.argv[2]
company = sys.argv[3]
beginTime = sys.argv[4]
endTime = sys.argv[5]

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg
filename = '/data/doina/UCSD-Amazon-Data/meta_Electronics.json.gz'
reviewsfile = '/data/doina/UCSD-Amazon-Data/reviews_Electronics.json.gz'
sc = SparkContext(appName="Amazon Products")
sqlc = SQLContext(sc)
df = sqlc.read.json(filename)

"""Cluster"""
products = df.select("asin", "title", "price")
meta = products.filter(products.title.rlike('(?i).*' + company + '.*')) 	\
	.filter(products.price > 100)

df2 = sqlc.read.json(reviewsfile)
reviews = df2.select('asin', "overall", "summary", "unixReviewTime", "reviewTime") \
	.filter(df2.unixReviewTime > beginTime) \
	.filter(df2.unixReviewTime < endTime)
	
reviews = reviews.join(meta, "asin") \
	.groupBy(reviews.reviewTime) \
	.agg(avg(col("overall")).alias('avgRating'), avg(col("unixReviewTime")).alias('avgtime')) \
	.orderBy("avgtime", ascending=True)

reviews.rdd.saveAsTextFile("/user/" + user + "/project/data/" + folder)

