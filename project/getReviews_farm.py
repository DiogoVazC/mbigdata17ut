"""
This computes the asins of products containing a certain word in title across the
amazon products in the HDFS file /data/doina/UCSD-Amazon-Data/meta_Electronics.json.gz

To execute on a Farm machine:
time spark-submit getReviews_farm.py [company] 2> /dev/null
"""

import sys
company = sys.argv[1]
beginTime = sys.argv[2] if (len(sys.argv) > 3) else 1356998400
endTime = sys.argv[3] if (len(sys.argv) > 3) else 1388534399

from pyspark import SparkContext
from pyspark.sql import SQLContext

metaFile = '/data/doina/UCSD-Amazon-Data/meta_Electronics.json.gz'
reviewsfile = 'file:///home/s2000032/reviews_Electronics.json.gz'

sc = SparkContext("local", "AmazonReviews")
sqlc = SQLContext(sc)

df = sqlc.read.json(metaFile)
products = df.select("asin", "title", "price")
meta = products.filter(products.title.rlike('(?i).*' + company + '.*')) 	\
	.filter(products.price > 100)

df2 = sqlc.read.json(reviewsfile)
reviews = df2.select('asin', "overall", "summary", "unixReviewTime", "reviewTime") \
	.filter(df2.unixReviewTime > beginTime) \
	.filter(df2.unixReviewTime < endTime)

reviews = reviews.join(meta, "asin")

print reviews.take(10)
