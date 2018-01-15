"""
This computes the asins of products containing a certain word in title across the
amazon products in the HDFS file /data/doina/UCSD-Amazon-Data/meta_Electronics.json.gz
And joins them with the reviews in the HDFS file /data/doina/UCSD-Amazon-Data/reviews_Electronics.json.gz
Cluster:
spark-submit --master yarn --deploy-mode cluster getReviews_cluster.py [user] [folder] [company]
hdfs dfs -cat /user/s*/project/data/part-00000 | head -5
"""

import sys
user = sys.argv[1]
folder = sys.argv[2]
company = sys.argv[3]
beginTime = sys.argv[4] if (len(sys.argv) > 5) else 1356998400
endTime = sys.argv[5] if (len(sys.argv) > 5) else 1388534399

from pyspark import SparkContext
from pyspark.sql import SQLContext
metaFile = '/data/doina/UCSD-Amazon-Data/meta_Electronics.json.gz'
reviewsFile = '/data/doina/UCSD-Amazon-Data/reviews_Electronics.json.gz'
sc = SparkContext(appName="Amazon Products")
sqlc = SQLContext(sc)
df = sqlc.read.json(metaFile)

products = df.select("asin", "title", "price")
meta = products.filter(products.title.rlike('(?i).*' + company + '.*')) 	\
	.filter(products.price > 100)

"""apple.rdd.saveAsTextFile("/user/s1997319/project/data/")"""

df2 = sqlc.read.json(reviewsFile)
reviews = df2.select('asin', "overall", "summary", "unixReviewTime", "reviewTime") \
	.filter(df2.unixReviewTime > beginTime) \
	.filter(df2.unixReviewTime < endTime)

reviews = reviews.join(meta, "asin")
"""reviews = reviews.groupBy(reviews.asin).avg('overall')"""

"""reviews.rdd.flatMap(lambda (file, contents): contents.lower().split())"""

""".sortBy(lambda record: record.reviewTime, ascending=True)"""

reviews.rdd.saveAsTextFile("/user/" + user + "/project/data/" + folder)
