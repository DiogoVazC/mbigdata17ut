"""
This computes the asins of products containing a certain word in title across the
amazon products in the HDFS file /data/doina/UCSD-Amazon-Data/meta_Electronics.json.gz
And joins them with the reviews in the HDFS file /data/doina/UCSD-Amazon-Data/reviews_Electronics.json.gz
Cluster:
spark-submit --master yarn --deploy-mode cluster getReviews_cluster.py [user] [folder] [company] [*unixBeginTime] [*unixEndTime]
hdfs dfs -cat /user/s*/project/data/part-00000 | head -5
"""

"""Import packages"""
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg, to_date, from_unixtime
import dataframeOperations as operation
import printResults as printR
import consts

"""Get Arguments"""
import sys
user = sys.argv[1]
folder = sys.argv[2]
company = sys.argv[3]
beginTime = sys.argv[4] if (len(sys.argv) > 5) else consts.Jan2013
endTime = sys.argv[5] if (len(sys.argv) > 5) else consts.Dez2014

"""Initialize Spark"""
sc = SparkContext(appName="Get Amazon Reviews")
sqlc = SQLContext(sc)

"""Read Files"""
df = sqlc.read.json(consts.filename)
df2 = sqlc.read.json(consts.reviewsfile)

"""Select Data"""
products = df.select("asin", "title", "price")
meta = products.filter(products.title.rlike('(?i).*' + company + '.*')) 	\
	.filter(products.price > 100)

"""apple.rdd.saveAsTextFile("/user/s1997319/project/data/")"""

reviews = df2.select('asin', "overall", "summary", "unixReviewTime", "reviewTime") \
	.filter(df2.unixReviewTime > beginTime) \
	.filter(df2.unixReviewTime < endTime)

"""Join"""
reviews = reviews.join(meta, "asin")

"""reviews = reviews.groupBy(reviews.asin).avg('overall')"""

"""reviews.rdd.flatMap(lambda (file, contents): contents.lower().split())"""

""".sortBy(lambda record: record.reviewTime, ascending=True)"""

"""Print"""
printR.printClusterRDD(reviews.rdd, user, folder)
