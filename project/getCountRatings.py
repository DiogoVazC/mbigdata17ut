"""
Cluster:
spark-submit --master yarn --deploy-mode cluster apple.py [user] [folder] [companyName] [unixBeginTime] [unixEndTime]
hdfs dfs -cat /user/s*/project/data/part-00000 | head -5
"""

import sys
user = sys.argv[1]
folder = sys.argv[2]
company = sys.argv[3]
beginTime = sys.argv[4] if (len(sys.argv) > 5) else 1356998400
endTime = sys.argv[5] if (len(sys.argv) > 5) else 1370044800

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import approxCountDistinct, countDistinct, col, avg


metaFile = '/data/doina/UCSD-Amazon-Data/meta_Electronics.json.gz'
reviewsfile = '/data/doina/UCSD-Amazon-Data/reviews_Electronics.json.gz'


sc = SparkContext("local", "AmazonReviews")
sqlc = SQLContext(sc)

df = sqlc.read.json(metaFile)
products = df.select("asin", "title", "price")
meta = products.filter(products.title.rlike('(?i).*' + company + '.*')) 	\
	.filter(products.price > 50)

df2 = sqlc.read.json(reviewsfile)
reviews = df2.select('asin', "unixReviewTime") \
	.filter(df2.unixReviewTime > beginTime) \
	.filter(df2.unixReviewTime < endTime)

reviews = reviews.join(meta, "asin")

contagem = reviews.countApprox()

contagem.saveAsTextFile("/user/" + user + "/project/data/" + folder)