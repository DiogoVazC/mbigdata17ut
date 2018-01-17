"""
Get Approximate count of reviews of products of a certain [companyName] 
by joining the asin of products in the Amazon Data Set with the ones in reviews

Cluster:
spark-submit --master yarn --deploy-mode cluster getCountRatings_cluster.py [user] [folder] [company] [unixBeginTime] [unixEndTime]
hdfs dfs -cat /user/s*/project/data/part-00000 | head -5
"""

"""Import packages"""
import dataframeOperations as operation
import printResults as printR
import consts
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg, to_date, from_unixtime

"""Get Arguments"""
import sys
user = sys.argv[1]
folder = sys.argv[2]
company = sys.argv[3]
beginTime = sys.argv[4] if (len(sys.argv) > 5) else consts.Jan2013
endTime = sys.argv[5] if (len(sys.argv) > 5) else consts.Jun2013

"""Initialize Spark"""
sc = SparkContext(appName="Amazon Ratings Count")
sqlc = SQLContext(sc)

"""Read Files"""
df = sqlc.read.json(consts.filename)
df2 = sqlc.read.json(consts.reviewsfile)

"""Select Data"""
meta = operation.selectProducts(df, ["asin", "title", "price"], company, 50)
reviews = operation.selectReviews(df2, ['asin', "unixReviewTime"], beginTime, endTime)

"""Join"""
reviews = reviews.join(meta, "asin")

"""Count"""
contagem = operation.countApprox(reviews.rdd)

printR.printClusterRDD(contagem.rdd, user, folder)
