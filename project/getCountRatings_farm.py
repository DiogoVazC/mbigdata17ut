"""
Get Approximate count of reviews of products of a certain [companyName] 
by joining the asin of products in the Amazon Data Set with the ones in reviews

To execute on a Farm machine:
time spark-submit getCountRatings_farm.py [companyName] [unixBeginTime] [unixEndTime] 2> /dev/null
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
company = sys.argv[1]
beginTime = sys.argv[2] if (len(sys.argv) > 3) else consts.Jan2013
endTime = sys.argv[3] if (len(sys.argv) > 3) else consts.Dez2014

"""Initialize Spark"""
sc = SparkContext("local", "AmazonCountReviews")
sqlc = SQLContext(sc)

"""Read Files"""
df = sqlc.read.json(consts.filename)
df2 = sqlc.read.json(consts.reviewsfilefarm)

"""Select Data"""
meta = operation.selectProducts(df, ["asin", "title", "price"], company, 50)
reviews = operation.selectReviews(df2, ['asin', "overall", "summary", "unixReviewTime", "reviewTime"], beginTime, endTime)

"""Join"""
reviews = reviews.join(meta, "asin")

"""Count"""
contagem = operation.countApprox(reviews.rdd)

"""Print"""
print contagem
