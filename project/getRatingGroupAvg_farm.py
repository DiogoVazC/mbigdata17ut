"""
This computes the average of Rating per day in a given time for a given company.
To execute on a Farm machine:

time spark-submit getRatingGroupAvg_farm.py [companyName] [day/10days/month] [unixBeginTime] [unixEndTime] 2> /dev/null

"""

"""Import packages"""
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg, to_date, from_unixtime, UserDefinedFunction
import pyspark.sql.functions as F
import pyspark.sql.types as T
import dataframeOperations as operation
import printResults as printR
import consts

"""Get Arguments"""
import sys
company = sys.argv[1]
timeframe = sys.argv[2]
beginTime = sys.argv[3] if (len(sys.argv) > 4) else consts.Jan2013
endTime = sys.argv[4] if (len(sys.argv) > 4) else consts.Jun2013

"""Initialize Spark"""
sc = SparkContext("local", "AmazonRatingAvg")
sqlc = SQLContext(sc)

"""Read Files"""
df = sqlc.read.json(consts.filename)
df2 = sqlc.read.json(consts.reviewsfilefarm)

"""Select Data"""
meta = operation.selectProducts(df, ["asin", "title", "price"], company, 50)
reviews = operation.selectReviews(df2, ['asin', "overall", "unixReviewTime"], beginTime, endTime)

"""Join"""
reviews = reviews.join(meta, "asin")
rating = operation.averageRating(reviews, timeframe)

"""Collect"""
printR.printFarm(rating)
