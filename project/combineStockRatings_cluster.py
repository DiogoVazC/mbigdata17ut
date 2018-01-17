"""
This computes the average of Rating per day in a given time for a given company.
To execute on Cluster:
spark-submit --packages com.databricks:spark-csv_2.11:1.5.0 --master yarn --deploy-mode cluster combineStockRatings_cluster.py [user] [folder] [company] [unixBeginTime] [unixEndTime]
"""

"""Import packages"""
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg, to_date, from_unixtime, udf
import pyspark.sql.types as T
import dataframeOperations as operation
import printResults as printR
import consts
import datetime

"""Get Arguments"""
import sys
user = sys.argv[1]
folder = sys.argv[2]
company = sys.argv[3]
beginTime = sys.argv[4] if (len(sys.argv) > 5) else consts.Jan2013
endTime = sys.argv[5] if (len(sys.argv) > 5) else consts.Jun2013

"""Initialize Spark"""
sc = SparkContext(appName="Combine Amazon ratings with stock values")
sqlc = SQLContext(sc)

"""Read stock file"""
stockData = sqlc.read.format('com.databricks.spark.csv') \
    .options(header='true') \
    .option("inferschema", 'true') \
    .option("encoding", "UTF-8") \
    .load(consts.stockFile)

a = datetime.datetime.fromtimestamp(beginTime).strftime('%Y/%m/%d')
b = datetime.datetime.fromtimestamp(endTime).strftime('%Y/%m/%d')

stockDataYear = operation.selectStock(stockData, ["date", "close"], a, b)

"""Change Date Format from Y/M/d to Y-M-d"""
my_udf = udf(operation.formatDate)
stockDataYear = stockDataYear.withColumn("date", my_udf(stockDataYear.date))
stockDataYear.printSchema()

"""Read Meta and Reviews Files"""
df = sqlc.read.json(consts.filename)
df2 = sqlc.read.json(consts.reviewsfilefarm)
meta = operation.selectProducts(df, ["asin", "title", "price"], company, 50)
reviews = operation.selectReviews(df2, ['asin', "overall", "unixReviewTime"], beginTime, endTime)

"""Join Reviews asin"""
reviews = reviews.join(meta, "asin")
rating = operation.averageRating(reviews, 'day')

"""Join ratings with stock"""
combine = rating.join(stockDataYear, "date")

printR.printClusterRDD(combine.rdd, user, folder)
