"""
This computes the average of Rating per day in a given time for a given company.
To execute on a Farm machine:
time spark-submit apple.py [companyName] [unixBeginTime] [unixEndTime] 2> /dev/null
Cluster:
spark-submit --master yarn --deploy-mode cluster getRatingGroupAvg.py [folder] [companyName] [unixBeginTime] [unixEndTime]
hdfs dfs -cat /user/s*/project/data/[folder]
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
folder = sys.argv[1]
company = sys.argv[2]
beginTime = sys.argv[3] if (len(sys.argv) > 4) else consts.Jan2013 
endTime = sys.argv[4] if (len(sys.argv) > 4) else consts.Jun2013 

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
rating = operation.averageRatingDay(reviews)

"""Print"""
printR.printClusterRDD(rating.rdd, consts.user, folder)
