"""
This computes the average of Rating per day in a given time for a given company.
To execute on a Farm machine:
time spark-submit apple.py [folder] [companyName] [unixBeginTime] [unixEndTime] 2> /dev/null
Cluster:
spark-submit --master yarn --deploy-mode cluster getRatingGroupAvg.py [folder] [companyName] [unixBeginTime] [unixEndTime]
hdfs dfs -cat /user/s*/project/data/[folder]
"""

"""Import packages"""
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg, to_date, from_unixtime, UserDefinedFunction
import pyspark.sql.types as T
import dataframeOperations as operation
import printResults as printR
import consts

"""Get Arguments"""
import sys
company = sys.argv[1]
beginTime = sys.argv[2] if (len(sys.argv) > 3) else consts.Jan2013 
endTime = sys.argv[3] if (len(sys.argv) > 3) else consts.Jun2013 

"""Initialize Spark"""
sc = SparkContext(appName="Amazon Rating Average")
sqlc = SQLContext(sc)

"""Read Files"""
df = sqlc.read.json(consts.filename)
df2 = sqlc.read.json(consts.reviewsfilefarm)

"""Select Data"""
meta = operation.selectProducts(df, ["asin", "title", "price"], company, 50)
reviews = operation.selectReviews(df2, ['asin', "overall", "summary", "unixReviewTime", "reviewTime"], beginTime, endTime)

"""Testing Phase"""
printR.printFarmExample(meta, 1)
printR.printFarmExample(reviews, 1)
reviews = reviews.withColumn("newDate", from_unixtime(reviews.unixReviewTime, "yyyy-MM-dd"))
printR.printFarmExample(reviews, 1)

"""Join"""
reviews = reviews.join(meta, "asin")
printR.printFarmExample(reviews, 1)
rating = operation.averageRatingDay(reviews)
printR.printFarmExample(rating, 1)

"""Collect"""
printR.printFarm(rating)

"""Calculations"""
name = 'avgRating'
my_udf = F.UserDefinedFunction(operation.subtractRating, T.FloatType())
new_df = old_df.select(*[my_udf(column).alias("Date") if column == name else column for column in old_df.columns])

printR.printFarm(rating)