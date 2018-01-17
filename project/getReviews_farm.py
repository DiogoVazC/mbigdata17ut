"""
This computes the asins of products containing a certain word in title across the
amazon products in the HDFS file /data/doina/UCSD-Amazon-Data/meta_Electronics.json.gz
And joins them with the reviews in the HDFS file file:///home/[user]/reviews_Electronics.json.gz

To execute on a Farm machine:
time spark-submit getReviews_farm.py [user] [company] [unixBeginTime] [unixEndTime] 2> /dev/null
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
endTime = sys.argv[3] if (len(sys.argv) > 3) else consts.Jun2013 

"""Initialize Spark"""
sc = SparkContext(appName="Amazon Reviews")
sqlc = SQLContext(sc)

"""Read Files"""
df = sqlc.read.json(consts.filename)
df2 = sqlc.read.json(consts.reviewsfilefarm)

"""Select Data"""
meta = operation.selectProducts(df, ["asin", "title", "price"], company, 50)
reviews = operation.selectReviews(df2, ['asin', "overall", "summary", "unixReviewTime", "reviewTime"], beginTime, endTime)

"""Join"""
reviews = reviews.join(meta, "asin")

"""Print"""
printR.printFarmExample(reviews, 10)
