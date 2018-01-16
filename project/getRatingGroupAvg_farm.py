"""
This computes the average of Rating per day in a given time for a given company.
To execute on a Farm machine:
time spark-submit apple.py [user] [folder] [companyName] [unixBeginTime] [unixEndTime] 2> /dev/null
Cluster:
spark-submit --master yarn --deploy-mode cluster getRatingGroupAvg.py [user] [folder] [companyName] [unixBeginTime] [unixEndTime]
hdfs dfs -cat /user/s*/project/data/[folder]
"""

import sys
user = sys.argv[1]
company = sys.argv[2]
beginTime = sys.argv[3] if (len(sys.argv) > 4) else 1356998400 
"""1 Jan 2013"""
endTime = sys.argv[4] if (len(sys.argv) > 4) else 1370044800 
"""1 Jun 2013"""

from pyspark import SparkContext
from pyspark.sql import SQLContext
import selects as sel
from pyspark.sql.functions import col, avg, to_date, from_unixtime
filename = '/data/doina/UCSD-Amazon-Data/meta_Electronics.json.gz'
reviewsfile = 'file:///home/' + user + '/reviews_Electronics.json.gz'
sc = SparkContext(appName="Amazon Products")
sqlc = SQLContext(sc)
df = sqlc.read.json(filename)

meta = sel.selectProducts(df, ["asin", "title", "price"], company, 50)

df2 = sqlc.read.json(reviewsfile)
reviews = df2.select('asin', "overall", "summary", "unixReviewTime", "reviewTime") \
	.filter(df2.unixReviewTime > beginTime) \
	.filter(df2.unixReviewTime < endTime)

example = reviews.take(1)
print example

reviews = reviews.withColumn("newDate", from_unixtime(reviews.unixReviewTime, "yyyy-MM-dd"))

example = reviews.take(1)
print example

	
reviews = reviews.join(meta, "asin") \
	.groupBy(reviews.reviewTime) \
	.agg(avg(col("overall")).alias('avgRating'), avg(col("unixReviewTime")).alias('avgtime')) \
	.orderBy("avgtime", ascending=True)

reviews = reviews.withColumn("DateFormat", from_unixtime(reviews.avgtime, "yyyy-MM-dd"))

print reviews.collect()

