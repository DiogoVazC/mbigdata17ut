"""
To execute on a Farm machine:
time spark-submit getCountRatings_farm.py [user] [companyName] [unixBeginTime] [unixEndTime] 2> /dev/null
"""

import sys
user = sys.argv[1]
company = sys.argv[2]
beginTime = sys.argv[3] if (len(sys.argv) > 4) else 1356998400
endTime = sys.argv[4] if (len(sys.argv) > 4) else 1370044800

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import approxCountDistinct, countDistinct, col, avg
import selects as sel


metaFile = '/data/doina/UCSD-Amazon-Data/meta_Electronics.json.gz'
reviewsfile = 'file:///home/' + user + '/reviews_Electronics.json.gz'


sc = SparkContext("local", "AmazonReviews")
sqlc = SQLContext(sc)

df = sqlc.read.json(metaFile)
meta = sel.selectProducts(df, ["asin", "title", "price"], company, 50)

df2 = sqlc.read.json(reviewsfile)
reviews = df2.select('asin', "unixReviewTime") \
	.filter(df2.unixReviewTime > beginTime) \
	.filter(df2.unixReviewTime < endTime)

reviews = reviews.join(meta, "asin")

contagem = reviews.rdd.countApprox(1000, 0.9)

print contagem