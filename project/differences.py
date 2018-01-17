

"""Read Files"""
df = sqlc.read.json(consts.filename)
df2 = sqlc.read.json(consts.reviewsfile)

"""Select Data"""
meta = operation.selectProducts(df, ["asin", "title", "price"], company, 50)
reviews = operation.selectReviews(df2, ['asin', "unixReviewTime"], beginTime, endTime)
"""----getReviews_cluster
result = operation.selectReviews(df2, ['asin', "overall", "summary", "unixReviewTime", "reviewTime"], beginTime, endTime)
"""

"""Join"""
reviews = reviews.join(meta, "asin")

"""----GetRatingGroupAvg
result = operation.averageRating(reviews, timeframe)
"""
"""----getCountRatings
result = operation.countApprox(reviews.rdd)
"""

printR.printClusterRDD(result.rdd, user, folder)

"""----Combine
stockData = sqlc.read.format('com.databricks.spark.csv') \
    .options(header='true') \
    .option("inferschema", 'true') \
    .option("encoding", "UTF-8") \
    .load(consts.stockFile)

a = datetime.datetime.fromtimestamp(beginTime).strftime('%Y/%m/%d')
b = datetime.datetime.fromtimestamp(endTime).strftime('%Y/%m/%d')

stockDataYear = operation.selectStock(stockData, ["date", "close"], a, b)
"""
"""Change Date Format from Y/M/d to Y-M-d"""
my_udf = udf(operation.formatDate)
stockDataYear = stockDataYear.withColumn("date", my_udf(stockDataYear.date))
stockDataYear.printSchema()


rating = operation.averageRating(reviews, "day")

"""Join ratings with stock"""
combine = rating.join(stockDataYear, "date")

printR.printClusterRDD(combine.rdd, user, folder)

