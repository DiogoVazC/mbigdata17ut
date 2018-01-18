"""
This File is the index and functions the program can do (in cluster)

At the end of the page there's an index. 
Everytime a function is added, the index should be updated with the correct arguments.
"""

"""Import packages"""
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg, to_date, from_unixtime
import dataframeOperations as operation
import printResults as printR
import consts

"""
Get Reviews examples of products of a certain company for a given time in Amazon.com

args:

return/print/save:
"""
def getReviews(sqlc):
	"""Read Files"""
	df = sqlc.read.json(consts.filename)
	df2 = sqlc.read.json(consts.reviewsfile)

	"""Select Data"""
	products = df.select("asin", "title", "price")
	meta = products.filter(products.title.rlike('(?i).*' + consts.company + '.*')) 	\
		.filter(products.price > 100)

	reviews = df2.select('asin', "overall", "summary", "unixReviewTime", "reviewTime") \
		.filter(df2.unixReviewTime > consts.beginTime) \
		.filter(df2.unixReviewTime < consts.endTime)

	"""Join"""
	reviews = reviews.join(meta, "asin")

	"""Print"""
	result = reviews.take(20)
	printR.printClusterRDD(result, consts.user, consts.folder)

"""
Get Stock values for a company in a given time

args:

return/print/save:
"""
def getStock(sqlc):
	stockFile = "file:///home/" + consts.user + "/aapl-apple-historicalStock.csv"
	#.option("mode", "DROPMALFORMED") \
	stockData = operation.readStockValue(stockFile)

	printR.printClusterRDD(stockData.rdd, consts.user, consts.folder)

"""
Get rating avg in reviews for a company's products sold in Amazon grouped by a timeframe

args:

return/print/save:
"""
def getRatingGroupAvg(sqlc):	
	"""Read Files"""
	df = sqlc.read.json(consts.filename)
	df2 = sqlc.read.json(consts.reviewsfile)

	"""Select Data"""
	meta = operation.selectProducts(df, ["asin", "title", "price"], consts.company, 50)
	reviews = operation.selectReviews(df2, ['asin', "unixReviewTime"], consts.beginTime, consts.endTime)

	"""Join"""
	reviews = reviews.join(meta, "asin")
	rating = operation.averageRating(reviews, consts.timeframe)

	"""Print"""
	printR.printClusterRDD(rating.rdd, consts.user, consts.folder)

"""
Same as getRatingGroupAvg, but avg for the complete time (all grouped in one row)

args:

return/print/save:
"""
def getRatingAvg(sqlc):
	"""Read Files"""
	df = sqlc.read.json(consts.filename)
	df2 = sqlc.read.json(consts.reviewsfile)

	"""Select Data"""
	meta = operation.selectProducts(df, ["asin", "title", "price"], consts.company, 50)
	reviews = operation.selectReviews(df2, ['asin', "overall", "summary", "unixReviewTime", "reviewTime"], consts.beginTime, consts.endTime)

	"""Join"""
	reviews = reviews.join(meta, "asin")
	rating = reviews.join(meta, "asin").agg({"overall":"avg"})

	"""Print"""
	printR.printClusterRDD(rating.rdd, consts.user, consts.folder)

"""
Get count of reveiws for a company's produtcs in Amazon.com

args:

return/print/save:
"""
def countRatings(sqlc):
	"""Read Files"""
	df = sqlc.read.json(consts.filename)
	df2 = sqlc.read.json(consts.reviewsfile)

	"""Select Data"""
	meta = operation.selectProducts(df, ["asin", "title", "price"], consts.company, 50)
	reviews = operation.selectReviews(df2, ['asin', "unixReviewTime"], consts.beginTime, consts.endTime)

	"""Join"""
	reviews = reviews.join(meta, "asin")

	"""Count"""
	contagem = operation.countApprox(reviews.rdd)

	printR.printClusterRDD(contagem.rdd, consts.user, consts.folder)

"""
Combine Stock Value for a company in stock market and 
the avg rating given in reviews in the same day in Amazon.com
for each day in a given time

args:

return/print/save:
"""
def combine(sqlc):
	"""Read stock file"""
	stockData = sqlc.read.format('com.databricks.spark.csv') \
	    .options(header='true') \
	    .option("inferschema", 'true') \
	    .option("encoding", "UTF-8") \
	    .load(consts.stockFile)

	a = datetime.datetime.fromtimestamp(consts.beginTime).strftime('%Y/%m/%d')
	b = datetime.datetime.fromtimestamp(consts.endTime).strftime('%Y/%m/%d')

	stockDataYear = operation.selectStock(stockData, ["date", "close"], a, b)

	"""Change Date Format from Y/M/d to Y-M-d"""
	my_udf = udf(operation.formatDate)
	stockDataYear = stockDataYear.withColumn("date", my_udf(stockDataYear.date))
	stockDataYear.printSchema()

	"""Read Meta and Reviews Files"""
	df = sqlc.read.json(consts.filename)
	df2 = sqlc.read.json(consts.reviewsfile)
	meta = operation.selectProducts(df, ["asin", "title", "price"], consts.company, 50)
	reviews = operation.selectReviews(df2, ['asin', "overall", "unixReviewTime"], consts.beginTime, consts.endTime)

	"""Join Reviews asin"""
	reviews = reviews.join(meta, "asin")
	rating = operation.averageRating(reviews, 'day')

	"""Join ratings with stock"""
	combine = rating.join(stockDataYear, "date")

	printR.printClusterRDD(combine.rdd, consts.user, consts.folder)


index = {
	'getReviews':getReviews,
	'stock':getStock,
	'ratingGroupAvg':getRatingGroupAvg,
	'ratingAvg':getRatingAvg,
	'countRatings':countRatings,
	'combine':combine
}