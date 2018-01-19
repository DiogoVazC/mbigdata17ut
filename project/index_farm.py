"""
This File is the index and functions the program can do (in cluster)

At the end of the page there's an index.
Everytime a function is added, the index should be updated with the correct arguments.
"""

"""Import packages"""
# PySpark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg, to_date, from_unixtime, udf
# Matplotlib
import matplotlib
matplotlib.use('Agg') # Force matplotlib to not use any Xwindows backend.
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
# Project
import dataframeOperations as operation
import printResults as printR
import consts
import datetime

"""
Get Reviews examples of products of a certain company for a given time in Amazon.com

args:

return/print/save:
"""
def getReviews(sqlc):
	"""Read Files"""
	df = sqlc.read.json(consts.filename)
	df2 = sqlc.read.json(consts.reviewsfilefarm)

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
	printR.printFarmExample(reviews, 10)

"""
Get Stock values for a company in a given time

args:

return/print/save:
"""
def getStock(sqlc):
	companyName = consts.company
	if companyName == 'apple':
		consts.stockFile = consts.appleStockFile
	elif companyName == 'hp':
		consts.stockFile = consts.hpStockFile
	if companyName == 'microsoft':
		consts.stockFile = consts.microsoftStockFile
	else:
		consts.stockFile = consts.appleStockFile

	stockData = operation.readStockValue(consts.stockFile, sqlc, consts.beginTime, consts.endTime)
	printR.printFarm(stockData)

"""
Get rating avg in reviews for a company's products sold in Amazon grouped by a timeframe

args:

return/print/save:
"""
def getRatingGroupAvg(sqlc):
	"""Read Files"""
	df = sqlc.read.json(consts.filename)
	df2 = sqlc.read.json(consts.reviewsfilefarm)

	"""Select Data"""
	meta = operation.selectProducts(df, ["asin", "title", "price"], consts.company, 50)
	reviews = operation.selectReviews(df2, ['asin', "overall", "unixReviewTime"], consts.beginTime, consts.endTime)

	"""Join"""
	reviews = reviews.join(meta, "asin")
	rating = operation.averageRating(reviews, consts.timeframe)

	"""Print"""
	printR.printFarmExample(rating)

"""
Same as getRatingGroupAvg, but avg for the complete time (all grouped in one row)

args:

return/print/save:
"""
def getRatingAvg(sqlc):
	"""Read Files"""
	df = sqlc.read.json(consts.filename)
	df2 = sqlc.read.json(consts.reviewsfilefarm)

	"""Select Data"""
	meta = operation.selectProducts(df, ["asin", "title", "price"], consts.company, 50)
	reviews = operation.selectReviews(df2, ['asin', "overall", "unixReviewTime", "reviewTime"], consts.beginTime, consts.endTime)

	"""Join"""
	reviews = reviews.join(meta, "asin")
	rating = reviews.join(meta, "asin").agg({"overall":"avg"})
	"""Print"""
	printR.printFarm(rating)

"""
Get count of reveiws for a company's produtcs in Amazon.com

args:

return/print/save:
"""
def countRatings(sqlc):
	"""Read Files"""
	df = sqlc.read.json(consts.filename)
	df2 = sqlc.read.json(consts.reviewsfilefarm)

	"""Select Data"""
	meta = operation.selectProducts(df, ["asin", "title", "price"], consts.company, 50)
	reviews = operation.selectReviews(df2, ['asin', "unixReviewTime"], consts.beginTime, consts.endTime)

	"""Join"""
	reviews = reviews.join(meta, "asin")

	"""Count"""
	contagem = operation.countApprox(reviews.rdd)

	print contagem

"""
Combine Stock Value for a company in stock market and
the avg rating given in reviews in the same day in Amazon.com
for each day in a given time

args:

return/print/save:
"""
def combine(sqlc):
	companyName = consts.company
	if companyName == 'apple':
		consts.stockFile = consts.appleStockFile
	elif companyName == 'hp':
		consts.stockFile = consts.hpStockFile
	if companyName == 'microsoft':
		consts.stockFile = consts.microsoftStockFile
	else:
		consts.stockFile = consts.appleStockFile

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

	"""Read Meta and Reviews Files"""
	df = sqlc.read.json(consts.filename)
	df2 = sqlc.read.json(consts.reviewsfilefarm)
	meta = operation.selectProducts(df, ["asin", "title", "price"], consts.company, 50)
	reviews = operation.selectReviews(df2, ['asin', "overall", "unixReviewTime"], consts.beginTime, consts.endTime)

	"""Join Reviews asin"""
	reviews = reviews.join(meta, "asin")
	rating = operation.averageRating(reviews, 'day')

	"""Join ratings with stock"""
	combine = rating.join(stockDataYear, "date")
	combine = combine.orderBy("date", ascending=True)

	printR.printFarm(combine)

	plt.figure(1)
	fig, ax = plt.subplots()
	datesObjs = [datetime.datetime.strptime(str(i.date),"%Y-%m-%d") for i in combine.select('date').collect()]
	ratings = [float(rat.avgRating) for rat in combine.select('avgRating').collect()]
	ax.set_ylim((min(ratings) - 0.4), 5.0)
	dates = matplotlib.dates.date2num(datesObjs)
	ax.plot_date(dates, ratings, 'g-')
	fig.autofmt_xdate()
	ax.fmt_xdata = mdates.DateFormatter('%m-%y')
	ax.set_title('Amazon Ratings Evolution')
	plt.savefig('ratings.png')
	plt.figure(2)
	fig, ax = plt.subplots()
	stocks = [float(stock.close) for stock in combine.select('close').collect()]
	ax.set_ylim((min(stocks) - 5.0), (max(stocks) + 5.0))
	ax.plot_date(dates, stocks, 'b-')
	ax.fmt_xdata = mdates.DateFormatter('%m-%y')
	fig.autofmt_xdate()
	ax.set_title('Stock Values Evolution')
	plt.savefig('stocks.png')

index = {
	'getReviews':getReviews,
	'stock':getStock,
	'ratingGroupAvg':getRatingGroupAvg,
	'ratingAvg':getRatingAvg,
	'countRatings':countRatings,
	'combine':combine
}
