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
import csv
# Project
import dataframeOperations as operation
import printResults as printR
import consts
import datetime
# Matplotlib
if(consts.saveGraph):
	import matplotlib
	matplotlib.use('Agg') # Force matplotlib to not use any Xwindows backend.
	import matplotlib.pyplot as plt
	import matplotlib.dates as mdates

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
	elif companyName == 'microsoft':
		consts.stockFile = consts.microsoftStockFile
	elif companyName == 'samsung':
		consts.stockFile = consts.samsungStockFile
	elif companyName == 'sony':
		consts.stockFile = consts.sonyStockFile
	elif companyName == 'dell':
		consts.stockFile = consts.dellStockFile
	else:
		consts.stockFile = consts.appleStockFile

	stockData = operation.readStockValue(consts.stockFile, sqlc, consts.beginTime, consts.endTime)
	stockData = stockData.orderBy("date", ascending=True)
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
	reviews = operation.selectReviewsText(df2, consts.company, ['asin', "overall", "unixReviewTime", "reviewText"], consts.beginTime, consts.endTime)

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
	elif companyName == 'microsoft':
		consts.stockFile = consts.microsoftStockFile
	elif companyName == 'samsung':
		consts.stockFile = consts.samsungStockFile
	elif companyName == 'sony':
		consts.stockFile = consts.sonyStockFile
	elif companyName == 'dell':
		consts.stockFile = consts.dellStockFile
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

	# Generate CSV with output data
	dates = [rat.date for rat in combine.select('date').collect()]
	ratings = [float(rat.avgRating) for rat in combine.select('avgRating').collect()]
	stocks = [float(stock.close) for stock in combine.select('close').collect()]
	diffRatings = [(((j-i)*100.0)/i) for i, j in zip(ratings[:-1], ratings[1:])]
	diffStocks = [(((j-i)*100.0)/i) for i, j in zip(stocks[:-1], stocks[1:])]

	rows = zip(dates, ratings, stocks, diffRatings, diffStocks)
	with open(companyName + '_' + str(consts.beginTime) + '_' + str(consts.endTime) + '.csv', 'w') as fileCSV:
	    writer = csv.writer(fileCSV)
	    for row in rows:
        	writer.writerow(row)

	if(consts.saveGraph):
		# RATINGS PLOT
		plt.figure(1)
		fig, ax = plt.subplots()
		datesObj = [datetime.datetime.strptime(str(i.date),"%Y-%m-%d") for i in combine.select('date').collect()]
		ax.set_ylim((min(ratings) - 0.4), 5.0)
		dates = matplotlib.dates.date2num(datesObj)
		ax.plot_date(dates, ratings, 'g-')
		fig.autofmt_xdate()
		xfmt = mdates.DateFormatter("%d/%m/%Y")
		ax.xaxis.set_major_formatter(xfmt)
		ax.set_title(companyName.capitalize() + ' - Amazon Ratings')
		plt.savefig('ratings_' + companyName + '_' + str(consts.beginTime) + '_' + str(consts.endTime) + '.png')
		# STOCKS PLOT
		plt.figure(2)
		fig, ax = plt.subplots()
		ax.set_ylim((min(stocks) - 5.0), (max(stocks) + 5.0))
		ax.plot_date(dates, stocks, 'b-')
		fig.autofmt_xdate()
		xfmt = mdates.DateFormatter("%d/%m/%Y")
		ax.xaxis.set_major_formatter(xfmt)
		ax.set_title(companyName.capitalize() + ' - Stock Values')
		plt.savefig('stocks_' + companyName + '_' + str(consts.beginTime) + '_' + str(consts.endTime) + '.png')

		diff = False
		if diff:
			# DIFF RATINGS PLOT
			plt.figure(3)
			fig, ax = plt.subplots()
			ax.set_ylim((min(diffRatings) - 0.2), (max(diffRatings) + 0.2))
			ax.plot_date(dates, diffRatings, 'g-')
			fig.autofmt_xdate()
			xfmt = mdates.DateFormatter("%d/%m/%Y")
			ax.xaxis.set_major_formatter(xfmt)
			ax.set_title(companyName.capitalize() + ' - Amazon Ratings Evolution (daily)')
			plt.savefig('diff_ratings_' + companyName + '_' + str(consts.beginTime) + '_' + str(consts.endTime) + '.png')
			# DIFF STOCKS PLOT
			plt.figure(4)
			fig, ax = plt.subplots()
			ax.set_ylim((min(diffStocks) - 2), (max(diffStocks) + 2))
			ax.plot_date(dates, diffStocks, 'b-')
			fig.autofmt_xdate()
			xfmt = mdates.DateFormatter("%d/%m/%Y")
			ax.xaxis.set_major_formatter(xfmt)
			ax.set_title(companyName.capitalize() + ' - Stock Value Evolution (daily)')
			plt.savefig('diff_stocks_' + companyName + '_' + str(consts.beginTime) + '_' + str(consts.endTime) + '.png')


def multipleCompanies(sqlc):
	stockDataYearApple = operation.readStockValue(consts.appleStockFile, sqlc, consts.beginTime, consts.endTime)
	stockDataYearHp = operation.readStockValue(consts.hpStockFile, sqlc, consts.beginTime, consts.endTime)
	stockDataYearMicrosoft = operation.readStockValue(consts.microsoftStockFile, sqlc, consts.beginTime, consts.endTime)
	stockDataYearDell = operation.readStockValue(consts.dellStockFile, sqlc, consts.beginTime, consts.endTime)
	stockDataYearSony = operation.readStockValue(consts.sonyStockFile, sqlc, consts.beginTime, consts.endTime)
	stockDataYearSamsung = operation.readStockValue(consts.samsungStockFile, sqlc, consts.beginTime, consts.endTime)
	stockDataList = [stockDataYearApple, stockDataYearHp, stockDataYearMicrosoft, stockDataYearDell, stockDataYearSony, stockDataYearSamsung]
	companyList = ['apple', 'hp', 'microsoft', 'dell', 'sony', 'samsung']

	"""Change Date Format from Y/M/d to Y-M-d"""
	my_udf = udf(operation.formatDate)
	index = 0
	for stock in stockDataList:
		stockDataList[index] = stock.withColumn("date", my_udf("date"))
		print stockDataList[index].take(2)
		index += 1

	"""Read Meta and Reviews Files"""
	df = sqlc.read.json(consts.filename)
	df2 = sqlc.read.json(consts.reviewsfilefarm)

	results = None

	index = 0
	for company in companyList:
		stockDataList[index] = stockDataList[index].withColumnRenamed('close', 'stock ' + company)
		meta = operation.selectProducts(df, ["asin", "title", "price"], company, 50)
		reviews = operation.selectReviews(df2, ['asin', "overall", "unixReviewTime"], consts.beginTime, consts.endTime)
		amazonjoin = reviews.join(meta, "asin")
		print "amazonjoin " + company
		print amazonjoin.take(5)
		rating = operation.averageRatingAlias(amazonjoin, 'day', 'rating ' + company)
		print "rating and stock " + company
		print rating.take(5)
		print stockDataList[index].take(5)
		combine = rating.join(stockDataList[index], "date")
		combine = combine.orderBy("date", ascending=True)
		print "combine " + company
		print combine.take(5)
		"""if index == 0:
									results = combine
								else:
									results = results.join(combine, "date")"""
		index += 1


index = {
	'getReviews':getReviews,
	'stock':getStock,
	'ratingGroupAvg':getRatingGroupAvg,
	'ratingAvg':getRatingAvg,
	'countRatings':countRatings,
	'combine':combine,
	'multipleCompanies': multipleCompanies
}
