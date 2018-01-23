"""
This File is the index and functions the program can do (in cluster)

At the end of the page there's an index.
Everytime a function is added, the index should be updated with the correct arguments.
"""

"""Import packages"""
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg, to_date, from_unixtime, udf, weekofyear, month
import csv
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
	result = reviews.rdd.sample(False, 20)
	printR.printClusterRDD(result, consts.user, consts.folder)

"""
Get Stock values for a company in a given time

args:

return/print/save:
"""
def getStock(sqlc):
	companyName = consts.company
	consts.stockFile = consts.setStockFile(companyName, consts.user)
	#.option("mode", "DROPMALFORMED") \
	stockData = operation.readStockValue(consts.stockFile, sqlc, ["date", "volume", "high", "low", "open", "close"], consts.beginTime, consts.endTime)

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
	reviews = operation.selectReviews(df2, ['asin', 'overall', "unixReviewTime"], consts.beginTime, consts.endTime)

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
	reviews = operation.selectReviews(df2, ['asin', "overall", "unixReviewTime", "reviewTime"], consts.beginTime, consts.endTime)

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
	meta = operation.selectProducts(df, ["asin", "title", "price"], consts.company, 25)
	reviews = operation.selectReviews(df2, ['asin', "unixReviewTime"], consts.beginTime, consts.endTime)

	"""Join Reviews asin"""
	reviews = reviews.join(meta, "asin")

	"""Count"""
	contagem = operation.countApprox(reviews.rdd)

	print contagem


"""
Get count of reveiws for a company's produtcs in Amazon.com

args:

return/print/save:
"""
def countReviews(sqlc):
	"""Read Files"""
	df = sqlc.read.json(consts.filename)
	df2 = sqlc.read.json(consts.reviewsfilefarm)

	"""Select Data"""
	meta = operation.selectProducts(df, ["asin", "title", "price"], consts.company, 25)
	reviews = operation.selectReviews(df2, ['asin', "unixReviewTime"], consts.beginTime, consts.endTime)

	timeframe = consts.timeframe

	"""Join Reviews asin"""
	reviews = reviews.join(meta, "asin") 

	if timeframe == 'month':
		res = reviews.groupBy(month(reviews.date)).count().orderBy('month(date)', ascending=True)
	elif timeframe == 'week':
		res = reviews.groupBy(weekofyear(reviews.date)).count().orderBy('weekofyear(date)', ascending=True)
	else:
		res = reviews.groupBy("date").count().orderBy('date', ascending=True)

	printR.printClusterRDD(res.rdd, consts.user, consts.folder)

"""
Combine Stock Value for a company in stock market and
the avg rating given in reviews in the same day in Amazon.com
for each day in a given time

args:

return/print/save:
"""
def combine(sqlc):
	companyName = consts.company
	consts.stockFile = consts.setStockFile(companyName, consts.user)

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
	stockData = stockDataYear.withColumn("date", my_udf(stockDataYear.date))
	if consts.timeframe != 'day': 
		stockData = operation.averageStock(stockData, consts.timeframe)
		print 'stockData != day'
		print stockData.take(3)

	"""Read Meta and Reviews Files"""
	df = sqlc.read.json(consts.filename)
	df2 = sqlc.read.json(consts.reviewsfile)
	meta = operation.selectProducts(df, ["asin", "title", "price"], consts.company, 50)
	reviews = operation.selectReviews(df2, ['asin', "overall", "unixReviewTime"], consts.beginTime, consts.endTime)

	"""Join Reviews asin"""
	reviews = reviews.join(meta, "asin")
	rating = operation.averageRating(reviews, consts.timeframe)

	"""Join ratings with stock"""
	combine = rating.join(stockData, "date")
	combine = combine.orderBy("date", ascending=True)

	"""combine.write.format("com.databricks.spark.csv").save("/user/" + consts.user + "/project/data/" + consts.folder, header="true")"""

	printR.printClusterRDD(combine.rdd, consts.user, consts.folder)
	"""printR.saveClusterCSV(combine, consts.user, consts.folder)"""

	dates = [rat.date for rat in combine.select('date').collect()]
	ratings = [float(rat.avgRating) for rat in combine.select('avgRating').collect()]
	stocks = [float(stock.close) for stock in combine.select('close').collect()]
	diffRatings = [(((j-i)*100.0)/i) for i, j in zip(ratings[:-1], ratings[1:])]
	diffStocks = [(((j-i)*100.0)/i) for i, j in zip(stocks[:-1], stocks[1:])]

	'''rows = zip(dates, ratings, stocks, diffRatings, diffStocks)
	with open('file:///home/' + consts.user + '/' + companyName + '_' + str(consts.beginTime) + '_' + str(consts.endTime) + '.csv', 'w') as fileCSV:
	    writer = csv.writer(fileCSV)
	    for row in rows:
        	writer.writerow(row)'''

def multipleCompanies(sqlc):
	stockDataYearApple = operation.readStockValue(consts.appleStockFile, sqlc, ["date", "close"], consts.beginTime, consts.endTime)
	stockDataYearHp = operation.readStockValue(consts.hpStockFile, sqlc, ["date", "close"], consts.beginTime, consts.endTime)
	stockDataYearMicrosoft = operation.readStockValue(consts.microsoftStockFile, sqlc, ["date", "close"], consts.beginTime, consts.endTime)
	stockDataYearDell = operation.readStockValue(consts.dellStockFile, sqlc, ["date", "close"], consts.beginTime, consts.endTime)
	stockDataYearSony = operation.readStockValue(consts.sonyStockFile, sqlc, ["date", "close"], consts.beginTime, consts.endTime)
	stockDataYearSamsung = operation.readStockValue(consts.samsungStockFile, sqlc, ["date", "close"], consts.beginTime, consts.endTime)
	stockDataList = [stockDataYearApple, stockDataYearHp, stockDataYearMicrosoft, stockDataYearDell, stockDataYearSony, stockDataYearSamsung]
	companyList = ['apple', 'hp', 'microsoft', 'dell', 'sony', 'samsung']

	"""Change Date Format from Y/M/d to Y-M-d"""
	my_udf = udf(operation.formatDate)
	for stock in stockDataList:
		stock = stock.withColumn("date", my_udf(stock.date))

	"""Read Meta and Reviews Files"""
	df = sqlc.read.json(consts.filename)
	df2 = sqlc.read.json(consts.reviewsfile)

	results = None

	index = 0
	for company in companyList:
		stockDataList[index] = stockDataList[index].withColumnRenamed('close', 'stock ' + company)
		meta = operation.selectProducts(df, ["asin", "title", "price"], company, 50)
		reviews = operation.selectReviews(df2, ['asin', "overall", "unixReviewTime"], consts.beginTime, consts.endTime)
		amazonjoin = reviews.join(meta, "asin")
		rating = operation.averageRatingAlias(amazonjoin, 'day', 'rating ' + company)
		combine = rating.join(stockDataList[index], "date")
		combine = combine.orderBy("date", ascending=True)
		printR.printClusterRDD(combine.rdd, consts.user, consts.folder + '' + str(index))
		"""if index == 0:
									results = combine
								else:
									results = results.join(combine, "date")"""
		index += 1

	printR.printClusterRDD(results.rdd, consts.user, consts.folder)

index = {
	'getReviews':getReviews,
	'count':countReviews,
	'stock':getStock,
	'ratingGroupAvg':getRatingGroupAvg,
	'ratingAvg':getRatingAvg,
	'countRatings':countRatings,
	'combine':combine,
	'multipleCompanies': multipleCompanies
}
