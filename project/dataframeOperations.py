from pyspark.sql.functions import col, avg, date_add, from_unixtime, weekofyear, month
import datetime

def selectProducts(dataframe, col, title, price):
	products = dataframe.select(col)
	meta = products.filter(products.title.rlike('(?i).* ' + title + ' .*')) 	\
		.filter(products.price > price)
	return meta

def selectReviews(dataframe, col, beginTime, endTime):
	reviews = dataframe.select(col) \
		.filter(dataframe.unixReviewTime > beginTime) \
		.filter(dataframe.unixReviewTime < endTime)
	reviews = reviews.withColumn("unixReviewTime", from_unixtime(reviews.unixReviewTime, "yyyy-MM-dd"))
	reviews = reviews.withColumnRenamed("unixReviewTime", "date")
	return reviews

def selectReviewsText(dataframe, company, col, beginTime, endTime):
	reviews = dataframe.select(col) \
		.filter(dataframe.reviewText.rlike('(?i).* ' + company + ' .*')) \
		.filter(dataframe.unixReviewTime > beginTime) \
		.filter(dataframe.unixReviewTime < endTime)
	reviews = reviews.withColumn("unixReviewTime", from_unixtime(reviews.unixReviewTime, "yyyy-MM-dd"))
	reviews = reviews.withColumnRenamed("unixReviewTime", "date")
	return reviews

def selectStock(dataframe, col, beginTime, endTime):
	stock = dataframe.select(col) \
		.filter(dataframe.date >= beginTime) \
    	.filter(dataframe.date <= endTime)
	return stock

def averageRating(dataframe, timeframe):
	if timeframe == 'month':
		rating = dataframe.groupBy(month(dataframe.date)) \
			.agg(avg(col("overall")).alias('avgRating')) \
			.withColumnRenamed("month(date)", "date") \
			.orderBy("date", ascending=True)
		return rating
	elif timeframe == 'week':
		rating = dataframe.groupBy(weekofyear(dataframe.date)) \
			.agg(avg(col("overall")).alias('avgRating')) \
			.withColumnRenamed("weekofyear(date)", "date") \
			.orderBy("date", ascending=True)
		return rating
	else:
		rating = dataframe.groupBy(dataframe.date) \
			.agg(avg(col("overall")).alias('avgRating')) \
			.orderBy("date", ascending=True)
		return rating

def averageStock(dataframe, timeframe):
	if timeframe == 'month':
		stock = dataframe.groupBy(month(dataframe.date)) \
			.agg(avg(col("close")).alias('close')) \
			.withColumnRenamed("month(date)", "date") \
			.orderBy("date", ascending=True)
		return stock
	elif timeframe == 'week':
		stock = dataframe.groupBy(weekofyear(dataframe.date)) \
			.agg(avg(col("close")).alias('close')) \
			.withColumnRenamed("weekofyear(date)", "date") \
			.orderBy("date", ascending=True)
		return stock
	else:
		return dataframe

def averageRatingAlias(dataframe, timeframe, colName):
	if timeframe == 'month':
		dataframe = dataframe.withColumn("date", dataframe['date'].substr(1,7))
	elif timeframe == '10days':
		dataframe = dataframe.withColumn("date", dataframe['date'].substr(1,9))

	rating = dataframe.groupBy(dataframe.date) \
		.agg(avg(col("overall")).alias(colName)) \
		.orderBy("date", ascending=True)
	return rating

def countApprox(rdd):
	return rdd.countApprox(1500, 0.95)

def formatDate(date):
	d = datetime.datetime.strptime(date, '%Y/%m/%d')
	return datetime.date.strftime(d, "%Y-%m-%d")

def readStockValue(filename, sqlc, col, beginTime, endTime):
	stockData = sqlc.read.format('com.databricks.spark.csv') \
	    .options(header='true') \
	    .option("inferschema", 'true') \
	    .option("encoding", "UTF-8") \
	    .load(filename)

	beginTime = int(beginTime)
	endTime = int(endTime)
	a = datetime.datetime.fromtimestamp(beginTime).strftime('%Y/%m/%d')
	b = datetime.datetime.fromtimestamp(endTime).strftime('%Y/%m/%d')
	stockDataYear = selectStock(stockData, col, a, b)
	return stockDataYear
