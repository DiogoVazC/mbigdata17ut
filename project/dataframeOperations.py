from pyspark.sql.functions import col, avg, date_add, from_unixtime
import datetime

def selectProducts(dataframe, col, title, price):
	products = dataframe.select(col)
	meta = products.filter(products.title.rlike('(?i).*' + title + '.*')) 	\
		.filter(products.price > price)
	return meta

def selectReviews(dataframe, col, beginTime, endTime):
	reviews = dataframe.select(col) \
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

def averageRatingDay(dataframe):
	rating = dataframe.groupBy(dataframe.date) \
		.agg(avg(col("overall")).alias('avgRating')) \
		.orderBy("date", ascending=True)
	return rating

def countApprox(rdd):
	return rdd.countApprox(1500, 0.95)

def formatDate(date):
	d = datetime.datetime.strptime(date, '%Y/%m/%d')
	return datetime.date.strftime(d, "%Y-%m-%d")
