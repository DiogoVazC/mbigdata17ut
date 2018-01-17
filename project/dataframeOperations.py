from pyspark.sql.functions import col, avg, to_date, from_unixtime

def selectProducts(dataframe, col, title, price):
	products = dataframe.select(col)
	meta = products.filter(products.title.rlike('(?i).*' + title + '.*')) 	\
		.filter(products.price > price)
	return meta

def selectReviews(dataframe, col, beginTime, endTime):
	reviews = dataframe.select(col) \
		.filter(dataframe.unixReviewTime > beginTime) \
		.filter(dataframe.unixReviewTime < endTime)
	return reviews

def averageRatingDay(dataframe):
	rating = dataframe.groupBy(dataframe.reviewTime) \
		.agg(avg(col("overall")).alias('avgRating'), avg(col("unixReviewTime")).alias('avgtime')) \
		.orderBy("avgtime", ascending=True)

	rating = rating.withColumn("DateFormat", from_unixtime(rating.avgtime, "yyyy-MM-dd"))
	return rating

def countApprox(rdd):
	return rdd.countApprox(1500, 0.95)
