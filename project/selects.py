def selectProducts(dataframe, col, title, price):
	products = dataframe.select(col)
	meta = products.filter(products.title.rlike('(?i).*' + title + '.*')) 	\
		.filter(products.price > price)
	return meta