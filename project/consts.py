Jan2013 = 1356998400 # 1 Jan
Feb2013 = 1359676800 # 1 Feb
Mar2013 = 1362096000 # 1 March
Jun2013 = 1370044800
Jan2014 = 1388534400 # 1 Jan
Jun2014 = 1404172800 # 31 June
Dec2014 = 1419984000 # 31 Dec
saveGraph = False

filename = '/data/doina/UCSD-Amazon-Data/meta_Electronics.json.gz'
reviewsfile = '/data/doina/UCSD-Amazon-Data/reviews_Electronics.json.gz'
reviewsfilefarm = 'file:///home/s1997319/reviews_Electronics.json.gz'
appleStockFile = "/aapl-apple-historicalStock.csv"
hpStockFile = "/hpe-hp-historicalStock.csv"
microsoftStockFile = "/msft-microsoft-historicalStock.csv"
dellStockFile = "/dmvt-dell-historicalStock.csv"
ciscoStockFile = "/cisco-historicalStock.csv"
intelStockFile = "/intc-intel-historicalStock.csv"
panasonicStockFile = "/panasonic-historicalStock.csv"
sonyStockFile = "/sne-sony-historicalStock.csv"
samsungStockFile = "/ssnlf-samsung-historicalStock.csv"

def setStockFile(company, user):
    path = 'file:///home/' + user
    if company == 'apple':
    	stockFile = path + appleStockFile
    elif company == 'hp':
    	stockFile = path + hpStockFile
    elif company == 'microsoft':
    	stockFile = path + microsoftStockFile
    elif company == 'samsung':
    	stockFile = path + samsungStockFile
    elif company == 'sony':
    	stockFile = path + sonyStockFile
    elif company == 'dell':
    	stockFile = path + dellStockFile
    elif company == 'cisco':
    	stockFile = path + ciscoStockFile
    elif company == 'intel':
    	stockFile = path + intelStockFile
    elif company == 'panasonic':
    	stockFile = path + panasonicStockFile
    else:
    	stockFile = path + appleStockFile
    return stockFile
