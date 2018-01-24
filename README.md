# Managing Big Data 1B - 17/18 - University of Twente
### mbigdata17ut

## Instructions
### Run in Farm machine:
For functions that access the stock value dataset (csv files)
```
time spark-submit --packages com.databricks:spark-csv_2.11:1.5.0 farm.py [function] [user] [company] [day/week/month] [unixBeginTime] [unixEndTime] 2> /dev/null
```
else:
```
time spark-submit farm.py [function] [user] [companyName] [day/week/month] [unixBeginTime] [unixEndTime] 2> /dev/null
```
### Run in Cluster:
Before runing any command, run this with the correct user:
```
export PYTHONPATH=$PYTHONPATH:/home/[user]/project
```

For functions that access the stock value dataset (csv files)
```
spark-submit --packages com.databricks:spark-csv_2.11:1.5.0 --master yarn --deploy-mode cluster cluster.py [function] [user] [folder] [company] [day/week/month] [unixBeginTime] [unixEndTime]
```
else:
```
spark-submit --master yarn --deploy-mode cluster cluster.py [function] [user] [folder] [company] [day/week/month] [unixBeginTime] [unixEndTime]
```
### Meaning of the arguments
- ***function*** \- the name of the function required. Options:
  - *getReviews* \- get the reviews of a company
  - *count* \- count the number of reviews per day/week/month for a company
  - *stock* \- get the stock of a company
  - *ratingGroupAvg* \- get the avg rating per day/month of a company's products
  - *ratingAvg* \- get the total avg of the ratings in a certain time	
  - *countRatings* \- number of ratings of a company's products
  - *combine* \- combine stockvalue with rating
- ***user*** \- sXXXXXXX for folder paths
- ***folder*** \- name of folder to save results
- ***company*** \- name of the company to search
- ***unixBeginTime*** \- time in unixtimestamp of begin of time to search ***(default 1Jan2013)***
- ***unixEndTime*** \- time in unixtimestamp of end of time to search ***(default 1Jan2014)***
- ***day/10days/month*** \- string 'day', '10days' or 'month' for group purposes ***(default day)***
