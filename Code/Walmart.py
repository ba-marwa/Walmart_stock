#Import all the necessary libraries

from pyspark.sql import SparkSession
from pyspark.sql.functions import substring
from pyspark.sql import GroupedData


#Question 1: Starting SparkSession

spark=SparkSession.builder.appName('walmart').getOrCreate()
sc=spark.sparkContext

#Question 2: Importing the DataSet

BDD = spark.read.format("csv").load("C:/Users/sakur/OneDrive/Documents/Cours/Outils Data Mining/Spark Walmart/walmart_stock.csv",header=True)

#Question 4: Extracting the DataFrame Schema

BDD.printSchema()

#Question 5: Creating a new dataframe with a new column called HV_Ratio

##Step 1: Converting StringType to DoubleType 

from pyspark.sql.types import DoubleType
for col in ['Open','High', 'Low', 'Close', 'Volume', 'Adj Close']:
     BDD = BDD.withColumn(col, BDD[col].cast("double"))

##Step 2: Creating the new DataFrame

new_BDD = BDD.withColumn("HV_Ratio", BDD["High"]/BDD["Volume"])

#Question 6: Getting the day with the Peak High in Price

##Step 1: Getting the biggest High value 

new_BDD.registerTempTable("new_BDD")
max_High=spark.sql("SELECT MAX(High) as maxval FROM new_BDD").first().asDict()['maxval']

##Step 2: Getting the row with the biggest High value 

new_BDD.filter(new_BDD.High == max_High).show(truncate=False)

#Question 7: The mean of the Close column

spark.sql("SELECT MEAN(Close) FROM new_BDD").first().asDict()

#Question 8: The max and min of the Volume column

spark.sql("SELECT MAX(Volume), MIN(Volume) FROM new_BDD").first().asDict()

#Question 9:  The number of days where the Close is lower than 60 dollars

spark.sql("SELECT count(Date) FROM new_BDD WHERE Close<60").first().asDict()

#Question 10: Proportion of the time the High is greater than 80 dollars

spark.sql("SELECT count(Date) FROM new_BDD WHERE High>80").first().asDict()["count(Date)"]/new_BDD.count()

#Question 11: Getting the max High per year

#Step 1: Creating the Year variable

new_BDD = new_BDD.withColumn("Year", substring(new_BDD.Date,1,4))

#Step 2: Getting the maximum High per Year

new_BDD.groupBy(new_BDD.Year).agg({"High":"max"}).orderBy("Year").show()

#Question 12: Getting the average Close for each Calendar Month

#Step 1: Creating the Month variable

new_BDD = new_BDD.withColumn("Month", substring(new_BDD.Date,6,2))

#Step 2: Getting the average Close per Month

new_BDD.groupBy(new_BDD.Month).agg({"Close":"avg"}).orderBy("Month").show()