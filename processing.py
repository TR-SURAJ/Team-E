import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.functions import split

spark = SparkSession.builder.master('local').appName('TeamEPRoject').getOrCreate()

data = spark.read.csv('survey_results_public.csv',inferSchema=True,header=True,sep=',')

data_filtered = data.select("ResponseId","MainBranch","Country","EdLevel","LanguageHaveWorkedWith",
                            "DatabaseHaveWorkedWith","NEWCollabToolsHaveWorkedWith","OpSys","Age","Gender")
#print(data_filtered.printSchema())



#print(data_filtered.groupby('MainBranch').count().orderBy('count').show(truncate=False))

#print(data_filtered.select('MainBranch').show(5))

data_filtered = data_filtered.withColumn("MainBranch_new",when(data_filtered.MainBranch == "None of these","None")\
                               .when(data_filtered.MainBranch == "I used to be a developer by profession, but no longer am","Ex-Developer")\
                               .when(data_filtered.MainBranch == "I code primarily as a hobby","Hobby")\
                               .when(data_filtered.MainBranch == "I am not primarily a developer, but I write code sometimes as part of my work","Not Developer")\
                               .when(data_filtered.MainBranch == "I am a student who is learning to code","Student")\
                               .when(data_filtered.MainBranch == "I am a developer by profession","Developer"))

#print('Step 1')
data_filtered = data_filtered.drop("MainBranch")
print(data_filtered.show(5))
# print('Step 2')
# data_filtered = data_filtered.withColumn('EdLevel', split(col('EdLevel'), ' (').getItem(0))
# print('Step 3')
# print(data_filtered.show(5,truncate = False))
#print(data_filtered.show(5, truncate=False))
