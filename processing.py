import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import *
import pyspark.sql.functions as F

S3_DATA_SOURCE_PATH = 's3://prod-bucket-team-e/raw_data/survey_results_public.csv'
S3_DATA_OUTPUT_PATH = 's3://prod-bucket-team-e/processed_data'

spark = SparkSession.builder.master('local').appName('TeamEPRoject').getOrCreate()
data = spark.read.csv(S3_DATA_SOURCE_PATH, inferSchema=True, header=True, sep=',')
print('Total number of records in the source data %s' % data.count())
data_filtered = data.select("ResponseId","MainBranch","Country","EdLevel","LanguageHaveWorkedWith",
                            "DatabaseHaveWorkedWith","NEWCollabToolsHaveWorkedWith","OpSys","Age","Gender")

data_filtered = data_filtered.withColumn("MainBranch_new",when(data_filtered.MainBranch == "None of these","None")\
                               .when(data_filtered.MainBranch == "I used to be a developer by profession, but no longer am","Ex-Developer")\
                               .when(data_filtered.MainBranch == "I code primarily as a hobby","Hobby")\
                               .when(data_filtered.MainBranch == "I am not primarily a developer, but I write code sometimes as part of my work","Not Developer")\
                               .when(data_filtered.MainBranch == "I am a student who is learning to code","Student")\
                               .when(data_filtered.MainBranch == "I am a developer by profession","Developer"))


data_filtered = data_filtered.drop("MainBranch")
data_filtered = data_filtered.withColumn('EdLevel_new', F.split(F.col('EdLevel'), " \\(").getItem(0)).drop("EdLevel")
data_filtered = data_filtered.withColumn('LanguageHaveWorkedWith_new', split(col('LanguageHaveWorkedWith'),';')).drop('LanguageHaveWorkedWith')
data_filtered = data_filtered.withColumn('DatabaseHaveWorkedWith_new', split(col('DatabaseHaveWorkedWith'),';')).drop('DatabaseHaveWorkedWith')
data_filtered = data_filtered.withColumn('NEWCollabToolsHaveWorkedWith_new', split(col('NEWCollabToolsHaveWorkedWith'),';')).drop('NEWCollabToolsHaveWorkedWith')
data_filtered = data_filtered.withColumn('Age_new', F.split(F.col('Age'), " ").getItem(0)).drop("Age")

data_filtered.repartition(1).write.mode("overwrite").parquet(S3_DATA_OUTPUT_PATH)
print('The data is successfully loaded to the target S3 bucket')
#print(data_filtered.show(5,truncate=False))
