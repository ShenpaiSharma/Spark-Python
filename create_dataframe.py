from pyspark.sql import SparkSession
import glob
import pandas as pd
import numpy as np

path = "/Users/shubhamkumar/PycharmProjects/python_spark_assignment/company"
files = glob.glob(path + "/*.csv")

data_frame = pd.DataFrame()
content = []

for filename in files:
    df = pd.read_csv(filename, index_col=None)
    content.append(df)

# converting content to data frame
data_frame = pd.concat(content)
data_frame['Row'] = np.arange(len(data_frame))

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

# Create PySpark DataFrame from Pandas
df = spark.createDataFrame(data_frame)


