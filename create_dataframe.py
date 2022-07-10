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

df.createOrReplaceTempView("table")

query1 = "select High, Low, Volume, Date, Stock, Row, ((Close - Open)/Close)*100 as Percent from table"
pdf = spark.sql(query1)

pdf.createOrReplaceTempView("new_table")

query2 = "SELECT mt.Stock as min_stock, mt.Date, mt.Percent as minPerc FROM new_table mt INNER JOIN (SELECT Date, MIN(Percent) AS MinPercent FROM new_table GROUP BY " \
         "Date) t ON mt.Date = t.Date AND mt.Percent = t.MinPercent"

query3 = "SELECT mt.Stock as max_stock, mt.Date, mt.Percent as maxPerc FROM new_table mt INNER JOIN (SELECT Date, MAX(Percent) AS MinPercent FROM new_table GROUP BY " \
         "Date) t ON mt.Date = t.Date AND mt.Percent = t.MinPercent"


# query4 = query2 + ' union ' + query3

new_pdf = spark.sql(query2)
new_pdf.createOrReplaceTempView("t1")

new_pdf1 = spark.sql(query3)
new_pdf1.createOrReplaceTempView("t2")

query4 = "select t1.Date,t1.min_stock,t1.minPerc,t2.max_stock,t2.maxPerc from t1 join t2 on t1.Date = t2.Date"

f_pdf = spark.sql(query4)

f_pdf.show()



# query_part1 = "select Date, Volume, Stock, Open, Close, High, Low, (((Close-Open)/Close))*100 as max_move from " \
#               "table"
# pdf = spark.sql(query_part1)
# pdf.createOrReplaceTempView("datatable")
#
# q2 = "select t1.Date,t1.Stock,t1.max_move from datatable t1 join ( select Date, Max(max_move) AS positive_move from " \
#      "datatable Group By Date) t2 on t1.Date = t2.Date and t2.positive_move = t1.max_move "
# pdf1 = spark.sql(q2)
# pdf1.createOrReplaceTempView("table1")
# # df2.show(100)
#
# q3 = "select t1.Date, t1.Stock, t1.max_move from datatable t1 join ( select Date, Min(max_move) AS negative_move from " \
#      "datatable Group By Date) t2 on t1.Date = t2.Date and t2.negative_move = t1.max_move "
# pdf2 = spark.sql(q3)
# pdf2.createOrReplaceTempView("table2")
# # df3.show(100)
# df4 = spark.sql("select t1.Date, t1.Stock as max_stock_name,t1.max_move as maxPercMove, t2.Stock as min_stock_name,"
#                 "t2.max_move as minPercMove from table1 t1 join table2 t2 on t2.Date = t1.Date order by t1.Date")
# df4.show(100)