from stocks_name import all_stocks

# SparkSession initialization
import pandas as pd
import numpy as np

import requests

url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"

comp_df = []

for i in all_stocks:
    querystring = {f"ticker_symbol": {i}, "years": "5", "format": "json"}
    headers = {
        "X-RapidAPI-Key": "c8a9fb6618msh1e288b5a17f70b4p102165jsn4d53c79c5480",
        "X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    data = response.json()["historical prices"]
    df = pd.DataFrame.from_dict(data)
    df['Stock'] = i

    df.to_csv(f"/Users/shubhamkumar/PycharmProjects/python_spark_assignment/company/{i}.csv")



















# data1 = [
#     {
#         "historical prices": [
#             {
#                 "Open": 35.724998,
#                 "High": 36.1875,
#                 "Low": 35.724998,
#                 "Close": 36.044998,
#                 "Adj Close": 34.122578,
#                 "Volume": 76806800,
#                 "Date": "2017-07-07T00:00:00"
#             },
#             {
#                 "Open": 36.0275,
#                 "High": 36.487499,
#                 "Low": 35.842499,
#                 "Close": 36.264999,
#                 "Adj Close": 34.330837,
#                 "Volume": 84362400,
#                 "Date": "2017-07-10T00:00:00"
#             }
#         ]
#     },
#     {
#         "historical prices": [
#             {
#                 "Open": 31.724998,
#                 "High": 46.1875,
#                 "Low": 25.724998,
#                 "Close": 46.044998,
#                 "Adj Close": 28.122578,
#                 "Volume": 96606800,
#                 "Date": "2017-08-07T00:00:00"
#             },
#             {
#                 "Open": 39.0275,
#                 "High": 37.487499,
#                 "Low": 34.842499,
#                 "Close": 39.264999,
#                 "Adj Close": 31.330837,
#                 "Volume": 94362400,
#                 "Date": "2017-07-10T00:00:00"
#             }
#         ]
#     }
# ]
#
# # for i in all_stocks:
# # 	print(i)
# # 	data = data1[i]
# # 	print(data['historical prices'][0])
# # 	print()
#
# data = data1[0]['historical prices']
# df = pd.DataFrame.from_dict(data)
#
# df.to_csv("/Users/shubhamkumar/PycharmProjects/python_spark_assignment/new.csv")
#
# print(df)
# # creating the session
# spark = SparkSession.builder.getOrCreate()
#
# dataframe = spark.read.csv('/Users/shubhamkumar/PycharmProjects/python_spark_assignment/new.csv', sep=',',
#                            inferSchema=True, header=True)
# # # show data frame
# dataframe.show()
#
# print(dataframe)
