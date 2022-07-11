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
