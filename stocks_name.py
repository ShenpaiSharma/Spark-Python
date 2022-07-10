import requests

url = "https://stock-market-data.p.rapidapi.com/market/index/s-and-p-six-hundred"

headers = {
	"X-RapidAPI-Key": "c8a9fb6618msh1e288b5a17f70b4p102165jsn4d53c79c5480",
	"X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
}

response = requests.request("GET", url, headers=headers)

all_stocks = (response.json()['stocks'][:25])
#
# print(all_stocks)

# all_stocks = ["AAPL", "AAON", "GCI"][:2]