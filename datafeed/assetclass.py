from abc import ABC, abstractmethod
from models import *
from alpha_vantage.foreignexchange import ForeignExchange
from alpha_vantage.timeseries import TimeSeries
import csv, urllib, requests, io, local_config, json

class AssetClass(ABC):
    # Returns the name of the asset class
    def get_name(self):
        pass

    # Returns the name of the data provider for this asset class
    def get_provider(self):
        pass

    # Method called at a select interval to update data for the asset class
    def on_interval(self):
        pass

    # Method called on application startup to query the relevant API for data
    def on_startup(self):
        pass

class CurrencyClass(AssetClass):

    def __init__(self):
        self.api = ForeignExchange()

    def get_name(self):
        return "Currency"

    def get_provider(self):
        return "AlphaVantage"

    def on_interval(self):
        print('[DataLink] Updating live currency prices...')
        for currency in Currency.objects:
            rate = self.api.get_currency_exchange_rate(currency.ticker, "USD")
            currency.price = float(rate[0]['5. Exchange Rate'])
            currency.save()
        print('[DataLink] Update complete!')

    def on_startup(self):
        if len(Currency.objects) == 0:
            with open("datafeed/defaults/supported_currencies.csv") as csv_file:
                result = csv.reader(csv_file,delimiter=',')
                for row in result:
                    currency = Currency(ticker=row[0], name=row[1])
                    currency.save()

class StocksClass(AssetClass):

    def __init__(self):
        self.api = TimeSeries()
        self.max_bulk_query = 100
        self.query_values = []

    def get_name(self):
        return "Stocks"

    def get_provider(self):
        return "AlphaVantage"

    def on_interval(self):
        print('[DataLink] Updating live stock prices...')
        start_index = 0
        while start_index < len(self.query_values):
            max_index = min(len(self.query_values), start_index+self.max_bulk_query)
            stocks = self.api.get_batch_stock_quotes(symbols=self.query_values[start_index:max_index])[0]
            for stock in stocks:
                Stock.objects(ticker=stock['1. symbol']).update(price=float(stock['2. price']))
            start_index += self.max_bulk_query
        print('[DataLink] Update complete!')

    def on_startup(self):
        if len(Stock.objects) == 0:
            with open("datafeed/defaults/supported_stocks.csv") as csv_file:
                result = csv.reader(csv_file, delimiter=",")
                for row in result:
                    stock = Stock(ticker=row[0], name=row[1])
                    stock.save()
        for stock in Stock.objects:
            self.query_values.append(stock.ticker)
        pass