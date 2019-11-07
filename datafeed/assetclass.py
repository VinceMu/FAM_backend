from abc import ABC, abstractmethod
from models import *
from datafeed.provider import *
from alpha_vantage.foreignexchange import ForeignExchange
from alpha_vantage.timeseries import TimeSeries
import csv, urllib, requests, io, local_config, json
import dateutil.parser, datetime
from dateutil.tz import UTC
import dateutil.tz as tz
import multiprocessing.dummy as mp

class AssetClass(ABC):
    # Returns the name of the asset class
    def get_name(self):
        pass

    # Returns the name of the data provider for this asset class
    def get_provider(self):
        pass

    # Method called daily to update pricings
    def on_daily(self):
        pass

    # Method called at a select interval to update data for the asset class
    def on_interval(self):
        pass

    # Method called on application startup to query the relevant API for data
    def on_startup(self):
        pass

class CurrencyClass(AssetClass):

    def __init__(self, provider):
        self.api = ForeignExchange()
        self.provider = provider

    def get_name(self):
        return "Currency"

    def get_provider(self):
        return self.provider.get_name()

    def grab_price(self, currency):
        while True:
            try:
                self.provider.make_request()
                rate = self.api.get_currency_exchange_rate(currency.ticker, "USD")
            except:
                print('[DataLink] An error occurred obtaining currency price data... retrying in 1s...')
                sleep(1)
                continue
            break
        datestamp = dateutil.parser.parse(rate[0]['6. Last Refreshed'])
        datestamp = datestamp.replace(tzinfo=UTC)
        currency.price = float(rate[0]['5. Exchange Rate'])
        currency.timestamp = datestamp
        currency.save()

    def grab_daily_history(self, currency):
        interval = 86400
        latest_data = currency.get_last_candle_interval(interval)
        search_type = None
        if latest_data is not None:
            diff = ((datetime.datetime.utcnow()-latest_data.close_time).total_seconds())/interval
            # Will force an update check after the daily close
            if diff >= 2:
                if diff > 99:
                    search_type = "full"
                else:
                    search_type = "compact"
        else:
            search_type = "full"
        if search_type is not None:
            print('[DataLink] Downloading all daily data for ' + currency.name + " (" + currency.ticker + ")")
            while True:
                try:
                    self.provider.make_request()
                    daily_data = self.api.get_currency_exchange_daily(currency.ticker, "USD", outputsize=search_type)
                except:
                    print('[DataLink] An error occurred obtaining daily currency data... retrying in 1s...')
                    sleep(local_config.ERROR_WAIT_TIME)
                    continue
                break
            candles = []
            for part in daily_data[0]:
                entry = daily_data[0][part]
                datestamp = dateutil.parser.parse(part)
                if datestamp.date() == datetime.datetime.utcnow().date():
                    continue
                if latest_data is not None and datestamp <= latest_data.close_time:
                    break
                candle = Candle(asset=currency, open=float(entry['1. open']), high=float(entry['2. high']), low=float(entry['3. low']), close=float(entry['4. close']), close_time=datestamp, interval=interval)
                if candles:
                    last_candle = candles[-1]
                    diff = (last_candle.close_time-candle.close_time).total_seconds()/interval
                    num_candles_required = int(round(diff-1))
                    while (num_candles_required > 0):
                        fake_candle_stamp = candle.close_time + datetime.timedelta(days=num_candles_required)
                        candles.append(Candle(asset=currency, close=candle.close, close_time=fake_candle_stamp, interval=86400))
                        num_candles_required -= 1
                candles.append(candle)
            if candles:
                Candle.objects.insert(candles)
            print('[DataLink] Finished downloading data for ' + currency.name + " (" + currency.ticker + ")")
        else:
            print('[DataLink] No update required on data for ' + currency.name + " (" + currency.ticker + ")")


    def on_daily(self):
        p = mp.Pool(local_config.WORKER_THREADS)
        p.map(self.grab_daily_history, Currency.objects)
        p.close()
        p.join()

    def on_interval(self):
        print('[DataLink] Updating live currency prices...')
        p = mp.Pool(local_config.WORKER_THREADS)
        p.map(self.grab_price, Currency.objects)
        p.close()
        p.join()
        print('[DataLink] Update complete!')

    def on_startup(self):
        if len(Currency.objects) == 0:
            with open("datafeed/defaults/supported_currencies.csv") as csv_file:
                result = csv.reader(csv_file,delimiter=',')
                for row in result:
                    currency = Currency(ticker=row[0], name=row[1])
                    currency.save()

class StocksClass(AssetClass):

    def __init__(self, provider):
        self.api = TimeSeries()
        self.max_bulk_query = 100
        self.provider = provider
        self.query_values = []

    def get_name(self):
        return "Stocks"

    def get_provider(self):
        return self.provider.get_name()

    def grab_price(self, start_index):
        max_index = min(len(self.query_values), start_index+self.max_bulk_query)
        while True:
            try:
                self.provider.make_request()
                stocks = self.api.get_batch_stock_quotes(symbols=self.query_values[start_index:max_index])[0]
            except:
                print('[DataLink] An error occurred obtaining stock price data... retrying in 1s...')
                sleep(1)
                continue
            break
        for stock in stocks:
            datestamp = dateutil.parser.parse(stock['4. timestamp'])
            datestamp = datestamp.replace(tzinfo=tz.gettz("US/Eastern"))
            datestamp = datestamp.astimezone(UTC)
            Stock.objects(ticker=stock['1. symbol']).update(price=float(stock['2. price']), timestamp=datestamp)

    def grab_daily_history(self, stock):
        interval = 86400
        latest_data = stock.get_last_candle_interval(interval)
        search_type = None
        if latest_data is not None:
            last_update_diff = ((datetime.datetime.utcnow()-stock.timestamp).total_seconds())
            # Only update daily data if the market has been closed for 30 minutes at least
            if last_update_diff > 1800:
                latest_date = latest_data.close_time.date()
                # Check if the latest daily date matches the data from the last update retrieved
                if latest_date < stock.timestamp.date():
                    # Approximate the date difference to determine the type of search to perform
                    diff = ((datetime.datetime.utcnow()-latest_data.close_time).total_seconds())/interval
                    if diff > 99:
                        search_type = "full"
                    else:
                        search_type = "compact"
        else:
            search_type = "full"
        if search_type is not None:
            print('[DataLink] Downloading all daily data for ' + stock.name + " (" + stock.ticker + ")")
            while True:
                try:
                    self.provider.make_request()
                    daily_data = self.api.get_daily(stock.ticker, outputsize=search_type)
                except:
                    print('[DataLink] An error occurred obtaining daily stock data... retrying in 1s...')
                    sleep(1)
                    continue
                break
            candles = []
            for part in daily_data[0]:
                entry = daily_data[0][part]
                datestamp = dateutil.parser.parse(part)
                datestamp = datestamp.replace(tzinfo=tz.gettz("US/Eastern"))
                datestamp = datestamp.astimezone(UTC)
                if datestamp.date() == datetime.datetime.utcnow().date():
                    continue
                if latest_data is not None and datestamp <= latest_data.close_time:
                    break
                candle = Candle(asset=stock, open=float(entry['1. open']), high=float(entry['2. high']), low=float(entry['3. low']), close=float(entry['4. close']), volume=float(entry['5. volume']), close_time=datestamp, interval=interval)
                if candles:
                    last_candle = candles[-1]
                    diff = (last_candle.close_time-candle.close_time).total_seconds()/interval
                    num_candles_required = int(round(diff-1))
                    while (num_candles_required > 0):
                        fake_candle_stamp = candle.close_time + datetime.timedelta(days=num_candles_required)
                        candles.append(Candle(asset=stock, close=candle.close, close_time=fake_candle_stamp, interval=86400))
                        num_candles_required -= 1
                candles.append(candle)
            if candles:
                Candle.objects.insert(candles)
            print('[DataLink] Finished downloading data for ' + stock.name + " (" + stock.ticker + ")")
        else:
            print('[DataLink] No update required on data for ' + stock.name + " (" + stock.ticker + ")")

    def on_daily(self):
        p = mp.Pool(local_config.WORKER_THREADS)
        p.map(self.grab_daily_history, Stock.objects)
        p.close()
        p.join()
        #self.create_filler_candles()

    def on_interval(self):
        print('[DataLink] Updating live stock prices...')
        start_index = 0
        array = []
        while start_index < len(self.query_values):
            array.append(start_index)
            start_index += self.max_bulk_query
        p = mp.Pool(local_config.WORKER_THREADS)
        p.map(self.grab_price, array)
        p.close()
        p.join()
        print('[DataLink] Update complete!')

    def on_startup(self):
        if len(Stock.objects) == 0:
            with open("datafeed/defaults/supported_stocks.csv") as csv_file:
                result = csv.reader(csv_file, delimiter=",")
                counter = 0
                for row in result:
                    if local_config.LIMIT_ASSETS == False or counter == 0:
                        stock = Stock(ticker=row[0], name=row[1])
                        stock.save()
                    counter += 1
                    if counter == 50:
                        counter = 0
        for stock in Stock.objects:
            self.query_values.append(stock.ticker)
        pass