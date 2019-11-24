from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from time import sleep
from typing import List
import csv
import json
import os
import multiprocessing.dummy as mp

from alpha_vantage.foreignexchange import ForeignExchange
from alpha_vantage.timeseries import TimeSeries
from dateutil import parser
from dateutil.relativedelta import relativedelta
from dateutil.tz import UTC
from pytrends.request import TrendReq
import dateutil.tz as tz

from models.asset import Currency, Stock
from models.candle import Candle
from models.constants import INTERVAL_DAY, INTERVAL_HOUR, INTERVAL_MINUTE, INTERVAL_MONTH, INTERVAL_WEEK
from models.trend import Trend
import local_config as CONFIG

DAILY_COMPACT_THRESHOLD = 99
DAILY_MAX_RETRIES = 5
DAILY_SYNC_COMPACT = "compact"
DAILY_SYNC_FULL = "full"
DAILY_UPDATE_INTERVAL = 2
MAX_BULK_QUERY = 100

class AssetClass(ABC):

    @abstractmethod
    def __init__(self, provider, trends_provider):
        self.provider = provider
        self.trends_provider = trends_provider
        self.updaters = []

    @abstractmethod
    def get_name(self) -> str:
        """Return the name of the AssetClass.
        
        Returns:
            str -- The name of the Asset class.
        """

    def get_provider(self) -> str:
        """Returns the name of the data provider for this AssetClass.
        
        Returns:
            str -- The name of the data provider.
        """
        if self.provider is None:
            return None
        return self.provider.get_name()

    def on_interval(self) -> None:
        """Checks whether updates are required on each of the intervals and
        applies the updates if they are required.
        """
        for updater in self.updaters:
            if updater.requires_update():
                updater.do_update()

    @abstractmethod
    def on_startup(self) -> None:
        """Method called on startup to ensure the environment is setup for the new AssetClass.
        """

class IntervalUpdater(ABC):
    @abstractmethod
    def __init__(self, api, provider):
        self.api = api
        self.interval = INTERVAL_DAY
        self.last_update = None
        self.provider = provider

    @abstractmethod
    def do_update(self) -> None:
        """Method called to perform the update on the given interval for an AssetClass.
        """

    def requires_update(self) -> bool:
        """Determines whether the data requires updating yet on the interval.
        
        Returns:
            bool -- True if it requires updating; False otherwise.
        """
        if self.last_update is None:
            return True
        diff = (datetime.utcnow() - self.last_update).total_seconds()
        return diff >= self.interval

class CurrencyClass(AssetClass):

    def __init__(self, provider, trends_provider):
        super(CurrencyClass, self).__init__(provider, trends_provider)
        self.api = ForeignExchange()
        self.updaters = []

    def get_name(self):
        return "Currency"

    def on_startup(self):
        CONFIG.DATA_LOGGER.info("CurrencyClass -> on_startup() -> start")
        full_load = (len(Currency.objects) == 0)
        currencies = []
        try:
            with open("datafeed/defaults/supported_currencies.csv") as csv_file:
                result = csv.reader(csv_file, delimiter=',')
                for row in result:
                    if not full_load:
                        search = Currency.objects(ticker=row[0], name=row[1]).first()
                    if full_load or search is None:
                        currency = Currency(ticker=row[0], name=row[1])
                        currencies.append(currency)
        except Exception as ex:
            CONFIG.DATA_LOGGER.error("CurrencyClass -> on_startup() -> 1")
            CONFIG.DATA_LOGGER.exception(str(ex))
        if currencies:
            Currency.objects.insert(currencies)
        self.updaters.append(CurrencyUpdaterLive(self.api, self.provider))
        self.updaters.append(CurrencyUpdaterDaily(self.api, self.provider))
        self.updaters.append(AssetUpdaterAggregation(self.api, self.provider, Currency.objects))
        self.updaters.append(AssetUpdaterTrends(None, self.trends_provider, Currency.objects))
        CONFIG.DATA_LOGGER.info("CurrencyClass -> on_startup() -> finish")

class CurrencyUpdaterDaily(IntervalUpdater):
    def __init__(self, api, provider):
        self.api = api
        self.interval = INTERVAL_HOUR
        self.last_update = None
        self.name = "Daily"
        self.provider = provider

    def do_update(self):
        CONFIG.DATA_LOGGER.info("CurrencyUpdaterDaily -> do_update() -> start")
        pool = mp.Pool(CONFIG.WORKER_THREADS)
        pool.map(self.sync_asset, Currency.objects)
        pool.close()
        pool.join()
        CONFIG.DATA_LOGGER.info("CurrencyUpdaterDaily -> do_update() -> finish")
        self.last_update = datetime.utcnow()

    def sync_asset(self, asset: 'Asset') -> List:
        """Updates the daily currency data for the specified asset.
        
        Arguments:
            asset {Asset} -- The asset to be updated.
        
        Returns:
            List -- Returns a list containing whether the update was successful (bool) and
            the number of entries which were updated (int).
        """
        CONFIG.DATA_LOGGER.info("CurrencyUpdaterDaily -> sync_asset(%s) -> start", asset.get_name())
        # Get the last updated candle
        latest_candle = asset.get_last_candle()
        # By default we won't update
        sync_type = None
        if latest_candle is not None:
            curr_time = datetime.utcnow()
            # Calculate how many days ago the last full synced candle was
            diff = (curr_time-latest_candle.get_open_time()).total_seconds()/INTERVAL_DAY
            CONFIG.DATA_LOGGER.debug("CurrencyUpdaterDaily -> sync_asset(%s) -> diff is %s", asset.get_name(), str(diff))
            # Only update if it is greater than the update interval (2 days - as it is open)
            if diff > DAILY_UPDATE_INTERVAL:
                # Compact sync will only return last 100 candles, so to reduce network usage
                # only sync what we need to
                if diff >= DAILY_COMPACT_THRESHOLD:
                    sync_type = DAILY_SYNC_FULL
                else:
                    sync_type = DAILY_SYNC_COMPACT
        else:
            # If we have no Candles in the dataset, we need to do a full sync
            sync_type = DAILY_SYNC_FULL
        # Return success in syncing 0 results
        if sync_type is None:
            CONFIG.DATA_LOGGER.info("CurrencyUpdaterDaily -> sync_asset(%s) -> finish(nosync)", asset.get_name())
            return [True, 0]
        counter = 0
        # Ensure we haven't attempted to sync too many times with failing
        while counter < DAILY_MAX_RETRIES:
            try:
                # Notify the provider we intend on making the request - ensure we are under quotas
                self.provider.make_request()
                # Make the request with the given ticker
                data = self.api.get_currency_exchange_daily(asset.get_ticker(), "USD", outputsize=sync_type)
                counter = DAILY_MAX_RETRIES
            except Exception as ex:
                # Log the details of the error if we fail
                CONFIG.DATA_LOGGER.error("Failed to update daily data for %s -> Attempt %s", asset.get_name(), str(counter))
                #CONFIG.DATA_LOGGER.error("CurrencyUpdaterDaily -> sync_asset() -> 1")
                #CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                #CONFIG.DATA_LOGGER.exception(str(ex))
                # Wait for specified time by configuration
                sleep(CONFIG.ERROR_WAIT_TIME)
                # Increment the number of failures
                counter += 1
                if counter == DAILY_MAX_RETRIES:
                    CONFIG.DATA_LOGGER.error("Failed to update daily data for %s -> Attempt %s (Terminated)", asset.get_name(), str(counter))
                    os._exit(1)
        # If no data was returned, notify that we failed to sync the data
        if data is None:
            return [False, 0]
        candles = []
        # Loop through all the days in the response
        for date in data[0]:
            # Extract the data for a particular day in the JSON
            candle_data = data[0][date]
            if candle_data is None:
                # Log the details of the error if we get a None response here
                CONFIG.DATA_LOGGER.error("CurrencyUpdaterDaily -> sync_asset() -> 2")
                CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                CONFIG.DATA_LOGGER.error(date)
                # Terminate syncing this asset
                return [False, 0]
            try:
                datestamp = parser.parse(date)
            except Exception as ex:
                CONFIG.DATA_LOGGER.error("CurrencyUpdaterDaily -> sync_asset() -> 3")
                CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                CONFIG.DATA_LOGGER.error(date)
                return [False, 0]
            if datestamp is None:
                # Log the details of the error if we fail to parse the date
                CONFIG.DATA_LOGGER.error("CurrencyUpdaterDaily -> sync_asset() -> 4")
                CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                CONFIG.DATA_LOGGER.error(date)
                # Terminate syncing this asset
                return [False, 0]
            # Don't sync daily data for the current date (candle incomplete)
            if datestamp.date() == datetime.utcnow().date():
                continue
            # If we find a candle stamped prior to our most recently updated one,
            # then we know we can terminate parsing the response here
            if latest_candle is not None and datestamp.date() <= latest_candle.get_open_time().date():
                break
            try:
                # Try and parse all the elements to create the candle object
                candle = Candle(asset=asset, open=float(candle_data['1. open']), high=float(candle_data['2. high']), low=float(candle_data['3. low']), close=float(candle_data['4. close']), open_time=datestamp, interval=INTERVAL_DAY)
            except Exception as ex:
                # If we fail, log all the details of the error
                CONFIG.DATA_LOGGER.error("CurrencyUpdaterDaily -> sync_asset() -> 5")
                CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                CONFIG.DATA_LOGGER.exception(str(ex))
                # Terminate syncing this asset
                return [False, 0]
            # If we have already parsed a candle already from the response
            if candles:
                # Then grab the date of the candle
                target_day = candles[-1].get_open_time().date()
            else:
                # Otherwise grab the current date
                target_day = datetime.utcnow().date()
            # Calculate the potential 'filler' candle - i.e. if we have a weekend or closed trading day
            filler_candle_stamp = candle.get_open_time() + timedelta(days=1)
            # Loop over the days between the candle date and the target date
            while filler_candle_stamp.date() != target_day:
                # Add the filler candles with closes set to the earliest candle
                candles.append(Candle(asset=asset, close=candle.get_close(), open_time=filler_candle_stamp, interval=INTERVAL_DAY))
                filler_candle_stamp = filler_candle_stamp + timedelta(days=1)
            # Finally append the original candle (to maintain the insertion order)
            candles.append(candle)
        if candles:
            # if we have candles to insert, then insert them all now
            Candle.objects.insert(candles)
            asset.update_earliest_timestamp()
            asset.save()
        else:
            target_day = datetime.utcnow().date()
            filler_candle_stamp = latest_candle.get_open_time() + timedelta(days=1)
            while filler_candle_stamp.date() != target_day:
                candles.append(Candle(asset=asset, close=latest_candle.get_close(), open_time=filler_candle_stamp, interval=INTERVAL_DAY))
                filler_candle_stamp = filler_candle_stamp + timedelta(days=1)
            if candles:
                Candle.objects.insert(candles)
        
        CONFIG.DATA_LOGGER.info("CurrencyUpdaterDaily -> sync_asset(%s) -> finish(sync)", asset.get_name())
        return [True, len(candles)]

class CurrencyUpdaterLive(IntervalUpdater):
    def __init__(self, api, provider):
        self.api = api
        self.interval = CONFIG.LIVE_UPDATE_INTERVAL
        self.last_update = None
        self.name = "Live"
        self.provider = provider

    def do_update(self):
        CONFIG.DATA_LOGGER.info("CurrencyUpdaterLive -> do_update() -> start")
        pool = mp.Pool(CONFIG.WORKER_THREADS*2)
        pool.map(self.sync_asset, Currency.objects)
        pool.close()
        pool.join()
        CONFIG.DATA_LOGGER.info("CurrencyUpdaterLive -> do_update() -> finish")
        self.last_update = datetime.utcnow()

    def sync_asset(self, asset: 'Asset') -> List:
        """Updates the live currency price for the specified asset.
        
        Arguments:
            asset {Asset} -- The Asset to update live prices for.
        
        Returns:
            List -- Returns whether the update was successful (bool) and the
            number of entries updated (int).
        """
        CONFIG.DATA_LOGGER.info("CurrencyUpdaterLive -> sync_asset(%s) -> start", asset.get_name())
        counter = 0
        while counter < CONFIG.MAX_RETRIES:
            try:
                self.provider.make_request()
                data = self.api.get_currency_exchange_rate(asset.get_ticker(), "USD")
                counter = CONFIG.MAX_RETRIES
            except Exception as ex:
                CONFIG.DATA_LOGGER.error("Failed to update live data for %s -> Attempt %s", asset.get_name(), str(counter))
                #CONFIG.DATA_LOGGER.error("CurrencyUpdaterLive -> sync_asset() -> 1")
                #CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                #CONFIG.DATA_LOGGER.exception(str(ex))
                sleep(CONFIG.ERROR_WAIT_TIME)
                counter += 1
                if counter == DAILY_MAX_RETRIES:
                    CONFIG.DATA_LOGGER.error("Failed to update live data for %s -> Attempt %s (Terminated)", asset.get_name(), str(counter))
                    os._exit(1)
        if data is None:
            return [False, 0]
        try:
            datestamp = parser.parse(data[0]['6. Last Refreshed'])
            datestamp = datestamp.replace(tzinfo=UTC)
        except Exception as ex:
            CONFIG.DATA_LOGGER.error("CurrencyUpdaterLive -> sync_asset() -> 2")
            CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
            CONFIG.DATA_LOGGER.exception(str(ex))
            return [False, 0]
        if datestamp is None:
            CONFIG.DATA_LOGGER.error("CurrencyUpdaterLive -> sync_asset() -> 3")
            CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
            CONFIG.DATA_LOGGER.error(data[0]['6. Last Refreshed'])
            return [False, 0]
        try:
            asset.set_price(float(data[0]['5. Exchange Rate']))
        except Exception as ex:
            CONFIG.DATA_LOGGER.error("CurrencyUpdaterLive -> sync_asset() -> 4")
            CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
            CONFIG.DATA_LOGGER.exception(str(ex))
            return [False, 0]
        asset.set_price_timestamp(datestamp)
        asset.save()
        CONFIG.DATA_LOGGER.info("CurrencyUpdaterLive -> sync_asset(%s) -> finish", asset.get_name())
        return [True, 1]

class StockClass(AssetClass):

    def __init__(self, provider, trends_provider):
        self.api = TimeSeries()
        self.provider = provider
        self.trends_provider = trends_provider
        self.updaters = []

    def get_name(self):
        return "Stocks"

    def on_startup(self):
        CONFIG.DATA_LOGGER.info("StocksClass -> on_startup() -> start")
        count_stocks = len(Stock.objects)
        stocks = []
        try:
            with open("datafeed/defaults/supported_stocks.csv") as csv_file:
                result = csv.reader(csv_file, delimiter=',')
                for row in result:
                    search = Stock.objects(ticker=row[0], name=row[1]).first()
                    if search is None:
                        stock = Stock(ticker=row[0], name=row[1])
                        stocks.append(stock)
                        count_stocks += 1
                    if CONFIG.LIMIT_ASSETS and count_stocks >= CONFIG.LIMIT_ASSETS_QUANTITY:
                        break
        except Exception as ex:
            CONFIG.DATA_LOGGER.error("StockClass -> on_startup() -> 1")
            CONFIG.DATA_LOGGER.exception(str(ex))
        if stocks:
            Stock.objects.insert(stocks)
        self.updaters.append(StockUpdaterLive(self.api, self.provider))
        self.updaters.append(StockUpdaterDaily(self.api, self.provider))
        self.updaters.append(AssetUpdaterAggregation(self.api, self.provider, Stock.objects))
        self.updaters.append(AssetUpdaterTrends(None, self.trends_provider, Stock.objects))
        CONFIG.DATA_LOGGER.info("StocksClass -> on_startup() -> finish")

class StockUpdaterDaily(IntervalUpdater):
    def __init__(self, api, provider):
        self.api = api
        self.interval = INTERVAL_HOUR
        self.last_update = None
        self.name = "Daily"
        self.provider = provider

    def do_update(self):
        CONFIG.DATA_LOGGER.info("StockUpdaterDaily -> do_update() -> start")
        pool = mp.Pool(CONFIG.WORKER_THREADS)
        pool.map(self.sync_asset, Stock.objects)
        pool.close()
        pool.join()
        CONFIG.DATA_LOGGER.info("StockUpdaterDaily -> do_update() -> finish")
        self.last_update = datetime.utcnow()

    def sync_asset(self, asset: 'Asset') -> List:
        """Updates the daily stock data for the specified asset.
        
        Arguments:
            asset {Asset} -- The Asset to be updated.
        
        Returns:
            List -- Returns a list reflecting whether the update was successful (bool)
            and the number of entries updated (int).
        """
        CONFIG.DATA_LOGGER.info("StockUpdaterDaily -> sync_asset(%s) -> start", asset.get_name())
        # Get the last updated candle
        latest_candle = asset.get_last_candle()
        # By default we won't update
        sync_type = None
        if latest_candle is not None:
            curr_time = datetime.utcnow()
            # Calculate how many days ago the last full synced candle was
            diff = (curr_time-latest_candle.get_open_time()).total_seconds()/INTERVAL_DAY
            CONFIG.DATA_LOGGER.debug("StockUpdaterDaily -> sync_asset(%s) -> diff is %s", asset.get_name(), str(diff))
            # Only update if it is greater than the update interval (2 days - as it is open)
            if diff > DAILY_UPDATE_INTERVAL:
                # Compact sync will only return last 100 candles, so to reduce network usage
                # only sync what we need to
                if diff >= DAILY_COMPACT_THRESHOLD:
                    sync_type = DAILY_SYNC_FULL
                else:
                    sync_type = DAILY_SYNC_COMPACT
        else:
            # If we have no Candles in the dataset, we need to do a full sync
            sync_type = DAILY_SYNC_FULL
        # Return success in syncing 0 results
        if sync_type is None:
            CONFIG.DATA_LOGGER.info("StockUpdaterDaily -> sync_asset(%s) -> finish(nosync)", asset.get_name())
            return [True, 0]
        counter = 0
        # Ensure we haven't attempted to sync too many times with failing
        while counter < DAILY_MAX_RETRIES:
            try:
                # Notify the provider we intend on making the request - ensure we are under quotas
                self.provider.make_request()
                # Make the request with the given ticker
                data = self.api.get_daily(asset.get_ticker(), outputsize=sync_type)
                counter = DAILY_MAX_RETRIES
            except Exception as ex:
                # Log the details of the error if we fail
                CONFIG.DATA_LOGGER.error("Failed to update daily data for %s -> Attempt %s", asset.get_name(), str(counter))
                #CONFIG.DATA_LOGGER.error("StockUpdaterDaily -> sync_asset() -> 1")
                #CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                #CONFIG.DATA_LOGGER.exception(str(ex))
                # Wait for specified time by configuration
                sleep(CONFIG.ERROR_WAIT_TIME)
                # Increment the number of failures
                counter += 1
                if counter == DAILY_MAX_RETRIES:
                    CONFIG.DATA_LOGGER.error("Failed to update daily data for %s -> Attempt %s (Terminated)", asset.get_name(), str(counter))
                    os._exit(1)
        # If no data was returned, notify that we failed to sync the data
        if data is None:
            return [False, 0]
        candles = []
        # Loop through all the days in the response
        for date in data[0]:
            # Extract the data for a particular day in the JSON
            candle_data = data[0][date]
            if candle_data is None:
                # Log the details of the error if we get a None response here
                CONFIG.DATA_LOGGER.error("StockUpdaterDaily -> sync_asset() -> 2")
                CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                CONFIG.DATA_LOGGER.error(date)
                # Terminate syncing this asset
                return [False, 0]
            try:
                datestamp = parser.parse(date)
                datestamp = datestamp.replace(tzinfo=tz.gettz("US/Eastern"))
                datestamp = datestamp.astimezone(UTC).replace(tzinfo=None)
            except Exception as ex:
                CONFIG.DATA_LOGGER.error("StockUpdaterDaily -> sync_asset() -> 3")
                CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                CONFIG.DATA_LOGGER.error(date)
                return [False, 0]
            if datestamp is None:
                # Log the details of the error if we fail to parse the date
                CONFIG.DATA_LOGGER.error("StockUpdaterDaily -> sync_asset() -> 4")
                CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                CONFIG.DATA_LOGGER.error(date)
                # Terminate syncing this asset
                return [False, 0]
            # Don't sync daily data for the current date (candle incomplete)
            if datestamp.date() == datetime.utcnow().date():
                continue
            # If we find a candle stamped prior to our most recently updated one,
            # then we know we can terminate parsing the response here
            if latest_candle is not None and datestamp.date() <= latest_candle.get_open_time().date():
                break
            try:
                # Try and parse all the elements to create the candle object
                candle = Candle(asset=asset, open=float(candle_data['1. open']), high=float(candle_data['2. high']), low=float(candle_data['3. low']), close=float(candle_data['4. close']), volume=float(candle_data['5. volume']), open_time=datestamp, interval=INTERVAL_DAY)
            except Exception as ex:
                # If we fail, log all the details of the error
                CONFIG.DATA_LOGGER.error("StockUpdaterDaily -> sync_asset() -> 5")
                CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                CONFIG.DATA_LOGGER.exception(str(ex))
                # Terminate syncing this asset
                return [False, 0]
            # If we have already parsed a candle already from the response
            if candles:
                # Then grab the date of the candle
                target_day = candles[-1].get_open_time().date()
            else:
                # Otherwise grab the current date
                target_day = datetime.utcnow().date()
            # Calculate the potential 'filler' candle - i.e. if we have a weekend or closed trading day
            filler_candle_stamp = candle.get_open_time() + timedelta(days=1)
            # Loop over the days between the candle date and the target date
            while filler_candle_stamp.date() != target_day:
                # Add the filler candles with closes set to the earliest candle
                candles.append(Candle(asset=asset, close=candle.get_close(), open_time=filler_candle_stamp, interval=INTERVAL_DAY))
                filler_candle_stamp = filler_candle_stamp + timedelta(days=1)
            # Finally append the original candle (to maintain the insertion order)
            candles.append(candle)
        if candles:
            # if we have candles to insert, then insert them all now
            Candle.objects.insert(candles)
            asset.update_earliest_timestamp()
            asset.save()
        else:
            target_day = datetime.utcnow().date()
            filler_candle_stamp = latest_candle.get_open_time() + timedelta(days=1)
            while filler_candle_stamp.date() != target_day:
                candles.append(Candle(asset=asset, close=latest_candle.get_close(), open_time=filler_candle_stamp, interval=INTERVAL_DAY))
                filler_candle_stamp = filler_candle_stamp + timedelta(days=1)
            if candles:
                Candle.objects.insert(candles)
        CONFIG.DATA_LOGGER.info("StockUpdaterDaily -> sync_asset(%s) -> finish(sync)", asset.get_name())
        return [True, len(candles)]

class StockUpdaterLive(IntervalUpdater):
    def __init__(self, api, provider):
        self.api = api
        self.interval = CONFIG.LIVE_UPDATE_INTERVAL
        self.last_update = None
        self.name = "Live"
        self.provider = provider

    def do_update(self):
        CONFIG.DATA_LOGGER.info("StockUpdaterLive -> do_update() -> start")
        index = 0
        array = []
        while index < len(Stock.objects):
            max_index = min(len(Stock.objects), index+MAX_BULK_QUERY)
            portion = []
            for stock in Stock.objects[index:max_index]:
                portion.append(stock.get_ticker())
            array.append(portion)
            index += MAX_BULK_QUERY
        pool = mp.Pool(CONFIG.WORKER_THREADS*2)
        pool.map(self.sync_asset, array)
        pool.close()
        pool.join()
        CONFIG.DATA_LOGGER.info("StockUpdaterLive -> do_update() -> finish")
        self.last_update = datetime.utcnow()

    def sync_asset(self, tickers: List[str]) -> List:
        """Updates the live stock prices for the specified assets.
        
        Arguments:
            tickers {List[str]} -- A list of ticker symbols (at most 100) to update the stock prices for.
        
        Returns:
            List -- Returns in a List whether the update was successful (bool) and the number
            of entries updated (int).
        """
        CONFIG.DATA_LOGGER.info("StockUpdaterLive -> sync_asset(%s to %s) -> start", tickers[0], tickers[-1])
        counter = 0
        while counter < CONFIG.MAX_RETRIES:
            try:
                self.provider.make_request()
                data = self.api.get_batch_stock_quotes(symbols=tickers)[0]
                counter = CONFIG.MAX_RETRIES
            except Exception as ex:
                CONFIG.DATA_LOGGER.error("Failed to update live data for (%s to %s) -> Attempt %s", tickers[0], tickers[-1], str(counter))
                #CONFIG.DATA_LOGGER.error("StockUpdaterLive -> sync_asset() -> 1")
                #CONFIG.DATA_LOGGER.error(repr(tickers))
                #CONFIG.DATA_LOGGER.exception(str(ex))
                sleep(CONFIG.ERROR_WAIT_TIME)
                counter += 1
                if counter == DAILY_MAX_RETRIES:
                    CONFIG.DATA_LOGGER.error("Failed to update live data for (%s to %s) -> Attempt %s (Terminated)", tickers[0], tickers[-1], str(counter))
                    os._exit(1)
        if data is None:
            return [False, 0]
        for stock in data:
            asset = Stock.objects(ticker=stock['1. symbol']).first()
            if asset is None:
                CONFIG.DATA_LOGGER.error("StockUpdaterLive -> sync_asset() -> 2")
                CONFIG.DATA_LOGGER.error(repr(stock))
                return [False, 0]
            try:
                datestamp = parser.parse(stock['4. timestamp'])
                datestamp = datestamp.replace(tzinfo=tz.gettz("US/Eastern"))
                datestamp = datestamp.astimezone(UTC).replace(tzinfo=None)
            except Exception as ex:
                CONFIG.DATA_LOGGER.error("StockUpdaterLive -> sync_asset() -> 3")
                CONFIG.DATA_LOGGER.error(stock['4. timestamp'])
                CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                CONFIG.DATA_LOGGER.exception(str(ex))
                return [False, 0]
            if datestamp is None:
                CONFIG.DATA_LOGGER.error("StockUpdaterLive -> sync_asset() -> 4")
                CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                CONFIG.DATA_LOGGER.error(data[0]['6. Last Refreshed'])
                return [False, 0]
            try:
                asset.set_price(float(stock['2. price']))
            except Exception as ex:
                CONFIG.DATA_LOGGER.error("StockUpdaterLive -> sync_asset() -> 5")
                CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                CONFIG.DATA_LOGGER.exception(str(ex))
                return [False, 0]
            asset.set_price_timestamp(datestamp)
            asset.save()
        CONFIG.DATA_LOGGER.info("StockUpdaterLive -> sync_asset(%s to %s) -> finish", tickers[0], tickers[-1])
        return [True, len(data)]

class AssetUpdaterAggregation(IntervalUpdater):
    def __init__(self, api, provider, source):
        self.api = api
        self.interval = INTERVAL_HOUR
        self.last_update = None
        self.name = "Aggregation"
        self.provider = provider
        self.source = source

    def aggregate_candles(self, asset: 'Asset', interval: int, candles: 'QuerySet[Candle]') -> 'Candle':
        """Combines multiple candles into a given candle of the given interval.
        
        Arguments:
            asset {Asset} -- The Asset for the resulting Candle is for.
            interval {int} -- The time interval (in seconds) of the Candle.
            candles {QuerySet[Candle]} -- An iterable QuerySet of the Candles to be combined.
        
        Returns:
            Candle -- The resulting aggregate Candle for the given data.
        """
        low_price = None
        high_price = None
        volume = 0
        open_price = 0
        open_stamp = None
        close_price = 0
        close_stamp = None
        for candle in candles:
            if candle.get_open() is None:
                continue
            if low_price is None:
                low_price = candle.get_low()
            else:
                if candle.get_low() < low_price:
                    low_price = candle.get_low()
            if high_price is None:
                high_price = candle.get_high()
            else:
                if candle.get_high() > high_price:
                    high_price = candle.get_high()
            if candle.get_volume() is not None:
                volume += candle.get_volume()
            if open_stamp is None or candle.get_open_time() < open_stamp:
                open_price = candle.get_open()
                open_stamp = candle.get_open_time()
            if close_stamp is None or candle.get_open_time() > close_stamp:
                close_price = candle.get_close()
                close_stamp = candle.get_open_time()
        if volume == 0:
            volume = None
        return Candle(asset=asset, low=low_price, high=high_price, open=open_price, close=close_price, volume=volume, open_time=open_stamp, interval=interval)

    def do_update(self):
        CONFIG.DATA_LOGGER.info("AssetUpdaterAggregation -> do_update() -> start")
        pool = mp.Pool(CONFIG.WORKER_THREADS)
        pool.map(self.sync_asset, self.source)
        pool.close()
        pool.join()
        CONFIG.DATA_LOGGER.info("AssetUpdaterAggregation -> do_update() -> finish")
        self.last_update = datetime.utcnow()

    def sync_asset(self, asset: 'Asset') -> None:
        """Calculates the weekly & monthly candles for the given asset.
        
        Arguments:
            asset {Asset} -- The Asset to aggregate the Candles for.
        """
        self.sync_asset_weekly(asset)
        self.sync_asset_monthly(asset)

    def sync_asset_monthly(self, asset: 'Asset') -> List:
        """Calculates the monthly candles based on an aggregation of daily candles
        for the given asset.
        
        Arguments:
            asset {Asset} -- The Asset to perform the calculations on.
        
        Returns:
            List -- Returns in a List whether the update was successful (bool) and the
            number of candles inserted (int).
        """
        CONFIG.DATA_LOGGER.info("AssetUpdaterAggregation -> sync_asset_monthly(%s) -> start", asset.get_name())
        last_candle = asset.get_last_candle(interval=INTERVAL_MONTH)
        candles = []
        if last_candle is None:
            first_daily_candle = asset.get_first_candle()
            if first_daily_candle is None:
                CONFIG.DATA_LOGGER.error("AssetUpdaterAggregation -> sync_asset_monthly() -> 1")
                CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                return [False, 0]
            if first_daily_candle.get_open_time().day != 1:
                first_daily_candle = asset.get_daily_candle(first_daily_candle.get_open_time().replace(day=1)+relativedelta(months=1))
                if first_daily_candle is None:
                    CONFIG.DATA_LOGGER.error("AssetUpdaterAggregation -> sync_asset_monthly() -> 2")
                    CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                    return [False, 0]
            month_start = first_daily_candle.get_open_time().date()
        else:
            last_monthly_candle = asset.get_last_candle(interval=INTERVAL_MONTH)
            if last_monthly_candle is None:
                CONFIG.DATA_LOGGER.error("AssetUpdaterAggregation -> sync_asset_monthly() -> 3")
                CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                return [False, 0]
            month_start = (last_monthly_candle.get_open_time().replace(day=1)+relativedelta(months=1)).date()
        month_end = (month_start+relativedelta(months=1))-timedelta(days=1)
        while month_end < datetime.utcnow().date():
            month_of_candles = asset.get_candles_within(interval=INTERVAL_DAY, start=month_start, finish=month_end)
            candles.append(self.aggregate_candles(asset, INTERVAL_MONTH, month_of_candles))
            month_start = month_start + relativedelta(months=1)
            month_end = (month_start+relativedelta(months=1))-timedelta(days=1)
        if candles:
            Candle.objects.insert(candles)
        CONFIG.DATA_LOGGER.info("AssetUpdaterAggregation -> sync_asset_monthly(%s) -> finish", asset.get_name())
        return [True, len(candles)]


    def sync_asset_weekly(self, asset: 'Asset') -> List:
        """Calculates the weekly candles based on an aggregation of daily candles
        for the given asset.
        
        Arguments:
            asset {Asset} -- The Asset to perform the calculations on.
        
        Returns:
            List -- Returns in a List whether the update was successful (bool) and the
            number of candles inserted (int).
        """
        CONFIG.DATA_LOGGER.info("AssetUpdaterAggregation -> sync_asset_weekly(%s) -> start", asset.get_name())
        last_candle = asset.get_last_candle(interval=INTERVAL_WEEK)
        candles = []
        if last_candle is None:
            first_daily_candle = asset.get_first_candle()
            if first_daily_candle is None:
                CONFIG.DATA_LOGGER.error("AssetUpdaterAggregation -> sync_asset_weekly() -> 1")
                CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                return [False, 0]
            weekday = first_daily_candle.get_open_time().weekday()
            if weekday != 0:
                first_daily_candle = asset.get_daily_candle(first_daily_candle.get_open_time()+timedelta(days=(7-weekday)))
                if first_daily_candle is None:
                    CONFIG.DATA_LOGGER.error("AssetUpdaterAggregation -> sync_asset_weekly() -> 2")
                    CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                    return [False, 0]
            week_start = first_daily_candle.get_open_time().date()
            week_end = week_start + timedelta(days=7)
        else:
            last_weekly_candle = asset.get_last_candle(interval=INTERVAL_WEEK)
            if last_weekly_candle is None:
                CONFIG.DATA_LOGGER.error("AssetUpdaterAggregation -> sync_asset_weekly() -> 3")
                CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                return [False, 0]
            weekday = last_weekly_candle.get_open_time().weekday()
            if weekday != 0:
                week_start = last_weekly_candle.get_open_time().date()+timedelta(days=(7-weekday))
            else:
                week_start = last_weekly_candle.get_open_time().date()+timedelta(days=7)
            week_end = week_start + timedelta(days=7)
        while week_end < datetime.utcnow().date():
            week_of_candles = Candle.get_asset_within(asset=asset, interval=INTERVAL_DAY, start=week_start, finish=week_end, exclude_finish=True)
            candles.append(self.aggregate_candles(asset, INTERVAL_WEEK, week_of_candles))
            week_start = week_start + timedelta(days=7)
            week_end = week_start + timedelta(days=7)
        if candles:
            Candle.objects.insert(candles)
        CONFIG.DATA_LOGGER.info("AssetUpdaterAggregation -> sync_asset_weekly(%s) -> finish", asset.get_name())
        return [True, len(candles)]

class AssetUpdaterTrends(IntervalUpdater):
    def __init__(self, api, provider, source):
        self.api = api
        self.interval = INTERVAL_MINUTE
        self.last_update = None
        self.name = "Trends"
        self.provider = provider
        self.source = source

    def do_update(self):
        CONFIG.DATA_LOGGER.info("AssetUpdaterTrends -> do_update() -> start")
        unused_quota = self.provider.get_unused_quota()
        if unused_quota == 0:
            return
        for asset in self.source:
            if asset.get_latest_trend() is None and asset.get_latest_trend_timestamp() is not None:
                continue
            if asset.get_latest_trend() is None or (datetime.utcnow()-asset.get_latest_trend().get_timestamp()).total_seconds() > 10*INTERVAL_DAY:
                self.sync_trends(asset)
                unused_quota -= 1
            if unused_quota <= 0:
                break
        CONFIG.DATA_LOGGER.info("AssetUpdaterTrends -> do_update() -> finish")

    def sync_trends(self, asset: 'Asset') -> List:
        """Sync the Google Trends data for the specified asset.
        
        Arguments:
            asset {Asset} -- The Asset trend data to update.
        
        Returns:
            [type] -- Returns a List reflecting whether the update was successful (bool)
            and the number of entries updated (int).
        """
        CONFIG.DATA_LOGGER.info("AssetUpdaterTrends -> sync_trends(%s) -> start", asset.get_name())
        self.provider.make_request()
        latest_trend = asset.get_latest_trend()
        if latest_trend is not None and latest_trend.get_timestamp() is not None:
            latest_stamp = latest_stamp.get_timestamp().date()
        else:
            latest_stamp = None
        request = TrendReq()
        request.build_payload([asset.get_name()])
        trends = []
        try:
            trends_over_time = request.interest_over_time()
            json_obj = json.loads(trends_over_time.to_json(orient="index", date_format="iso"))
            for given_date, _ in json_obj.items():
                stamp = parser.parse(given_date)
                if latest_stamp is not None and stamp.date() <= latest_stamp:
                    break
                trends.append(Trend(search_term=asset.get_name(), timestamp=stamp, is_partial=json_obj[given_date]["isPartial"], value=json_obj[given_date][asset.get_name()]))
        except Exception as ex:
            CONFIG.DATA_LOGGER.error("AssetUpdaterTrends -> sync_trends(%s) -> 1", asset.get_name())
            CONFIG.DATA_LOGGER.exception(str(ex))
            return [False, 0]
        if trends:
            Trend.objects.insert(trends)
            asset.update_latest_trend()
        else:
            asset.update_latest_trend()
        asset.save()
        CONFIG.DATA_LOGGER.info("AssetUpdaterTrends -> sync_trends(%s) -> finish", asset.get_name())
        return [True, len(trends)]
