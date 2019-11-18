from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from time import sleep
from typing import List
import csv
import multiprocessing.dummy as mp

from alpha_vantage.foreignexchange import ForeignExchange
from alpha_vantage.timeseries import TimeSeries
from dateutil import parser
from dateutil.tz import UTC
import dateutil.tz as tz

from models.asset import Currency, Stock
from models.candle import Candle
from models.constants import INTERVAL_DAY, INTERVAL_HOUR, INTERVAL_MINUTE
import local_config as CONFIG

DAILY_COMPACT_THRESHOLD = 99
DAILY_MAX_RETRIES = 5
DAILY_SYNC_COMPACT = "compact"
DAILY_SYNC_FULL = "full"
DAILY_UPDATE_INTERVAL = 2
MAX_BULK_QUERY = 100

class AssetClass(ABC):

    @abstractmethod
    def __init__(self, provider):
        self.provider = provider
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

    def __init__(self, provider):
        super(CurrencyClass, self).__init__(provider)
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
            except Exception as ex:
                # Log the details of the error if we fail
                CONFIG.DATA_LOGGER.error("CurrencyUpdaterDaily -> sync_asset() -> 1")
                CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                CONFIG.DATA_LOGGER.exception(str(ex))
                # Wait for specified time by configuration
                sleep(CONFIG.ERROR_WAIT_TIME)
                # Increment the number of failures
                counter += 1
                continue
            break
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
        self.interval = INTERVAL_MINUTE
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
            except Exception as ex:
                CONFIG.DATA_LOGGER.error("CurrencyUpdaterLive -> sync_asset() -> 1")
                CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                CONFIG.DATA_LOGGER.exception(str(ex))
                sleep(CONFIG.ERROR_WAIT_TIME)
                counter += 1
                continue
            break
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

    def __init__(self, provider):
        self.api = TimeSeries()
        self.provider = provider
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
            except Exception as ex:
                # Log the details of the error if we fail
                CONFIG.DATA_LOGGER.error("StockUpdaterDaily -> sync_asset() -> 1")
                CONFIG.DATA_LOGGER.error(str(asset.as_dict()))
                CONFIG.DATA_LOGGER.exception(str(ex))
                # Wait for specified time by configuration
                sleep(CONFIG.ERROR_WAIT_TIME)
                # Increment the number of failures
                counter += 1
                continue
            break
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
        self.interval = INTERVAL_MINUTE
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
            except Exception as ex:
                CONFIG.DATA_LOGGER.error("StockUpdaterLive -> sync_asset() -> 1")
                CONFIG.DATA_LOGGER.error(repr(tickers))
                CONFIG.DATA_LOGGER.exception(str(ex))
                sleep(CONFIG.ERROR_WAIT_TIME)
                counter += 1
                continue
            break
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
