from abc import ABC, abstractmethod
from models import *
from datafeed.provider import *
from alpha_vantage.foreignexchange import ForeignExchange
from alpha_vantage.timeseries import TimeSeries
import csv, urllib, requests, io, json
import local_config as CONFIG
import dateutil.parser
import datetime
from dateutil.tz import UTC
import dateutil.tz as tz
import multiprocessing.dummy as mp

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

    # Returns the name of the asset class
    @abstractmethod
    def get_name(self):
        pass

    # Returns the name of the data provider for this asset class
    def get_provider(self):
        if self.provider == None:
            return None
        return self.provider.get_name()

    # Method called at a select interval to update data for the asset class
    def on_interval(self):
        for updater in self.updaters:
            if updater.requires_update():
                updater.do_update()

    # Method called on application startup to query the relevant API for data
    @abstractmethod
    def on_startup(self):
        pass

class IntervalUpdater(ABC):
    def __init__(self, api, provider):
        self.api = api
        self.interval = INTERVAL_DAY
        self.last_update = None
        self.provider = provider

    # Method called to perform the update for the given interval
    @abstractmethod
    def do_update(self):
        pass

    # Returns whether this interval requires updating at the given time
    def requires_update(self):
        if self.last_update is None:
            return True
        diff = (datetime.datetime.utcnow() - self.last_update).total_seconds()
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
                    if full_load or search == None:
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

    def sync_asset(self, asset: Asset):
        CONFIG.DATA_LOGGER.info("CurrencyUpdaterDaily -> sync_asset(" + asset.get_name() + ") -> start")
        # Get the last updated candle
        latest_candle = asset.get_last_candle()
        # By default we won't update
        sync_type = None
        if latest_candle is not None:
            curr_time = datetime.datetime.utcnow()
            # Calculate how many days ago the last full synced candle was
            diff = (curr_time-latest_candle.get_open_time()).total_seconds()/self.interval
            CONFIG.DATA_LOGGER.debug("CurrencyUpdaterDaily -> sync_asset(" + asset.get_name() + ") -> diff is " + str(diff))
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
            CONFIG.DATA_LOGGER.info("CurrencyUpdaterDaily -> sync_asset(" + asset.get_name() + ") -> finish(nosync)")
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
                datestamp = dateutil.parser.parse(date)
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
            if datestamp.date() == datetime.datetime.utcnow().date():
                continue
            # If we find a candle stamped prior to our most recently updated one,
            # then we know we can terminate parsing the response here
            if latest_candle is not None and datestamp <= latest_candle.get_open_time():
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
                target_day = datetime.datetime.utcnow().date()
            # Calculate the potential 'filler' candle - i.e. if we have a weekend or closed trading day
            filler_candle_stamp = candle.get_open_time() + datetime.timedelta(days=1)
            # Loop over the days between the candle date and the target date
            while (filler_candle_stamp.date() != target_day):
                # Add the filler candles with closes set to the earliest candle
                candles.append(Candle(asset=asset, close=candle.get_close(), open_time=filler_candle_stamp, interval=self.interval))
                filler_candle_stamp = filler_candle_stamp + datetime.timedelta(days=1)
            # Finally append the original candle (to maintain the insertion order)
            candles.append(candle)
        CONFIG.DATA_LOGGER.info("CurrencyUpdaterDaily -> sync_asset(" + asset.get_name() + ") -> finish(sync)")
        if candles:
            # if we have candles to insert, then insert them all now
            Candle.objects.insert(candles)

class CurrencyUpdaterLive(IntervalUpdater):
    def __init__(self, api, provider):
        self.api = api
        self.interval = INTERVAL_MINUTE
        self.last_update = None
        self.name = "Live"
        self.provider = provider

    def do_update(self):
        CONFIG.DATA_LOGGER.info("CurrencyUpdaterLive -> do_update() -> start")
        pool = mp.Pool(CONFIG.WORKER_THREADS)
        pool.map(self.sync_asset, Currency.objects)
        pool.close()
        pool.join()
        CONFIG.DATA_LOGGER.info("CurrencyUpdaterLive -> do_update() -> finish")

    def sync_asset(self, asset: Asset):
        CONFIG.DATA_LOGGER.info("CurrencyUpdaterLive -> sync_asset(" + asset.get_name() + ") -> start")
        counter = 0
        while (counter < CONFIG.MAX_RETRIES):
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
            datestamp = dateutil.parser.parse(data[0]['6. Last Refreshed'])
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
        CONFIG.DATA_LOGGER.info("CurrencyUpdaterLive -> sync_asset(" + asset.get_name() + ") -> finish")

class StockClass(AssetClass):

    def __init__(self, provider):
        self.api = TimeSeries()
        self.provider = provider
        self.updaters = []

    def get_name(self):
        return "Stocks"

    def on_startup(self):
        CONFIG.DATA_LOGGER.info("StocksClass -> on_startup() -> start")
        full_load = (len(Stock.objects) == 0)
        stocks = []
        try:
            with open("datafeed/defaults/supported_stocks.csv") as csv_file:
                result = csv.reader(csv_file, delimiter=',')
                for row in result:
                    if not full_load:
                        search = Stock.objects(ticker=row[0], name=row[1]).first()
                    if full_load or search is None:
                        stock = Stock(ticker=row[0], name=row[1])
                        stocks.append(stock)
                    if CONFIG.LIMIT_ASSETS and len(stocks) > CONFIG.LIMIT_ASSETS_QUANTITY:
                        break
        except Exception as ex:
            CONFIG.DATA_LOGGER.error("StockClass -> on_startup() -> 1")
            CONFIG.DATA_LOGGER.exception(str(ex))
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

    def sync_asset(self, asset: Asset):
        CONFIG.DATA_LOGGER.info("StockUpdaterDaily -> sync_asset(" + asset.get_name() + ") -> start")
        # Get the last updated candle
        latest_candle = asset.get_last_candle()
        # By default we won't update
        sync_type = None
        if latest_candle is not None:
            curr_time = datetime.datetime.utcnow()
            # Calculate how many days ago the last full synced candle was
            diff = (curr_time-latest_candle.get_open_time()).total_seconds()/self.interval
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
            CONFIG.DATA_LOGGER.info("StockUpdaterDaily -> sync_asset(" + asset.get_name() + ") -> finish(nosync)")
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
                datestamp = dateutil.parser.parse(date)
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
            if datestamp.date() == datetime.datetime.utcnow().date():
                continue
            # If we find a candle stamped prior to our most recently updated one,
            # then we know we can terminate parsing the response here
            if latest_candle is not None and datestamp <= latest_candle.get_open_time():
                break
            try:
                # Try and parse all the elements to create the candle object
                candle = Candle(asset=asset, open=float(candle_data['1. open']), high=float(candle_data['2. high']), low=float(candle_data['3. low']), close=float(candle_data['4. close']), volume=float(candle_data['5.volume']), open_time=datestamp, interval=INTERVAL_DAY)
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
                target_day = datetime.datetime.utcnow().date()
            # Calculate the potential 'filler' candle - i.e. if we have a weekend or closed trading day
            filler_candle_stamp = candle.get_open_time() + datetime.timedelta(days=1)
            # Loop over the days between the candle date and the target date
            while (filler_candle_stamp.date() != target_day):
                # Add the filler candles with closes set to the earliest candle
                candles.append(Candle(asset=asset, close=candle.get_close(), open_time=filler_candle_stamp, interval=self.interval))
                filler_candle_stamp = filler_candle_stamp + datetime.timedelta(days=1)
            # Finally append the original candle (to maintain the insertion order)
            candles.append(candle)
        if candles:
            # if we have candles to insert, then insert them all now
            Candle.objects.insert(candles)
        CONFIG.DATA_LOGGER.info("StockUpdaterDaily -> sync_asset(" + asset.get_name() + ") -> finish(sync)")

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
            array.append(o.get_ticker() for o in Stock.objects[index:max_index])
            index += MAX_BULK_QUERY
        pool = mp.Pool(CONFIG.WORKER_THREADS)
        pool.map(self.sync_asset, array)
        pool.close()
        pool.join()
        CONFIG.DATA_LOGGER.info("StockUpdaterLive -> do_update() -> finish")

    def sync_asset(self, tickers):
        CONFIG.DATA_LOGGER.info("StockUpdaterLive -> sync_asset(" + str(len(tickers)) + " stocks) -> start")
        counter = 0
        while (counter < CONFIG.MAX_RETRIES):
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
            asset = Stock.objects(ticker=stock['1. symbol'])
            if asset is None:
                CONFIG.DATA_LOGGER.error("StockUpdaterLive -> sync_asset() -> 2")
                CONFIG.DATA_LOGGER.error(repr(stock))
                return [False, 0]
            try:
                datestamp = dateutil.parser.parse(stock['4. timestamp'])
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
        CONFIG.DATA_LOGGER.info("StockUpdaterLive -> sync_asset(" + str(len(tickers)) + " stocks) -> start")
        return [True, len(data)]