from datetime import datetime, timedelta

from bson import ObjectId
from dateutil.relativedelta import relativedelta
from mongoengine import Document, DateTimeField, FloatField, ReferenceField, StringField

from models.candle import Candle
from models.constants import INTERVAL_DAY
from models.trend import Trend

class Asset(Document):
    ticker = StringField(required=True)
    name = StringField(required=True)
    price = FloatField()
    timestamp = DateTimeField()
    earliest_timestamp = DateTimeField()
    latest_trend = ReferenceField('Trend')
    latest_trend_timestamp = DateTimeField()
    meta = {
        'allow_inheritance': True,
        'indexes': [
            'ticker',
            'name'
        ]
    }

    @staticmethod
    def autocomplete_by_name(name: str) -> 'QuerySet[Asset]':
        """Returns a QuerySet of Assets whose name contains the text specified.
        
        Arguments:
            name {str} -- The name to search the Asset names for.
        
        Returns:
            QuerySet[Asset] -- An iterable QuerySet containing Assets in the collection which match the query.
        """
        return Asset.objects(name__contains=name)

    @staticmethod
    def get() -> 'QuerySet[Asset]':
        """Returns a QuerySet of all Assets.
        
        Returns:
            QuerySet[Asset] -- An iterable QuerySet containing all the Assets in the collection.
        """
        return Asset.objects

    @staticmethod
    def get_by_id(identifier: str) -> 'Asset':
        """Returns an Asset object based on a unique identifier.
        
        Arguments:
            identifier {str} -- The unique identifier for the Asset.
        
        Returns:
            Asset -- The Asset object; None if it cannot be found.
        """
        if not ObjectId.is_valid(identifier):
            return None
        return Asset.objects(id=identifier).first()

    def as_dict(self) -> dict:
        """Returns the details of the Asset object represented as a dictionary.
        
        Returns:
            dict -- A dictionary representing the details of the Asset.
        """
        return {
            "id": self.get_id(),
            "name": self.get_name(),
            "ticker": self.get_ticker(),
            "price": self.get_price(),
            "class": self.get_asset_class(),
            "price_timestamp": self.get_price_timestamp(),
            "has_recent_update": self.has_recent_update(),
            "daily_performance": self.get_daily_performance(),
            "interval_performance": self.get_interval_performance(),
            "earliest_date": self.get_earliest_timestamp()
        }

    def as_dict_autocomplete(self) -> dict:
        """Returns the relevant autocomplete details of an Asset object as a dictionary.
        
        Returns:
            dict -- The relevant details of the Asset.
        """
        return {
            "id": self.get_id(),
            "name": self.get_name(),
            "ticker": self.get_ticker(),
            "class": self.get_asset_class(),
            "earliest_date": self.get_earliest_timestamp()
        }

    def compare_candle_percent(self, candle: 'Asset', use_close: bool = True) -> float:
        """Calculates the percentage change between the current asset price and a given candle's closing price.
        
        Arguments:
            candle {Candle} -- The Candle object to compare to.
            use_cool {bool} -- Whether to use the close_price for comparison (True) or open price (False).
        
        Returns:
            float -- The percentage change.
        """
        if self.get_price() is None:
            return None
        if use_close:
            return round(((self.get_price()-candle.get_close())/candle.get_close())*100, 2)
        return round(((self.get_price()-candle.get_open())/candle.get_open())*100, 2)

    def get_asset_class(self) -> str:
        """Returns the Asset class of the given asset.
        
        Returns:
            str -- The name of the Asset class.
        """
        return self._cls.split('.')[1]

    def get_candles(self, interval: int = INTERVAL_DAY) -> 'QuerySet[Candle]':
        """Returns the candles for the asset on the given Candle interval.
        
        Keyword Arguments:
            interval {int} -- The number of seconds each Candle represents. (default: {INTERVAL_DAY})
        
        Returns:
            QuerySet[Candle] -- An iterable QuerySet containing Candles in the collection which match the query.
        """
        return Candle.get_asset(self, interval)

    def get_candles_within(self, interval: int = INTERVAL_DAY, start: datetime = datetime.min, finish: datetime = datetime.max, exclude_start: bool = False, exclude_finish: bool = False) -> 'QuerySet[Candle]':
        """Returns the candles for the asset on the given Candle interval within the timeframe specified.
        
        Keyword Arguments:
            interval {int} -- The number of seconds each Candle represents. (default: {INTERVAL_DAY})
            start {datetime} -- The starting datetime of the interval. (default: {datetime.datetime.min})
            finish {datetime} -- The finishing datetime of the interval. (default: {datetime.datetime.max})
            exclude_start {bool} -- Whether to exclude the start datetime from the interval. (default: {False})
            exclude_finish {bool} -- Whether to exclude the finish datetime from the interval. (default: {False})
        
        Returns:
            QuerySet[Candle] -- An iterable QuerySet containing Candles in the collection which match the query.
        """
        return Candle.get_asset_within(self, interval, start, finish, exclude_start, exclude_finish)

    def get_daily_candle(self, date: datetime) -> 'Candle':
        """Returns the model.constants.INTERVAL_DAY candle for the given date.
        
        Arguments:
            date {datetime/date} -- The datetime (date extracted) or date for which the Candle is required.
        
        Returns:
            Candle -- The daily Candle object matching the query.
        """
        if isinstance(date, datetime):
            date = date.date()
        next_date = date+timedelta(days=1)
        return Candle.get_asset_within(self, INTERVAL_DAY, date, next_date, False, True).first()

    def get_daily_performance(self) -> dict:
        """Calculates and returns the percentage change on the previous days' trade and the current
        day of trade.
        
        Returns:
            dict -- A dictionary containing values for 'previous' and 'current' reflecting
            the percentage change as specified.
        """
        last_candle = self.get_last_candle(market_open=True)
        if last_candle is None:
            return None
        return {
            "current": self.compare_candle_percent(last_candle),
            "previous": last_candle.get_performance_percent(),
            "combined": self.compare_candle_percent(last_candle, False)
        }

    def get_earliest_timestamp(self) -> datetime:
        """Returns the earliest date timestamp on a candle for the Asset.
        
        Returns:
            datetime -- The timestamp of the earliest candle.
        """
        if self.earliest_timestamp is None:
            return None
        return self.earliest_timestamp.date()

    def get_first_candle(self, interval: int = INTERVAL_DAY) -> 'Candle':
        """Returns the first candle for the asset on the given interval.
        
        Keyword Arguments:
            interval {int} -- The number of seconds each Candle represents. (default: {INTERVAL_DAY})
        
        Returns:
            Candle -- A Candle object matching the query - None if cannot be found.
        """
        return Candle.get_asset_first_candle(self, interval)

    def get_id(self) -> str:
        """Returns the unique object identifier for the Asset.
        
        Returns:
            string -- Unique object identifier as a string.
        """
        return str(self.pk)

    def get_interval_performance(self) -> dict:
        """Calculates and returns the percentage change from a preset selection of
        dates to the current date.
        
        Returns:
            dict -- A dictionary containing the timeframe and the percentage performance.
        """
        current = datetime.now().date()
        return {
            "1W": self.get_percent_change(current-timedelta(weeks=1)),
            "1M": self.get_percent_change(current-relativedelta(months=1)),
            "3M": self.get_percent_change(current-relativedelta(months=3)),
            "6M": self.get_percent_change(current-relativedelta(months=6)),
            "1Y": self.get_percent_change(current-relativedelta(years=1)),
            "3Y": self.get_percent_change(current-relativedelta(years=3))
        }

    def get_name(self) -> str:
        """Returns the full name of the asset.
        
        Returns:
            string -- The full name of the asset.
        """
        return self.name

    def get_last_candle(self, interval: int = INTERVAL_DAY, market_open: bool = False) -> 'Candle':
        """Returns the last candle for the asset on the given interval.
        
        Keyword Arguments:
            interval {int} -- The number of seconds each Candle represents. (default: {INTERVAL_DAY})
            market_open {bool} -- Whether the market needs to be open on the provided candle - i.e. not filler candle. (default: {False})
        
        Returns:
            Candle -- A Candle object matching the query - None if cannot be found.
        """
        return Candle.get_asset_last_candle(self, interval, market_open)

    def get_latest_trend(self) -> 'Trend':
        """Returns the latest Google Trends object.
        
        Returns:
            Trend -- The latest Trend object.
        """
        return self.latest_trend

    def get_latest_trend_timestamp(self) -> datetime:
        """Returns the timestamp of the latest Trends update for the Asset.
        
        Returns:
            datetime -- The datetime of the latest update.
        """
        return self.latest_trend_timestamp

    def get_percent_change(self, date: datetime, use_close: bool = True) -> float:
        """Returns the percentage change in price from the specified date.
        
        Arguments:
            date {datetime} -- The date to compare the current price to.
            use_close {bool} -- Whether to use the close_price for comparison (True) or open price (False).
        
        Returns:
            float -- The percentage change in price.
        """
        old_candle = self.get_daily_candle(date)
        if old_candle is None:
            return None
        return self.compare_candle_percent(old_candle, use_close)

    def get_price(self) -> float:
        """Returns the most recently updated price for the Asset.
        
        Returns:
            float -- Most recent price for the asset.
        """
        return self.price

    def get_price_timestamp(self) -> datetime:
        """Returns the time the price was most recently updated for the Asset.
        
        Returns:
            datetime -- The time the price was most recently updated.
        """
        return self.timestamp

    def get_ticker(self) -> str:
        """Returns the symbol representing the Asset.
        
        Returns:
            string -- The symbol (or ticker) which represents the asset.
        """
        return self.ticker

    def get_trends(self) -> 'QuerySet[Trend]':
        """Returns the Google Trends data associated with the Asset.
        
        Returns:
            QuerySet[Trend] -- An iterable QuerySet of Trend objects.
        """
        return Trend.get_trends(self.get_name())

    def has_recent_update(self, interval: int = 600) -> bool:
        """Returns whether the asset current price has been updated recently within an optional specified interval.
        
        Keyword Arguments:
            interval {int} -- The number of seconds to check for a recent update. (default: {INTERVAL_MINUTE*10})
        
        Returns:
            bool -- True if an update has occurred within the interval; False otherwise.
        """
        last_update_diff = ((datetime.utcnow()-self.timestamp).total_seconds())
        return last_update_diff < interval

    def set_earliest_timestamp(self, timestamp: datetime) -> None:
        """Sets the earliest timestamp on a candle for the Asset.
        
        Arguments:
            timestamp {datetime} -- The new earliest timestamp on a Candle.
        """
        self.earliest_timestamp = timestamp

    def set_price(self, price: float) -> None:
        """Sets the price of the asset to that specified.
        
        Arguments:
            price {float} -- The current price of the asset.
        """
        self.price = price

    def set_price_timestamp(self, timestamp: datetime) -> None:
        """Sets the timestamp of the price as specified.
        
        Arguments:
            timestamp {datetime} -- Timestamp at which the price of the asset is given.
        """
        self.timestamp = timestamp

    def update_earliest_timestamp(self) -> None:
        """Update the earliest timestamp on a daily candle according to the first
        Candle that can be found.
        """
        earliest_candle = self.get_first_candle()
        if earliest_candle is not None:
            self.set_earliest_timestamp(earliest_candle.get_open_time())

    def update_latest_trend(self) -> None:
        """Update the latest Trend object for the asset.
        """
        self.latest_trend = Trend.get_latest_trend(self.get_name())
        self.latest_trend_timestamp = datetime.utcnow()

class Currency(Asset):
    pass

class Stock(Asset):
    pass
