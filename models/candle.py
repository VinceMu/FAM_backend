from datetime import datetime
from mongoengine import DateTimeField, Document, FloatField, IntField, LazyReferenceField, Q

from .constants import INTERVAL_DAY

class Candle(Document):
    asset = LazyReferenceField('Asset', required=True)
    open = FloatField()
    close = FloatField()
    high = FloatField()
    low = FloatField()
    volume = FloatField()
    open_time = DateTimeField()
    interval = IntField()

    meta = {
        'ordering': ['-open_time'],
        'indexes': [
            'asset',
            'open_time',
            'interval'
        ]
    }

    def as_dict(self) -> dict:
        """Returns the details of the Candle object represented as a dictionary.
        
        Returns:
            dict -- Details of the Candle object.
        """
        return {
            "open": self.get_open(),
            "close": self.get_close(),
            "high": self.get_high(),
            "low": self.get_low(),
            "volume": self.get_volume(),
            "open_time": self.get_open_time(),
            "interval": self.get_interval()
        }

    @staticmethod
    def get_asset(asset: 'Asset', interval: int = INTERVAL_DAY) -> 'QuerySet[Candle]':
        """Returns a list of candles given the asset and the interval.
        
        Arguments:
            asset {Asset} -- The Asset collection object.
            interval {int} -- The number of seconds each candle represents. (default: {INTERVAL_DAY})
        
        Returns:
            QuerySet[Candle] -- An iterable QuerySet containing objects in the collection matching the query.
        """
        return Candle.objects(asset=asset, interval=interval)

    @staticmethod
    def get_asset_within(asset: 'Asset', interval: int = INTERVAL_DAY, start: datetime = datetime.min, finish: datetime = datetime.max, exclude_start: bool = False, exclude_finish: bool = False) -> 'QuerySet[Candle]':
        """Returns the candles for the given asset and interval within the specified timeframe.
        
        Arguments:
            asset {Asset} -- The Asset collection object.
            interval {int} -- The number of seconds each candle represents. (default: {INTERVAL_DAY})
            start {datetime} -- The starting datetime of the interval. (default: {datetime.min})
            finish {datetime} -- The finishing datetime of the interval. (default: {datetime.max})
            exclude_start {bool} -- Whether to exclude the start datetime from the interval. (default: {False})
            exclude_finish {bool} -- Whether to exclude the finish datetime from the interval. (default: {False})
    
        Returns:
            QuerySet[Candle] -- An iterable QuerySet containing objects in the collection matching the query.
        """
        if start is None:
            start = datetime.min
        if finish is None:
            finish = datetime.max
        result_set = Candle.get_asset(asset, interval)
        if exclude_start and exclude_finish:
            return result_set.filter(Q(open_time__gt=start) & Q(open_time__lt=finish))
        if exclude_start:
            return result_set.filter(Q(open_time__gt=start) & Q(open_time__lte=finish))
        return result_set.filter(Q(open_time__gte=start) & Q(open_time__lt=finish))

    @staticmethod
    def get_asset_first_candle(asset: 'Asset', interval: int = INTERVAL_DAY) -> 'Candle':
        """Returns the first candle (date-timewise) for the asset with the given interval.
        
        Arguments:
            asset {Asset} -- The Asset collection object.
            interval {int} -- The number of seconds each candle represents. (default: {INTERVAL_DAY})
        
        Returns:
            QuerySet -- An iterable QuerySet containing objects in the collection matching the query.
        """
        return Candle.objects(asset=asset, interval=interval).order_by('open_time').first()

    @staticmethod
    def get_asset_last_candle(asset: 'Asset', interval: int = INTERVAL_DAY, market_open: bool = False) -> 'Candle':
        """Returns the last candle (date-timewise) for the asset with the given interval.
        
        Arguments:
            asset {Asset} -- The Asset collection object.
            interval {int} -- The number of seconds each candle represents. (default: {INTERVAL_DAY})
            market_open {bool} -- Whether the market needs to be open on the provided candle - i.e. not filler candle. (default: {False})
        
        Returns:
            Candle -- The Candle object representing the most recent for the interval; None if doesn't exist.
        """
        if market_open:
            return Candle.objects(asset=asset, interval=interval, open__ne=None).first()
        return Candle.objects(asset=asset, interval=interval).first()

    def get_close(self) -> float:
        """Returns the closing price of the Candle.
        
        Returns:
            float -- Closing price of the Candle.
        """
        return self.close

    def get_high(self) -> float:
        """Returns the high price in the Candle.
        
        Returns:
            float -- High price in the Candle.
        """
        return self.high

    def get_interval(self) -> float:
        """Returns the number of seconds the candle lasted for.
        
        Returns:
            int -- Number of seconds the candle represents.
        """
        return self.interval

    def get_low(self) -> float:
        """Returns the low price of the Candle.
        
        Returns:
            float -- Low price of the candle.
        """
        return self.low

    def get_open(self) -> float:
        """Returns the opening price of the Candle.
        
        Returns:
            float -- Opening price of the Candle.
        """
        return self.open

    def get_open_time(self) -> datetime:
        """Returns the opening time of the Candle.
        
        Returns:
            datetime -- Opening time of the Candle representing in UTC.
        """
        return self.open_time

    def get_performance_percent(self) -> float:
        """Returns the percentage change over the course of the Candle.
        
        Returns:
            float -- Percent change from open to close for the candle, rounded to 2.d.p.
        """
        return round(((self.get_close()-self.get_open())/self.get_open())*100, 2)

    def get_volume(self) -> float:
        """Returns the units of volume associated with the Candle.
        
        Returns:
            float -- Volume of the candle; None if not provided.
        """
        return self.volume
