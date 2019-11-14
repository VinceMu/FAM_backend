from mongoengine import Document, StringField, FloatField, DateTimeField, LazyReferenceField, ListField, ReferenceField, FileField, IntField, Q
import datetime, enum

class Interval(enum.Enum):
    Second = 1,
    Minute = Second*60,
    Hour = Minute*60,
    Day = Hour*24,
    Week = Day*7

class Asset(Document):
    ticker = StringField(required=True)
    name = StringField(required=True)
    price = FloatField()
    timestamp = DateTimeField()
    meta = {'allow_inheritance': True}

    def as_dict(self):
        return {
            "id": self.get_id(),
            "name": self.get_name(),
            "ticker": self.get_ticker(),
            "price": self.get_price(),
            "price_timestamp": self.get_price_timestamp(),
            "has_recent_update": self.has_recent_update(),
            "daily_performance": self.get_daily_performance()
        }

    def compare_candle_percent(self, candle):
        if self.get_price() == None:
            return None
        return round(((self.get_price()-candle.get_close())/candle.get_close())*100, 2)

    def get_candles(self, interval=Interval.Day):
        """Returns the candles for the asset on the given Candle interval.
        
        Keyword Arguments:
            interval {integer} -- The number of seconds each Candle represents. (default: {Interval.Day})
        
        Returns:
            QuerySet -- An iterable QuerySet containing Candles in the collection which match the query.
        """
        return Candle.get_asset(self, interval)

    def get_candles_within(self, interval=Interval.Day, start=datetime.datetime.min, finish=datetime.datetime.max):
        """Returns the candles for the asset on the given Candle interval within the timeframe specified.
        
        Keyword Arguments:
            interval {integer} -- The number of seconds each Candle represents. (default: {Interval.Day})
            start {datetime} -- The starting datetime of the interval. (default: {datetime.datetime.min})
            finish {datetime} -- The finishing datetime of the interval. (default: {datetime.datetime.max})
        
        Returns:
            QuerySet -- An iterable QuerySet containing Candles in the collection which match the query.
        """
        return Candle.get_asset_within(self, interval, start, finish)

    def get_daily_candle(self, date):
        """Returns the Interval.Day candle for the given date.
        
        Arguments:
            date {datetime/date} -- The datetime (date extracted) or date for which the Candle is required.
        
        Returns:
            QuerySet -- An iterable QuerySet containing Candles in the collection which match the query.
        """
        if isinstance(date, datetime.datetime):
            date = date.date()
        next_date = date+datetime.timedelta(days=1)
        return Candle.get_asset_within(self, Interval.Day, date, next_date, False, True)

    def get_daily_performance(self):
        if self.has_recent_update() is False:
            return 0
        last_candle = self.get_last_candle(market_open=True)
        if last_candle is None:
            return None
        return {
            "current": self.compare_candle_percent(last_candle),
            "previous": last_candle.get_performance_percent()
        }    

    def get_first_candle(self, interval=Interval.Day):
        """Returns the first candle for the asset on the given interval.
        
        Keyword Arguments:
            interval {integer} -- The number of seconds each Candle represents. (default: {Interval.Day})
        
        Returns:
            Candle -- A Candle object matching the query - None if cannot be found.
        """
        return Candle.get_asset_first_candle(self, interval)

    def get_id(self):
        return str(self.pk)

    def get_name(self):  
        return self.name

    def get_last_candle(self, interval=Interval.Day, market_open=False):
        """Returns the last candle for the asset on the given interval.
        
        Keyword Arguments:
            interval {integer} -- The number of seconds each Candle represents. (default: {Interval.Day})
            market_open {bool} -- Whether the market needs to be open on the provided candle - i.e. not filler candle. (default: {False})
        
        Returns:
            Candle -- A Candle object matching the query - None if cannot be found.
        """
        return Candle.get_asset_last_candle(self, interval, market_open)

    def get_price(self):
        return self.price

    def get_price_timestamp(self):
        return self.timestamp

    def get_ticker(self):
        return self.ticker

    def has_recent_update(self, interval=Interval.Minute*10):
        """Returns whether the asset current price has been updated recently within an optional specified interval.
        
        Keyword Arguments:
            interval {integer} -- The number of seconds to check for a recent update. (default: {Interval.Minute*10})
        
        Returns:
            bool -- True if an update has occurred within the interval; False otherwise.
        """
        last_update_diff = ((datetime.datetime.utcnow()-self.timestamp).total_seconds())
        return (last_update_diff > interval)

class Auth(Document):
    email = StringField(required=True, unique=True)
    password = StringField()
    salt = StringField()

    def get_email(self):
        return self.email

    def get_password(self):
        return self.password

    def get_salt(self):
        return self.salt

class AuthRevokedToken(Document):
    jti = StringField(required=True, unique=True)

    meta = {
        'indexes': [
            'jti'
        ]
    }

    @staticmethod
    def has_token(token):
        return (AuthRevokedToken.objects(jti=token).first() is not None)

class Candle(Document):
    asset = LazyReferenceField(Asset, required=True)
    open = FloatField()
    close = FloatField()
    high = FloatField()
    low = FloatField()
    volume = FloatField()
    close_time = DateTimeField()
    interval = IntField()

    meta = {
        'ordering': ['-close_time'],
        'indexes': [
            'asset',
            'close_time',
            'interval'
        ]
    }

    def as_dict(self):
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
    def get_asset(asset, interval=Interval.Day):
        """Returns a list of candles given the asset and the interval.
        
        Arguments:
            asset {Asset} -- The Asset collection object.
            interval {integer} -- The number of seconds each candle represents. (default: {Interval.Day})
        
        Returns:
            QuerySet -- An iterable QuerySet containing objects in the collection matching the query.
        """
        return Candle.object(asset=asset, interval=interval)

    @staticmethod
    def get_asset_within(asset, interval=Interval.Day, start=datetime.datetime.min, finish=datetime.datetime.max, exclude_start=False, exclude_finish=False):
        """Returns the candles for the given asset and interval within the specified timeframe.
        
        Arguments:
            asset {Asset} -- The Asset collection object.
            interval {integer} -- The number of seconds each candle represents. (default: {Interval.Day})
            start {datetime} -- The starting datetime of the interval. (default: {datetime.datetime.min})
            finish {datetime} -- The finishing datetime of the interval. (default: {datetime.datetime.max})
            exclude_start {bool} -- Whether to exclude the start datetime from the interval. (default: {False})
            exclude_finish {bool} -- Whether to exclude the finish datetime from the interval. (default: {False})
    
        Returns:
            QuerySet -- An iterable QuerySet containing objects in the collection matching the query.
        """
        if start is None:
            start = datetime.datetime.min
        if finish is None:
            finish = datetime.datetime.max
        result_set = Candle.get_asset(asset, interval)
        if exclude_start and exclude_finish:
            return result_set.filter(Q(close_time__gt=start) & Q(close_time__lt=finish))
        elif exclude_start:
            return result_set.filter(Q(close_time__gt=start) & Q(close_time__lte=finish))
        else:
            return result_set.filter(Q(close_time__gte=start) & Q(close_time__lt=finish))

    @staticmethod
    def get_asset_first_candle(asset, interval=Interval.Day):
        """Returns the first candle (date-timewise) for the asset with the given interval.
        
        Arguments:
            asset {Asset} -- The Asset collection object.
            interval {integer} -- The number of seconds each candle represents. (default: {Interval.Day})
        
        Returns:
            QuerySet -- An iterable QuerySet containing objects in the collection matching the query.
        """
        return Candle.object(asset=asset, interval=interval).order_by('close_time').first()

    @staticmethod
    def get_asset_last_candle(asset, interval=Interval.Day, market_open=False):
        """Returns the last candle (date-timewise) for the asset with the given interval.
        
        Arguments:
            asset {Asset} -- The Asset collection object.
            interval {integer} -- The number of seconds each candle represents. (default: {Interval.Day})
            market_open {bool} -- Whether the market needs to be open on the provided candle - i.e. not filler candle. (default: {False})
        
        Returns:
            QuerySet -- An iterable QuerySet containing objects in the collection matching the query.
        """
        if market_open:
            return Candle.object(asset=asset, interval=interval, open__ne=None).first()
        return Candle.object(asset=asset, interval=interval).first()

    def get_close(self):
        return self.close

    def get_high(self):
        return self.high

    def get_interval(self):
        return self.interval

    def get_low(self):
        return self.low

    def get_open(self):
        return self.open

    def get_open_time(self):
        return self.open_time

    def get_performance_percent(self):
        return round(((self.get_close()-self.get_open())/self.get_open())*100,2)

    def get_volume(self):
        return self.volume

class Currency(Asset):
    pass

class Stock(Asset):
    pass

class Transaction(Document):
    user = LazyReferenceField('User', required=True)
    asset = ReferenceField(Asset, required=True)
    quantity = FloatField(required=True)
    buy_date = DateTimeField(required=True)
    buy_price = FloatField(required=True)
    sell_date = DateTimeField()
    sell_price = FloatField()

    meta = {
        'indexes': [
            'user',
            'asset'
        ]
    }

    def as_dict(self):
        return {
            "id": self.get_id(),
            "asset_id": self.get_asset().get_id(),
            "asset_name": self.get_asset().get_name(),
            "asset_ticker": self.get_asset().get_ticker(),
            "asset_price": self.get_asset().get_price(),
            "quantity": self.get_quantity(),
            "buy_date": self.get_buy_date(),
            "buy_price": self.get_buy_price(),
            "sell_date": self.get_sell_date(),
            "sell_price": self.get_sell_price(),
            "profit_percent": self.get_profit_percent()
        }

    def get_asset(self):
        return self.asset

    def get_buy_date(self):
        return self.buy_date

    def get_buy_price(self):
        return self.buy_price

    def get_id(self):
        return str(self.pk)

    def get_profit_percent(self):
        buy_price = self.get_buy_price()
        sell_price = self.get_sell_price()
        if sell_price is None:
            sell_price = self.get_asset().get_price()
            if sell_price is None:
                return None
        return ((sell_price-buy_price)/buy_price)*100

    def get_sell_date(self):
        return self.sell_date

    def get_sell_price(self):
        return self.sell_price

    def get_quantity(self):
        return self.quantity

class User(Document):
    email = StringField(required=True, unique=True)
    fullname = StringField()
    base_currency = ReferenceField(Currency)
    picture = FileField()
    assets = ListField(ReferenceField(Transaction))

    def get_assets(self):
        return self.assets

    def get_base_currency(self):
        return self.base_currency

    def get_email(self):
        return self.email

    def get_name(self):
        return self.name

    def get_picture(self):
        if picture is None:
            return None
        content = self.picture.read()
        if content == None:
            return None
        response = make_response(content)
        response.headers.set("Content-Type", self.picture.content_type)
        response.headers.set("Content-Disposition", "attachment", filename=self.picture.filename)
        return response

    def as_dict(self):
        return {
            "email": self.email,
            "fullname": self.fullname,
            "base_currency": self.base_currency
        }

    def get_portfolio_outliers(self):
        newlist = sorted(self.get_assets().all(), key=lambda x: x.get_profit_percent())
        return newlist