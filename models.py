from mongoengine import Document, StringField, FloatField, DateTimeField, LazyReferenceField, ListField, ReferenceField, FileField, IntField
import datetime

class Asset(Document):
    ticker = StringField(required=True)
    name = StringField(required=True)
    price = FloatField()
    timestamp = DateTimeField()
    historical_data = LazyReferenceField('HistoricalData')
    meta = {'allow_inheritance': True}

    def get_candles(self):
        return Candle.objects(asset=self)

    def get_candles_interval(self, interval):
        return Candle.objects(asset=self, interval=interval)

    def get_candles_interval_sorted(self, interval):
        return Candle.objects(asset=self, interval=interval).order_by('-close_time')

    def get_last_candle_interval(self, interval):
        return Candle.objects(asset=self, interval=interval).order_by('-close_time').first()

    def is_closed(self):
        last_update_diff = ((datetime.datetime.utcnow()-self.timestamp).total_seconds())
        if last_update_diff > 1800:
            return True
        return False

    def serialize(self):
        fields = {}
        fields['id'] = str(self.pk)
        fields['ticker'] = self.name
        fields['price'] = self.price
        fields['timestamp'] = self.timestamp
        fields['is_closed'] = self.is_closed()
        candle = self.get_last_candle_interval(86400)
        fields['last_daily_candle'] = candle.serialize_price()
        fields['last_performance_percent'] = round((candle.close - candle.open)/candle.open*100,2)
        fields['curr_performance_percent'] = round((self.price - candle.close)/candle.close*100,2)
        return fields

    def serialize_price(self):
        fields = {}
        fields['closed'] = self.is_closed()
        fields['price'] = self.price
        fields['timestamp'] = self.timestamp
        return fields

class AssetOwnership(Document):
    user = LazyReferenceField('User', required=True)
    asset = ReferenceField(Asset, required=True)
    quantity = FloatField(required=True)
    date_purchased = DateTimeField(required=True)
    date_sold = DateTimeField()

    def serialize(self):
        fields = {}
        fields['id'] = str(self.pk)
        fields['asset_id'] = str(self.asset.pk)
        fields['asset_name'] = self.asset.name
        fields['asset_ticker'] = self.asset.ticker
        fields['asset_price'] = self.asset.price
        fields['quantity'] = self.quantity
        fields['date_purchased'] = self.date_purchased
        fields['date_sold'] = self.date_sold
        return fields

class Auth(Document):
    email = StringField(required=True, unique=True)
    password = StringField()
    salt = StringField()

class AuthRevokedToken(Document):
    jti = StringField(required=True, unique=True)

class Candle(Document):
    asset = LazyReferenceField(Asset, required=True)
    open = FloatField()
    close = FloatField()
    high = FloatField()
    low = FloatField()
    volume = FloatField()
    close_time = DateTimeField()
    interval = IntField()

    def serialize_price(self):
        fields = {}
        fields['open'] = self.open
        fields['close'] = self.close
        fields['timestamp'] = self.close_time
        return fields

class Currency(Asset):
    pass

class Stock(Asset):
    pass

class User(Document):
    email = StringField(required=True, unique=True)
    fullname = StringField()
    base_currency = ReferenceField(Currency)
    picture = FileField()
    assets = ListField(ReferenceField(AssetOwnership))