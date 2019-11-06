from mongoengine import Document, StringField, FloatField, DateTimeField, LazyReferenceField, ListField, ReferenceField, FileField, IntField

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

    def get_last_candle_interval(self, interval):
        return Candle.objects(asset=self, interval=interval).order_by('-close_time').first()

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
        return fields

class Auth(Document):
    email = StringField(required=True, unique=True)
    password = StringField()
    salt = StringField()

class Candle(Document):
    asset = LazyReferenceField(Asset, required=True)
    open = FloatField()
    close = FloatField()
    high = FloatField()
    low = FloatField()
    volume = FloatField()
    close_time = DateTimeField()
    interval = IntField()

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