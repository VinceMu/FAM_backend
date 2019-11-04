from mongoengine import Document, StringField, FloatField, DateTimeField, LazyReferenceField, ListField, ReferenceField

class Asset(Document):
    ticker = StringField(required=True)
    name = StringField(required=True)
    price = FloatField()
    historical_data = LazyReferenceField('HistoricalData')
    meta = {'allow_inheritance': True}

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
    open = FloatField()
    close = FloatField()
    high = FloatField()
    low = FloatField()
    volume = FloatField()
    meta = {'allow_inheritance': True}

class CandleTimed(Candle):
    start_time = DateTimeField()
    interval = DateTimeField()

class Currency(Asset):
    pass

class HistoricalData(Document):
    candles = ListField(ReferenceField(CandleTimed))

class Stock(Asset):
    pass

class User(Document):
    email = StringField(required=True, unique=True)
    fullname = StringField()
    assets = ListField(ReferenceField(AssetOwnership))