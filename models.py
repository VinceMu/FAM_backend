from mongoengine import Document, StringField, FloatField, DateTimeField, LazyReferenceField, ListField, ReferenceField

class Asset(Document):
    ticker = StringField(required=True)
    name = StringField(required=True)
    price = FloatField()
    meta = {'allow_inheritance': True}

class AssetOwnership(Document):
    asset = ReferenceField(Asset, required=True)
    quantity = FloatField(required=True)
    date_purchased = DateTimeField(required=True)
    date_sold = DateTimeField()

class Auth(Document):
    email = StringField(required=True, unique=True)
    fullname = StringField()
    password = StringField()
    salt = StringField()
    assets = ListField(ReferenceField(AssetOwnership))

class Currency(Asset):
    pass

class Stock(Asset):
    pass