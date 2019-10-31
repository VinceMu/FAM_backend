import mongoengine

class Auth(mongoengine.Document):
    email = mongoengine.StringField(required=True, unique=True)
    fullname = mongoengine.StringField()
    password = mongoengine.StringField()
    salt = mongoengine.StringField()

class Currency(mongoengine.Document):
    ticker = mongoengine.StringField(required=True,unique=True)
    name = mongoengine.StringField()
    rate = mongoengine.FloatField()

class Stock(mongoengine.Document):
    ticker = mongoengine.StringField(required=True, unique=True)
    name = mongoengine.StringField()
    price = mongoengine.FloatField()