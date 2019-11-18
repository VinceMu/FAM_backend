from datetime import datetime

from bson import ObjectId
from mongoengine import DateTimeField, Document, FloatField, LazyReferenceField, ReferenceField

class Transaction(Document):
    user = LazyReferenceField('User', required=True)
    asset = ReferenceField('Asset', required=True)
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

    @staticmethod
    def create(user: 'User', asset: 'Asset', quantity: float, date_purchased: datetime, date_sold: datetime, price_purchased: float, price_sold: float) -> 'Transaction':
        """Creates a Transaction object with the specified properties.
        
        Arguments:
            user {User} -- The User object the Transaction is associated with.
            asset {Asset} -- The Asset that is being used in the Transaction.
            quantity {float} -- The amount of the Asset which is being purchased in the Transaction.
            date_purchased {datetime} -- The date on which the Asset was purchased.
            date_sold {datetime} -- The date on which the Asset was sold; specify None if still owned.
            price_purchased {float} -- The price at which the Asset was purchased.
            price_sold {float} -- The price at which the Asset was sold; specify None if still owned.
        
        Returns:
            Transaction -- The Transaction object encompassing the given details.
        """
        try:
            transaction = Transaction(user=user, asset=asset, quantity=quantity, buy_date=date_purchased, buy_price=price_purchased, sell_date=date_sold, sell_price=price_sold)
            transaction.save()
            user.add_transaction(transaction)
            user.set_portfolio_historical(None)
            user.save()
        except Exception:
            return None
        return transaction

    @staticmethod
    def remove(transaction: 'Transaction') -> bool:
        """Deletes the specified Transaction.
        
        Arguments:
            transaction {Transaction} -- The Transaction to be deleted.
        
        Returns:
            bool -- Returns True if the deletion was successful; False otherwise.
        """
        try:
            user = transaction.get_user()
            user.delete_transaction(transaction)
            user.set_portfolio_historical(None)
            user.save()
            transaction.delete()
        except Exception:
            return False
        return True

    @staticmethod
    def get_by_id(identifier: str) -> 'Transaction':
        """Returns the Transaction object with the given id.
        
        Arguments:
            identifier {str} -- The unique identifier for the Transaction.
        
        Returns:
            Transaction -- The Transaction object with the specified id.
        """
        if not ObjectId.is_valid(identifier):
            return None
        return Transaction.objects(id=identifier).first()

    @staticmethod
    def get_user_current(user: 'User') -> 'QuerySet[Transaction]':
        """Returns the User's current transactions. (i.e. no sell date)
        
        Arguments:
            user {User} -- The user to return the Transactions for.
        
        Returns:
            QuerySet[Transaction] -- Returns an iterable QuerySet containing the User's current Transactions.
        """
        return Transaction.objects(user=user, sell_date=None)

    def as_dict(self) -> dict:
        """Returns the details of the Transaction object as a dictionary.
        
        Returns:
            dict -- Details of the Transaction as a dictionary.
        """
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

    def get_asset(self) -> 'Asset':
        """Returns the Asset object associated with the Transaction.
        
        Returns:
            Asset -- The Asset linked to the Transaction.
        """
        return self.asset

    def get_buy_date(self) -> datetime:
        """Returns the datetime the Transaction occurred.
        
        Returns:
            datetime -- The datetime the purchase occurred.
        """
        return self.buy_date

    def get_buy_price(self) -> float:
        """Returns the price the asset was purchased at in the Transaction.
        
        Returns:
            float -- Price of the Asset at the time of Transaction.
        """
        return self.buy_price

    def get_id(self) -> str:
        """Returns the unique object identifier for the Transaction.
        
        Returns:
            str -- Unique object identifier.
        """
        return str(self.pk)

    def get_profit_percent(self) -> float:
        """Returns the profit (as a percent) achieved by the Transaction.
        
        Returns:
            float -- Profit percent of the Transaction - current price is used if the
            Asset has not been sold.
        """
        buy_price = self.get_buy_price()
        sell_price = self.get_sell_price()
        if sell_price is None:
            sell_price = self.get_asset().get_price()
            if sell_price is None:
                return None
        return ((sell_price-buy_price)/buy_price)*100

    def get_quantity(self) -> float:
        """Returns the number of units of the Asset purchased in the Transaction.
        
        Returns:
            float -- Number of units of the asset purchased.
        """
        return self.quantity

    def get_sell_date(self) -> datetime:
        """Returns the datetime the sale of the Asset occurred.
        
        Returns:
            datetime -- The datetime the sale occurred - value is None if the Asset
            is still owned.
        """
        return self.sell_date

    def get_sell_price(self) -> float:
        """Returns the price the Asset was sold at in the Transaction.
        
        Returns:
            float -- The sale price of the Asset - value is None if the Asset is
            still owned.
        """
        return self.sell_price

    def get_user(self) -> 'User':
        """Returns the User associated with the Transaction.
        
        Returns:
            User -- The linked User object.
        """
        return self.user.fetch()

    def set_asset(self, asset: 'Asset') -> None:
        """Sets the Asset of the Transaction.
        
        Arguments:
            asset {Asset} -- The Asset involved in the Transaction.
        """
        self.asset = asset

    def set_buy_date(self, buy_date: datetime) -> None:
        """Sets the buy date of the Transaction.
        
        Arguments:
            buy_date {datetime} -- The buy date of the Transaction.
        """
        self.buy_date = buy_date

    def set_buy_price(self, buy_price: float) -> None:
        """Sets the buy price of the Transaction.
        
        Arguments:
            buy_price {float} -- The buy price of the Transaction.
        """
        self.buy_price = buy_price

    def set_quantity(self, quantity: float) -> None:
        """Sets the quantity involved in the Transaction.
        
        Arguments:
            quantity {float} -- The quantity involved in the Transaction.
        """
        self.quantity = quantity

    def set_sell_date(self, sell_date: datetime) -> None:
        """Sets the sell date of the Asset in the Transaction.
        
        Arguments:
            sell_date {datetime} -- The sell date of the Asset.
        """
        self.sell_date = sell_date

    def set_sell_price(self, sell_price: float) -> None:
        """Sets the sell price of the Asset in the Transaction.
        
        Arguments:
            sell_price {float} -- The sell price of the Asset.
        """
        self.sell_price = sell_price
