from datetime import datetime, timedelta
from typing import List
import base64
from flask import make_response
from mongoengine import DateTimeField, DictField, Document, FileField, ListField, ReferenceField, StringField

class User(Document):
    email = StringField(required=True, unique=True)
    fullname = StringField()
    base_currency = ReferenceField('Currency')
    picture = FileField()
    portfolio_historical = DictField()
    portfolio_historical_lastupdate = DateTimeField()
    transactions = ListField(ReferenceField('Transaction'))

    @staticmethod
    def create(email: str, fullname: str) -> 'User':
        """Creates a user with the given details and returns it.
        
        Arguments:
            email {str} -- The email address of the user.
            fullname {str} -- The full name of the user.
        
        Returns:
            User -- A newly created User object with the given details.
        """
        try:
            user = User(email=email, fullname=fullname)
            user.save()
        except Exception:
            return None
        return user

    @staticmethod
    def get_by_email(email: str) -> 'User':
        """Returns the User object associated with the given email.
        
        Arguments:
            email {str} -- The email address to lookup.
        
        Returns:
            User -- The User object associated with the email; None if cannot be found.
        """
        return User.objects(email=email).first()

    def add_transaction(self, transaction: 'Transaction') -> None:
        """Adds the given transaction to the User's list of transactions.
        
        Arguments:
            transaction {Transaction} -- The Transaction object to add.
        """
        self.transactions.append(transaction)

    def as_dict(self) -> dict:
        """Returns the details of the User object as a dictionary.
        
        Returns:
            dict -- Details of the user.
        """
        return {
            "email": self.get_email(),
            "name": self.get_name(),
            "base_currency": self.get_base_currency()
        }

    def delete_transaction(self, transaction: 'Transaction') -> None:
        """Deletes the transaction from the list of the User's transactions.
        
        Arguments:
            transaction {Transaction} -- The Transaction to be removed.
        """
        self.transactions.remove(transaction)

    def get_base_currency(self) -> 'Currency':
        """Returns the base currency of the User.
        
        Returns:
            Currency -- The base Currency of the User - USD is the default.
        """
        return self.base_currency

    def get_email(self) -> str:
        """Returns the email address of the User
        
        Returns:
            str -- The string representing the User's email.
        """
        return self.email

    def get_name(self) -> str:
        """Returns the full name of the User account.
        
        Returns:
            str -- The full name of the User.
        """
        return self.fullname

    def get_portfolio_current(self) -> DictField:
        """Return the current value of the portfolio of the User.
        
        Returns:
            DictField -- Contains three fields:
            - purchase_value: The amount made on closed transactions minus the amount spent
            on opening transactions.
            - net_value: The purchase_value combined with the current value of portfolio.
            - value: The value of the portfolio currently.
        """
        spent_value = 0
        value = 0
        for transaction in self.get_transactions():
            spent_value -= (transaction.get_quantity() * transaction.get_buy_price())
            if transaction.get_sell_date() is None:
                value += (transaction.get_quantity() * transaction.get_asset().get_price())
            else:
                spent_value += (transaction.get_quantity() * transaction.get_sell_price())
        result = {
            "purchase_value": spent_value,
            "net_value": spent_value + value,
            "value": value
        }
        return result

    def get_portfolio_historical(self) -> DictField:
        """Return the current historical value of the portfolio of the user.
        
        Returns:
            DictField -- Returns a dictionary containing all the dates in the history of a User's
            portfolio with a underlying dictionary as per get_portfolio_current(). If the historical
            portfolio is not up-to-date it will return {'empty': True}
        """
        if self.portfolio_historical_lastupdate is None:
            return None
        if self.portfolio_historical_lastupdate.date() != datetime.utcnow().date():
            return None
        return self.portfolio_historical

    def get_picture(self) -> 'Response':
        """Returns the Flask response for serving a profile picture.
        
        Returns:
            response_class -- A Flask response for the profile picture.
        """
        if self.picture is None:
            return None
        content = self.picture.read()
        if content is None:
            return None
        response = make_response(base64.b64encode(content))
        response.headers.set("Content-Type", self.picture.content_type)
        response.headers.set("Content-Disposition", "attachment", filename=self.picture.filename)
        return response

    def get_transactions(self) -> List['Transaction']:
        """Returns a list of the User's Transactions.
        
        Returns:
            List(Transaction) -- List of the Transactions the User was involved in.
        """
        return self.transactions

    def set_base_currency(self, base_currency: 'Currency') -> None:
        """Set the base currency for a User.
        
        Arguments:
            base_currency {Currency} -- The base Currency to set for the User.
        """
        self.base_currency = base_currency

    def set_name(self, name: str) -> None:
        """Set the name for the User.
        
        Arguments:
            name {str} -- The new name for the User.
        """
        self.fullname = name

    def set_picture(self, picture: 'werkzeug.datastructures.FileStorage') -> None:
        """Set the user's profile picture to the file specified.
        
        Arguments:
            picture {werkzeug.datastructures.FileStorage} -- The file storage structure for the picture.
        """
        self.picture.replace(picture)

    def set_portfolio_historical(self, portfolio_historical: datetime) -> None:
        """Set the user's historical portfolio lastupdate value as specified.
        
        Arguments:
            portfolio_historical {datetime} -- The new historical portfolio lastupdate field.
        """
        self.portfolio_historical_lastupdate = portfolio_historical

    def update_portfolio_historical(self) -> None:
        """Update the User's historical portfolio value from their transactions.
        """
        spent_value = {"default": 0}
        value = {"default": 0}
        earliest_date = None
        latest_date = datetime.utcnow().date()
        for transaction in self.get_transactions():
            buy_date = transaction.get_buy_date().date() if (transaction.get_buy_date() is not None) else None
            if earliest_date is None or buy_date < earliest_date:
                earliest_date = buy_date
            sell_date = (transaction.get_sell_date().date()+timedelta(days=1)) if (transaction.get_sell_date() is not None) else None
            candles = transaction.get_asset().get_candles_within(start=buy_date, finish=sell_date)
            for candle in candles:
                tag = str(candle.get_open_time().date())
                if tag in value:
                    spent_value[tag] = spent_value[tag] - (transaction.get_quantity() * transaction.get_buy_price())
                    value[tag] = value[tag] + (transaction.get_quantity() * candle.get_close())
                else:
                    spent_value[tag] = - (transaction.get_quantity() * transaction.get_buy_price())
                    value[tag] = (candle.get_close() * transaction.get_quantity())
            if sell_date is not None:
                curr_date = sell_date
                while curr_date < latest_date:
                    tag = str(curr_date)
                    if tag in spent_value:
                        spent_value[tag] = spent_value[tag] + (transaction.get_quantity() * (transaction.get_sell_price() - transaction.get_buy_price()))
                    else:
                        spent_value[tag] = (transaction.get_quantity() * (transaction.get_sell_price() - transaction.get_buy_price()))
                    curr_date = curr_date + timedelta(days=1)
        if len(value) == 1 or len(spent_value) == 1:
            self.portfolio_historical = {}
            self.portfolio_historical_lastupdate = datetime.utcnow()
            return
        curr_date = earliest_date
        result = {}
        while curr_date < latest_date:
            tag = str(curr_date)
            if tag not in spent_value:
                spent_value[tag] = 0
            if tag not in value:
                value[tag] = 0
            result[tag] = {
                "purchase_value": spent_value[tag],
                "net_value": spent_value[tag] + value[tag],
                "value": value[tag]
            }
            curr_date = curr_date + timedelta(days=1)
        self.portfolio_historical = result
        self.portfolio_historical_lastupdate = datetime.utcnow()
