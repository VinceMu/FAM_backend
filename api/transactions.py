from datetime import datetime

from dateutil import parser
from flask import make_response, jsonify, Response
from flask_jwt_extended import get_jwt_identity, jwt_required
from flask_restplus import abort, Namespace, Resource

from models.asset import Asset
from models.transaction import Transaction
from models.user import User

API = Namespace('transactions', description='transactions endpoint')

CREATE_PARSER = API.parser()
CREATE_PARSER.add_argument('asset_id', type=str, required=True, help='The unique identifier for the asset', location='json')
CREATE_PARSER.add_argument('quantity', type=float, required=True, help='The quantity of the asset', location='json')
CREATE_PARSER.add_argument('date_purchased', type=str, required=True, help='The date the asset was purchased', location='json')
CREATE_PARSER.add_argument('date_sold', type=str, required=False, help='The date the asset was sold (empty if not sold)', location='json')
CREATE_PARSER.add_argument("price_purchased", type=float, required=False, help='The price the asset was purchased at', location='json')
CREATE_PARSER.add_argument('price_sold', type=float, required=False, help='The price the asset was sold at', location='json')

@API.route('/create')
class CreateTransaction(Resource):

    @jwt_required
    @API.expect(CREATE_PARSER)
    def post(self) -> Response:
        """Endpoint (private) responsible for allowing a User to create a Transaction.
        
        Returns:
            Response -- The Flask response object.
        """
        args = CREATE_PARSER.parse_args()
        user = User.get_by_email(get_jwt_identity())
        if user is None:
            return abort(403, "You are not permitted to create a transaction.")
        asset = Asset.get_by_id(args['asset_id'])
        if asset is None:
            return abort(400, "Invalid {asset_id} has been specified.")
        try:
            asset_quantity = float(args['quantity'])
        except Exception:
            return abort(400, "Invalid asset {quantity} specified.")
        if asset_quantity <= 0:
            return abort(400, "The {quantity} cannot be less than or equal to 0.")
        try:
            date_purchased = parser.parse(args['date_purchased'])
            date_purchased = date_purchased.replace(tzinfo=None)
        except Exception:
            return abort(400, "Invalid {date_purchased} specified.")
        if date_purchased > datetime.utcnow():
            return abort(400, "The {date_purchased} cannot be ahead of time.")
        purchase_candle = asset.get_daily_candle(date_purchased)
        if purchase_candle is None:
            return abort(400, "The given {date_purchased} is prior to the platform's pricing history for the asset.")
        if args['price_purchased'] is None:
            price_purchased = purchase_candle.get_close()
        else:
            try:
                price_purchased = float(args['price_purchased'])
            except Exception:
                return abort(400, "Invalid {price_purchased} given.")
            if price_purchased <= 0:
                return abort(400, "The {price_purchased} cannot be less than or equal to 0.")
        if args['date_sold'] is None:
            date_sold = None
            price_sold = None
        else:
            try:
                date_sold = parser.parse(args['date_sold'])
                date_sold = date_sold.replace(tzinfo=None)
            except Exception:
                return abort(400, "Invalid {date_sold} specified.")
            if date_sold > datetime.utcnow():
                return abort(400, "The {date_sold} cannot be ahead of time.")
            if date_sold <= date_purchased:
                return abort(400, "The {date_sold} must be further in time than the {date_purchased}.")
            if args['price_sold'] is None:
                if date_sold.date() == datetime.utcnow().date():
                    price_sold = asset.get_price()
                else:
                    sell_candle = asset.get_daily_candle(date_sold)
                    if sell_candle is None:
                        return abort(400, "The given {date_sold} has no data for the asset in the platform's pricing history.")
                    price_sold = sell_candle.get_close()
            else:
                if date_sold.date() != datetime.utcnow().date():
                    sell_candle = asset.get_daily_candle(date_sold)
                    if sell_candle is None:
                        return abort(400, "The given {date_sold} has no data for the asset in the platform's pricing history.")
                try:
                    price_sold = float(args['price_sold'])
                except Exception:
                    return abort(400, "Invalid {price_sold} given.")
                if price_sold < 0:
                    return abort(400, "The {price_sold} cannot be less than 0.")
        new_transaction = Transaction.create(user, asset, asset_quantity, date_purchased, date_sold, price_purchased, price_sold)
        if new_transaction is None:
            return abort(500, "An error occurred in creating the transaction.")
        return make_response(jsonify({"msg": "The transaction has been successfully created."}), 200)

DELETE_PARSER = API.parser()
DELETE_PARSER.add_argument("transaction_id", type=str, required=True, help="The ID of the transaction", location="json")

@API.route('/delete')
class DeleteTransaction(Resource):

    @jwt_required
    @API.expect(DELETE_PARSER)
    def delete(self) -> Response:
        """Endpoint (private) responsible for allowing a User to delete a Transaction.
        
        Returns:
            Response -- The Flask response object.
        """
        args = DELETE_PARSER.parse_args()
        transaction = Transaction.get_by_id(args['transaction_id'])
        if transaction is None:
            return abort(400, "Invalid {transaction_id} specified.")
        if transaction.get_user().get_email() != get_jwt_identity():
            return abort(401, "You are not authorised to delete this transaction.")
        if Transaction.remove(transaction):
            return make_response(jsonify({"msg": "The transaction was successfully deleted."}), 200)
        return abort(400, "An error occurred in deleting the transaction.")

READ_PARSER = API.parser()
READ_PARSER.add_argument("transaction_id", type=str, required=True, help="The ID of the transaction", location="args")

@API.route('/read')
class ReadTransaction(Resource):

    @jwt_required
    @API.expect(READ_PARSER)
    def get(self) -> Response:
        """Endpoint (private) responsible for allowing a User to read a transaction's details.
        
        Returns:
            Response -- The Flask response object.
        """
        args = READ_PARSER.parse_args()
        transaction = Transaction.get_by_id(args['transaction_id'])
        if transaction is None:
            return abort(400, "Invalid {transaction_id} specified.")
        if transaction.get_user().get_email() != get_jwt_identity():
            return abort(401, "You are not authorised to read this transaction.")
        return make_response(jsonify(transaction.as_dict()), 200)

UPDATE_PARSER = API.parser()
UPDATE_PARSER.add_argument("transaction_id", type=str, required=True, help="The ID of the transaction", location="json")
UPDATE_PARSER.add_argument("asset_id", type=str, required=False, help="The unique identifier for the asset", location="json")
UPDATE_PARSER.add_argument("quantity", type=float, required=False, help="The quantity of the asset", location="json")
UPDATE_PARSER.add_argument("date_purchased", type=str, required=False, help="The date the asset was purchased", location="json")
UPDATE_PARSER.add_argument("date_sold", type=str, required=False, help="The date the asset was sold", location="json")
UPDATE_PARSER.add_argument("price_purchased", type=float, required=False, help="The price the asset was purchased at", location="json")
UPDATE_PARSER.add_argument("price_sold", type=float, required=False, help="The price the asset was sold at", location="json")

@API.route('/update')
class UpdateTransaction(Resource):

    @jwt_required
    @API.expect(UPDATE_PARSER)
    def patch(self) -> Response:
        """Endpoint (private) responsible for updating the details of a User's Transaction.
        
        Returns:
            Response -- The Flask response object.
        """
        args = UPDATE_PARSER.parse_args()
        transaction = Transaction.get_by_id(args['transaction_id'])
        if transaction is None:
            return abort(400, "Invalid {transaction_id} specified.")
        if transaction.get_user().get_email() != get_jwt_identity():
            return abort(401, "You are not authorised to update this transaction.")
        if args['asset_id'] is not None:
            asset = Asset.get_by_id(args['asset_id'])
            if asset is None:
                return abort(400, "Invalid {asset_id} specified.")
            transaction.set_asset(asset)
        if args['quantity'] is not None:
            try:
                quantity = float(args['quantity'])
            except Exception:
                return abort(400, "Invalid {quantity} specified.")
            if quantity <= 0:
                return abort(400, "The {quantity} cannot be less than or equal to 0.")
            transaction.set_quantity(quantity)
        if args['date_purchased'] is not None:
            try:
                date_purchased = parser.parse(args['date_purchased'])
                date_purchased = date_purchased.replace(tzinfo=None)
            except Exception:
                return abort(400, "Invalid {date_purchased} specified.")
            if date_purchased > datetime.utcnow():
                return abort(400, "The {date_purchased} cannot be ahead of time.")
            purchase_candle = transaction.get_asset().get_daily_candle(date_purchased)
            if purchase_candle is None:
                return abort(400, "The given {date_purchased} is prior to the platform's pricing history for the asset.")
            transaction.set_buy_date(date_purchased)
        if args['price_purchased'] is not None:
            try:
                price_purchased = float(args['price_purchased'])
            except Exception:
                return abort(400, "Invalid {price_purchased} specified.")
            if price_purchased <= 0:
                return abort(400, "The {price_purchased} cannot be less than or equal to 0.")
            transaction.set_buy_price(price_purchased)
        else:
            if args['date_purchased'] is not None:
                transaction.set_buy_price(purchase_candle.get_close())
        if args['date_sold'] is not None:
            try:
                date_sold = parser.parse(args['date_sold'])
                date_sold = date_sold.replace(tzinfo=None)
            except Exception:
                return abort(400, "Invalid {date_sold} specified.")
            if date_sold > datetime.utcnow():
                return abort(400, "The {date_sold} cannot be ahead of time.")
            if date_sold <= date_purchased:
                return abort(400, "The {date_sold} must be further ahead in time than the {date_purchased}.")
            transaction.set_sell_date(date_sold)
        if args['price_sold'] is not None:
            try:
                price_sold = float(args['price_sold'])
            except Exception:
                return abort(400, "Invalid {price_sold} specified.")
            if price_sold <= 0:
                return abort(400, "The {price_sold} cannot be less than or equal to 0.")
            transaction.set_sell_price(price_sold)
        else:
            if args['date_sold'] is not None:
                if date_sold.date() == datetime.utcnow().date():
                    transaction.set_sell_price(transaction.get_asset().get_price())
                else:
                    sell_candle = transaction.get_asset().get_daily_candle(date_sold)
                    if sell_candle is None:
                        return abort(400, "The given {date_sold} has no data for the asset in the platform's pricing history.")
                    transaction.set_sell_price(sell_candle.get_close())
        transaction.save()
        user = transaction.user.fetch()
        user.set_portfolio_historical(None)
        user.save()
        return make_response(jsonify({"msg": "The transaction was successfully updated."}), 200)
