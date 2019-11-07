from flask import make_response, jsonify
from flask_restplus import Namespace, Resource, fields, abort
from flask_jwt_extended import get_jwt_identity, jwt_required
from bson import ObjectId
import dateutil.parser
from models import *
from bson import ObjectId
from werkzeug.datastructures import FileStorage

api = Namespace('users', description='users endpoint')

@api.route('/assets')
class UserAssets(Resource):
    @jwt_required
    def get(self):
        user = User.objects(email=get_jwt_identity()).first()
        if user == None:
            return abort(401, "forbidden")
        else:
            owned_assets = []
            for asset_ownership in user.assets:
                owned_assets.append(asset_ownership.serialize())
            return make_response(jsonify(owned_assets), 200)

@api.route('/portfolio/current')
class UserPortfolioCurrent(Resource):
    @jwt_required
    def get(self):
        user = User.objects(email=get_jwt_identity()).first()
        if user == None:
            return abort(401, "forbidden")
        value = 0
        spent_value = 0
        for transaction in user.assets:
            date_purchased = transaction.date_purchased.date()
            start_candle = transaction.asset.get_daily_candle(86400, date_purchased)
            spent_value -= (transaction.quantity * start_candle.close)
            if transaction.date_sold != None:
                date_sold = transaction.date_sold.date()
                end_candle = transaction.asset.get_daily_candle(86400, date_sold)
                spent_value += (transaction.quantity * end_candle.close)
            else:
                value += (transaction.asset.price * transaction.quantity)
        result = {
            "purchase_value": spent_value,
            "net_value": value + spent_value,
            "value": value
        }
        return make_response(jsonify(result), 200)

@api.route('/portfolio/historical')
class UserPortfolioHistorical(Resource):
    @jwt_required
    def get(self):
        user = User.objects(email=get_jwt_identity()).first()
        if user == None:
            return abort(401, "forbidden")
        value = {}
        spent_value = {}
        earliest_date = None
        loop_end_date = datetime.datetime.utcnow().date()
        for transaction in user.assets:
            date_purchased = transaction.date_purchased.date() if (transaction.date_purchased != None) else None
            if earliest_date == None or date_purchased < earliest_date:
                earliest_date = date_purchased
            date_sold = (transaction.date_sold.date()+datetime.timedelta(days=1)) if (transaction.date_sold != None) else None
            candles = transaction.asset.get_daily_candles(86400, date_purchased, date_sold)
            start_price = candles[len(candles)-1].close
            if len(candles) == 1:
                end_price = start_price
            else:
                end_price = candles[0].close
            for candle in candles:
                tag = str(candle.close_time.date())
                if tag in value:
                    value[tag] = value[tag] + (candle.close * transaction.quantity)
                    spent_value[tag] = spent_value[tag] - (transaction.quantity * start_price)
                else:
                    value[tag] = (candle.close * transaction.quantity)
                    spent_value[tag] = -(transaction.quantity * start_price)
            if date_sold != None:
                loop_start_date = date_sold
                while loop_start_date < loop_end_date:
                    other_tag = str(loop_start_date)
                    if other_tag in spent_value:
                        spent_value[other_tag] = spent_value[other_tag] + (transaction.quantity * (end_price - start_price))
                    else:
                        spent_value[other_tag] = (transaction.quantity * (end_price - start_price))
                    loop_start_date = loop_start_date + datetime.timedelta(days=1)
        if len(value) == 0 and len(spent_value) == 0:
            return make_response(jsonify({}))
        result = {}
        # Check all date values have been filled and combined
        while earliest_date < loop_end_date:
            mytag = str(earliest_date)
            if mytag not in value:
                value[mytag] = 0
            if mytag not in spent_value:
                spent_value[mytag] = 0
            result[mytag] = {
                "purchase_value": spent_value[mytag],
                "net_value": value[mytag] + spent_value[mytag],
                "value": value[mytag]
            }
            earliest_date = earliest_date + datetime.timedelta(days=1)
        return make_response(jsonify(result), 200)
        

update_parser = api.parser()
update_parser.add_argument("fullname", type=str, required=False, help="The full name of the user", location="json")
update_parser.add_argument("currency_id", type=str, required=False, help="The ID of the base currency to be used", location="json")

@api.route('/update')
class UpdateUser(Resource):
    @jwt_required
    @api.expect(update_parser)
    def patch(self):
        args = update_parser.parse_args()
        user = User.objects(email=get_jwt_identity()).first()
        if user is None:
            return abort(401, "forbidden")
        if args['fullname'] is not None:
            user.fullname = args['fullname']
        if args['currency_id'] is not None:
            if not ObjectId.is_valid(args['currency_id']):
                return abort(400, "invalid currency id")
            currency = Currency.objects(pk=args['currency_id']).first()
            if currency is None:
                return abort(400, "currency not found")
            user.base_currency = currency
        user.save()
        return make_response("Success", 200)

upload_parser = api.parser()
upload_parser.add_argument("profile_picture", type=FileStorage, required=True, help='A profile picture for the user', location='files')

@api.route('/upload')
class UploadUser(Resource):
    @jwt_required
    @api.expect(upload_parser)
    def post(self):
        args = upload_parser.parse_args()
        user = User.objects(email=get_jwt_identity()).first()
        if user == None:
            return abort(401, 'forbidden')
        user.picture.replace(args['profile_picture'])
        user.save()
        return make_response("Success", 200)