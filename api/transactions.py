from flask import make_response, jsonify
from flask_restplus import Namespace, Resource, fields, abort
from flask_jwt_extended import get_jwt_identity, jwt_required
from bson import ObjectId
import dateutil.parser
from models import *

api = Namespace('transactions', description='transactions endpoint')

create_parser = api.parser()
create_parser.add_argument('asset_id', type=str, required=True, help='The unique identifier for the asset', location='json')
create_parser.add_argument('quantity', type=float, required=True, help='The quantity of the asset', location='json')
create_parser.add_argument('date_purchased', type=str, required=True, help='The date the asset was purchased', location='json')
create_parser.add_argument('date_sold', type=str, required=False, help='The date the asset was sold (empty if not sold)', location='json')

@api.route('/create')
class CreateTransaction(Resource):
    @jwt_required
    @api.expect(create_parser)
    def post(self):
        args = create_parser.parse_args()
        user = User.objects(email=get_jwt_identity()).first()
        if user == None:
            return abort(403, "forbidden")
        else:
            if ObjectId.is_valid(args['asset_id']) == False:
                return abort(400, "invalid asset id")
            asset = Asset.objects(id=args['asset_id']).first()
            if asset == None:
                return abort(400, "invalid asset id")
            else:
                try:
                    asset_quantity = float(args['quantity'])
                except:
                    return abort(400, "invalid quantity")
                try:
                    date_purchased = dateutil.parser.parse(args['date_purchased'])
                except:
                    return abort(400, "invalid date purchased")
                if args['date_sold'] != None:
                    try:
                        date_sold = dateutil.parser.parse(args['date_sold'])
                    except:
                        return abort(400, "invalid date sold")
                else:
                    date_sold = None
                new_asset_ownership = AssetOwnership(user=user, asset=asset, quantity=asset_quantity, date_purchased=date_purchased, date_sold=date_sold)
                new_asset_ownership.save()
                user.assets.append(new_asset_ownership)
                user.save()
                return make_response("Success", 200)

delete_parser = api.parser()
delete_parser.add_argument("transaction_id", type=str, required=True, help="The ID of the transaction", location="json")

@api.route("/delete")
class DeleteTransaction(Resource):
    @jwt_required
    @api.expect(delete_parser)
    def delete(self):
        args = delete_parser.parse_args()
        if ObjectId.is_valid(args['transaction_id']) == False:
            return abort(400, "invalid transaction id")
        transaction = AssetOwnership.objects(pk=args['transaction_id']).first()
        if transaction is None:
            return abort(400, "transaction not found")
        user = transaction.user.fetch()
        if user == None or user.email != get_jwt_identity():
            return abort(401, "forbidden")
        user.assets.remove(transaction)
        user.save()
        transaction.delete()
        return make_response("Success", 200)

read_parser = api.parser()
read_parser.add_argument("transaction_id", type=str, required=True, help="The ID of the transaction", location="args")

@api.route('/read')
class ReadTransaction(Resource):
    @jwt_required
    @api.expect(read_parser)
    def get(self):
        args = read_parser.parse_args()
        if ObjectId.is_valid(args['transaction_id']) == False:
            return abort(400, "invalid transaction id")
        # Check the transaction ID exists, abort if it doesn't
        transaction = AssetOwnership.objects(pk=args['transaction_id']).first()
        if transaction is None:
            return abort(400, "transaction not found")
        else:
            # Check that the user is authorised to fetch the details of this transaction
            user = transaction.user.fetch()
            if user == None or user.email != get_jwt_identity():
                return abort(401, "forbidden")
            else:
                return make_response(jsonify(transaction.serialize()), 200)

update_parser = api.parser()
update_parser.add_argument("transaction_id", type=str, required=True, help="The ID of the transaction", location="json")
update_parser.add_argument("asset_id", type=str, required=False, help="The unique identifier for the asset", location="json")
update_parser.add_argument("quantity", type=float, required=False, help="The quantity of the asset", location="json")
update_parser.add_argument("date_purchased", type=str, required=False, help="The date the asset was purchased", location="json")
update_parser.add_argument("date_sold", type=str, required=False, help="The date the asset was sold", location="json")

@api.route("/update")
class UpdateTransaction(Resource):
    @jwt_required
    @api.expect(update_parser)
    def patch(self):
        args = update_parser.parse_args()
        if ObjectId.is_valid(args['transaction_id']) == False:
            return abort(400, "invalid transaction id")
        transaction = AssetOwnership.objects(pk=args['transaction_id']).first()
        if transaction is None:
            return abort(400, "transaction not found")
        user = transaction.user.fetch()
        if user == None or user.email != get_jwt_identity():
            return abort(401, "forbidden")
        if args['asset_id'] != None:
            if ObjectId.is_valid(args['asset_id']) == False:
                return abort(400, "invalid asset id")
            asset = Asset.objects(pk=args['asset_id']).first()
            if asset == None:
                return abort(400, "asset not found")
            transaction.asset = asset
        if args['quantity'] != None:
            if args['quantity'] < 0:
                return abort(400, "invalid transaction amount")
            transaction.quantity = args['quantity']
        if args['date_purchased'] != None:
            try:
                date_purchased = dateutil.parser.parse(args['date_purchased'])
            except:
                return abort(400, "invalid date purchased")
            transaction.date_purchased = date_purchased
        if args['date_sold'] != None:
            try:
                date_sold = dateutil.parser.parse(args['date_sold'])
            except:
                return abort(400, "invalid date sold")
        else:
            date_sold = None
        transaction.date_sold = date_sold
        transaction.save()
        return make_response("Success", 200)