from flask import make_response, jsonify
from flask_restplus import Namespace, Resource, fields, abort
from flask_jwt_extended import get_jwt_identity, jwt_required
import dateutil.parser
from models import *

api = Namespace('userassets', description='user asset management')

add_asset_parser = api.parser()
add_asset_parser.add_argument('asset_id', type=str, required=True, help='The unique identifier for the asset', location='json')
add_asset_parser.add_argument('quantity', type=float, required=True, help='The quantity of the asset', location='json')
add_asset_parser.add_argument('date_purchased', type=str, required=True, help='The date the asset was purchased', location='json')

@api.route('/add_asset')
class AddAsset(Resource):
    @jwt_required
    @api.expect(add_asset_parser)
    def post(self):
        args = add_asset_parser.parse_args()
        user = Auth.objects(email=get_jwt_identity()).first()
        if user == None:
            abort(403, "forbidden")
        else:
            asset = Asset.objects(id=args['asset_id']).first()
            if asset == None:
                abort(400, "invalid asset id")
            else:
                try:
                    asset_quantity = float(args['quantity'])
                except:
                    abort(400, "invalid quantity")
                try:
                    date_purchased = dateutil.parser.parse(args['date_purchased'])
                except:
                    abort(400, "invalid date")
                new_asset_ownership = AssetOwnership(asset=asset, quantity=asset_quantity, date_purchased=date_purchased)
                new_asset_ownership.save()
                user.assets.append(new_asset_ownership)
                user.save()
                return make_response("Success", 200)

asset_autocomplete_parser = api.parser()
asset_autocomplete_parser.add_argument('asset_name', type=str, required=True, help='The start of the name of the asset', location='json')

@api.route('/asset_autocomplete')
class AssetAutocomplete(Resource):
    @api.expect(asset_autocomplete_parser)
    def post(self):
        args = asset_autocomplete_parser.parse_args()
        assets = Asset.objects(name__istartswith=args['asset_name']).all().to_json()
        return make_response(jsonify(assets), 201)

@api.route('/get_assets')
class GetAssets(Resource):
    @jwt_required
    def post(self):
        user = Auth.objects(email=get_jwt_identity()).first()
        if user == None:
            abort(403, "forbidden")
        else:
            owned_assets = []
            for asset_ownership in user.assets:
                data = {}
                data['id'] = str(asset_ownership.pk)
                data['asset_name'] = asset_ownership.asset.name
                data['asset_ticker'] = asset_ownership.asset.ticker
                data['asset_price'] = asset_ownership.asset.price
                data['quantity'] = asset_ownership.quantity
                data['date_purchased'] = asset_ownership.date_purchased
                owned_assets.append(data)
            return make_response(jsonify(owned_assets), 200)

@api.route('/get_available_assets')
class GetAvailableAssets(Resource):
    def post(self):
        return make_response(jsonify(Asset.objects.all().to_json()), 201)