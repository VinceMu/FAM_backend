from flask import make_response, jsonify
from flask_restplus import Namespace, Resource
from flask_jwt_extended import jwt_required
from models import Asset
from bson import ObjectId
import json

api = Namespace('assets', description='assets endpoint')

AUTOCOMPLETE_PARSER = api.parser()
AUTOCOMPLETE_PARSER.add_argument('asset_name', type=str, required=True, help='The start of the name of the asset', location='args')

@api.route('/autocomplete')
class AutocompleteAsset(Resource):
    @api.expect(AUTOCOMPLETE_PARSER)
    def get(self):
        args = AUTOCOMPLETE_PARSER.parse_args()
        assets = Asset.objects(name__istartswith=args['asset_name']).all().to_json()
        return make_response(jsonify(assets), 201)

PERFORMANCE_DAILY_PARSER = api.parser()
PERFORMANCE_DAILY_PARSER.add_argument('asset_id', type=str, required=True, help='The ID of the asset', location='args')

def get_performance_fields(self, asset):
    candle = asset.get_candles_interval_sorted(86400).first()
    fields = {}
    fields['last_daily'] = candle.serialize_price()
    fields['curr_daily'] = asset.serialize_price()
    fields['last_performance_percent'] = round((candle.close - candle.open)/candle.open*100,2)
    fields['curr_performance_percent'] = round((asset.price - candle.close)/candle.close*100,2)
    return fields

@api.route('/performance/daily')
class PerformanceDaily(Resource):
    @jwt_required
    @api.expect(PERFORMANCE_DAILY_PARSER)
    def get(self):
        args = PERFORMANCE_DAILY_PARSER.parse_args()
        if ObjectId.is_valid(args['asset_id']) == False:
            return abort(400, "invalid asset id")
        asset = Asset.objects(id=args['asset_id']).first()
        if asset == None:
            return abort(400, "invalid asset")
        return make_response(asset.serialize(), 200)

@api.route('/read')
class ReadAssets(Resource):
    def get(self):
        results = []
        for asset in Asset.objects:
            results.append(asset.serialize())
        return make_response(jsonify(results), 201)
