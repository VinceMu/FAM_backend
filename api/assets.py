from dateutil import parser
from flask import jsonify, make_response, Response
from flask_restplus import abort, Namespace, Resource
from flask_jwt_extended import jwt_required

from models.asset import Asset

API = Namespace('assets', description='assets endpoint')

AUTOCOMPLETE_PARSER = API.parser()
AUTOCOMPLETE_PARSER.add_argument('asset_name', type=str, required=True, help='The start of the name of the asset', location='args')

@API.route('/autocomplete')
class AutocompleteAsset(Resource):

    @API.expect(AUTOCOMPLETE_PARSER)
    def get(self) -> Response:
        """Endpoint (public) provides the details of all assets containing the given asset_name.
        
        Returns:
            Response -- The flask Response object.
        """
        args = AUTOCOMPLETE_PARSER.parse_args()
        assets = Asset.autocomplete_by_name(args['asset_name'])
        assets_dict = [asset.as_dict_autocomplete() for asset in assets]
        return make_response(jsonify(assets_dict), 200)

HISTORICAL_DAILY_PARSER = API.parser()
HISTORICAL_DAILY_PARSER.add_argument('asset_id', type=str, required=True, help='The ID of the asset', location='args')
HISTORICAL_DAILY_PARSER.add_argument('start_date', type=str, required=False, help='The start date of the request', location='args')
HISTORICAL_DAILY_PARSER.add_argument('end_date', type=str, required=False, help='The end date of the request', location='args')

@API.route("/historical/daily")
class HistoricalDaily(Resource):

    @jwt_required
    @API.expect(HISTORICAL_DAILY_PARSER)
    def get(self) -> Response:
        """Endpoint (private) provides the historical candle data for a given asset.
        
        Returns:
            Response -- The flask Response object.
        """
        args = HISTORICAL_DAILY_PARSER.parse_args()
        asset = Asset.get_by_id(args['asset_id'])
        if asset is None:
            return abort(400, "Invalid {asset_id} given.")
        if args['start_date'] is None:
            start_date = None
        else:
            try:
                start_date = parser.parse(args['start_date'])
            except Exception:
                abort(400, "Invalid {start_date} given.")
        if args['end_date'] is None:
            end_date = None
        else:
            try:
                end_date = parser.parse(args['end_date'])
            except Exception:
                abort(400, "Invalid {end_date} given.")
        candles = asset.get_candles_within(start=start_date, finish=end_date)
        candles_dict = [candle.as_dict() for candle in candles]
        return make_response(jsonify(candles_dict), 200)

@API.route('/list')
class ListAssets(Resource):
    
    def get(self) -> Response:
        """Endpoint (public) provides a list of all the Assets and associated information.
        
        Returns:
            Response -- The flask Response object.
        """
        assets = Asset.get()
        assets_dict = [asset.as_dict() for asset in assets]
        return make_response(jsonify(assets_dict), 200)

PERFORMANCE_DAILY_PARSER = API.parser()
PERFORMANCE_DAILY_PARSER.add_argument('asset_id', type=str, required=True, help='The ID of the asset', location='args')

@API.route('/performance/daily')
class PerformanceDaily(Resource):

    @jwt_required
    @API.expect(PERFORMANCE_DAILY_PARSER)
    def get(self) -> Response:
        """Endpoint (private) provides the performance of a specified asset over the current and previous day.
        
        Returns:
            Response -- The flask Response object.
        """
        args = PERFORMANCE_DAILY_PARSER.parse_args()
        asset = Asset.get_by_id(args['asset_id'])
        if asset is None:
            return abort(400, "Invalid {asset_id} given.")
        return make_response(jsonify(asset.get_daily_performance()), 200)

READ_ASSET_PARSER = API.parser()
READ_ASSET_PARSER.add_argument('asset_id', type=str, required=True, help='The ID of the asset', location='args')

@API.route('/read')
class ReadAsset(Resource):

    @jwt_required
    @API.expect(READ_ASSET_PARSER)
    def get(self) -> Response:
        """Endpoint (private) provides the data associated with an Asset.
        
        Returns:
            Response -- The flask Response object.
        """
        args = READ_ASSET_PARSER.parse_args()
        asset = Asset.get_by_id(args['asset_id'])
        if asset is None:
            return abort(400, "Invalid {asset_id} given.")
        asset_dict = asset.as_dict()
        return make_response(jsonify(asset_dict), 200)
