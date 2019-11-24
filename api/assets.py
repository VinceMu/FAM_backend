from datetime import datetime

from dateutil import parser
from flask import jsonify, make_response, Response
from flask_restplus import abort, Namespace, Resource
from flask_jwt_extended import jwt_required

from models.asset import Asset
from models.constants import MAX_INT, MIN_INT

API = Namespace('assets', description='assets endpoint')

AUTOCOMPLETE_PARSER = API.parser()
AUTOCOMPLETE_PARSER.add_argument('asset_name', type=str, required=True, help='The start of the name of the asset', location='args')

@API.route('/autocomplete')
class AutocompleteAsset(Resource):

    @jwt_required
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
        candles = asset.get_candles_within(start=start_date, finish=end_date, exclude_filler=True)
        candles_dict = [candle.as_dict() for candle in candles]
        return make_response(jsonify(candles_dict), 200)

HISTORICAL_INTERVAL_PARSER = API.parser()
HISTORICAL_INTERVAL_PARSER.add_argument('asset_id', type=str, required=True, help='The ID of the asset', location='args')
HISTORICAL_INTERVAL_PARSER.add_argument('start_datetime', type=str, required=False, help='The start datetime of the request', location='args')
HISTORICAL_INTERVAL_PARSER.add_argument('end_datetime', type=str, required=False, help='The end datetime of the request', location='args')
HISTORICAL_INTERVAL_PARSER.add_argument('interval', type=int, required=True, help='The length of candle interval required in seconds (i.e. 86400 for daily)', location='args')

@API.route('/historical/interval')
class HistoricalInterval(Resource):

    @jwt_required
    @API.expect(HISTORICAL_INTERVAL_PARSER)
    def get(self) -> Response:
        """Endpoint (private) provides the historical candle data for a given asset and interval.
        
        Returns:
            Response -- The Flask response object.
        """
        args = HISTORICAL_INTERVAL_PARSER.parse_args()
        asset = Asset.get_by_id(args['asset_id'])
        if asset is None:
            return abort(400, "Invalid {asset_id} given.")
        if args['start_datetime'] is None:
            start_date = None
        else:
            try:
                start_date = parser.parse(args['start_datetime'])
            except Exception:
                abort(400, "Invalid {start_datetime} given.")
        if args['end_datetime'] is None:
            end_date = None
        else:
            try:
                end_date = parser.parse(args['end_datetime'])
            except Exception:
                abort(400, "Invalid {end_datetime} given.")
        if args['interval'] <= 0 or args['interval'] < MIN_INT or args['interval'] > MAX_INT:
            abort(400, "Invalid {interval} given.")
        candles = asset.get_candles_within(start=start_date, finish=end_date, interval=args['interval'], exclude_filler=True)
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

PRICING_PARSER = API.parser()
PRICING_PARSER.add_argument('asset_id', type=str, required=True, help='The ID of the asset', location='args')
PRICING_PARSER.add_argument('date', type=str, required=False, help='The date to retrieve pricing for', location='args')

@API.route('/pricing')
class PricingAsset(Resource):

    @jwt_required
    @API.expect(PRICING_PARSER)
    def get(self) -> Response:
        """Endpoint (private) provides the price of a specified asset on a given day.

        Returns:
            Response -- The flask Response object.
        """
        args = PRICING_PARSER.parse_args()
        asset = Asset.get_by_id(args['asset_id'])
        if asset is None:
            return abort(400, "Invalid {asset_id} given.")
        if args['date'] is None:
            return make_response(jsonify({"price": asset.get_price()}), 200)
        try:
            date = parser.parse(args['date'])
        except Exception:
            abort(400, "Invalid {date} given.")
        candle = asset.get_daily_candle(date.date())
        if candle is None:
            if date.date() == datetime.utcnow().date():
                return make_response(jsonify({"price": asset.get_price()}), 200)
            return abort(400, "The specified {date} is outside the data available for the asset.")
        return make_response(jsonify({"price": candle.get_close()}), 200)

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

TRENDS_READ_PARSER = API.parser()
TRENDS_READ_PARSER.add_argument('asset_id', type=str, required=True, help='The ID of the asset', location='args')
TRENDS_READ_PARSER.add_argument('start_date', type=str, required=False, help='The start date of the request', location='args')
TRENDS_READ_PARSER.add_argument('end_date', type=str, required=False, help='The end date of the request', location='args')

@API.route('/trends/read')
class TrendsRead(Resource):

    @jwt_required
    @API.expect(TRENDS_READ_PARSER)
    def get(self) -> Response:
        """Endpoint (private) for providing the Google Trends data associated with an Asset.
        
        Returns:
            Response -- The Flask response object.
        """
        args = TRENDS_READ_PARSER.parse_args()
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
        trends = asset.get_trends(start=start_date, finish=end_date)
        trends_dict = [trend.as_dict() for trend in trends]
        return make_response(jsonify(trends_dict), 200)
