from flask import make_response, jsonify
from flask_restplus import Namespace, Resource
from models import Asset

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

@api.route('/read')
class ReadAssets(Resource):
    def get(self):
        return make_response(jsonify(Asset.objects.all().to_json()), 201)
