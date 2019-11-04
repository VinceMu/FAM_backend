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