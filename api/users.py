from flask import make_response, jsonify
from flask_restplus import Namespace, Resource, fields, abort
from flask_jwt_extended import get_jwt_identity, jwt_required
from bson import ObjectId
import dateutil.parser
from models import *

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