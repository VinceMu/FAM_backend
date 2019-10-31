from flask_restplus import Namespace, Resource, fields, abort
from flask_jwt_extended import get_jwt_identity, jwt_required
from datetime import datetime
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
                    date_purchased = datetime.strptime(datetime_str, '%m/%d/%y %H:%M:%S')
                except:
                    abort(400, "invalid date_purchased")
                new_asset_ownership = AssetOwnership(asset=asset,quantity=asset_quantity, date_purchased=date_purchased)
                user.assets.append(new_asset_ownership)
                user.save()
