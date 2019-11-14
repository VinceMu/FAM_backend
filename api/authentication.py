from flask_restplus import Namespace, Resource, fields, abort
from controllers import authentication_controller
from flask import jsonify, request, make_response
from flask_jwt_extended import jwt_refresh_token_required,get_jwt_identity,get_raw_jwt, jwt_required
from globals import jwt
from models import *
import hashlib, uuid, secrets

api = Namespace('auth', description='authentication endpoint')

@jwt.token_in_blacklist_loader
def check_if_token_in_blacklist(decrypted_token):
    jti = decrypted_token['jti']
    return AuthRevokedToken.has_token(jti)

login_parser = api.parser()
login_parser.add_argument('email', type=str, required=True, help='The users email', location='json')
login_parser.add_argument('password', type=str, required=True, help='The users password', location='json')


@api.route('/login')
class Login(Resource):

    @api.expect(login_parser)
    def post(self):
        args = login_parser.parse_args()
        is_authenticated, user = authentication_controller.authenticate_user(args["email"],
                                                                          args["password"])
        if is_authenticated:
            tokens = authentication_controller.generate_tokens(args["email"])
            return make_response(jsonify(tokens), 200)
        else:
            return abort(401,"login failed")

@api.route('/logout')
class Logout(Resource):

    @jwt_required
    def delete(self):
        jti = get_raw_jwt()['jti']
        new_revoked = AuthRevokedToken(jti=jti)
        new_revoked.save()
        return make_response(jsonify({"msg": "Successfully logged out"}), 200)

@api.route('/logout_refresh')
class LogoutRefresh(Resource):

    @jwt_refresh_token_required
    def delete(self):
        jti = get_raw_jwt()['jti']
        new_revoked = AuthRevokedToken(jti=jti)
        new_revoked.save()
        return jsonify({"msg": "Successfully logged out"}, 200)

@api.route('/refresh')
class Refresh(Resource):

    @jwt_refresh_token_required
    def post(self):
        curr_user = get_jwt_identity()
        new_access_token = authentication_controller.generate_access_token(curr_user)
        return make_response(jsonify(new_access_token), 200)

register_parser = api.parser()
register_parser.add_argument('email', type=str, required=True, help='The users email', location='json')
register_parser.add_argument('password', type=str, required=True, help='The users password', location='json')
register_parser.add_argument('fullname', type=str, required=True, help='The users full name', location='json')

@api.route('/register')
class Register(Resource):

    @api.expect(register_parser)
    def post(self):
        args = register_parser.parse_args()
        #  Hash and Salt password
        salt = secrets.token_hex(8)
        salted_password = str(args['password'] + salt).encode('utf8')
        hash_password = hashlib.sha256(salted_password).hexdigest()

        if Auth.objects(email=args['email']):
            # Found one, don't create new
            # Return with Error Code 409 - Conflict. No duplicate users
            return abort(409, "account with that email already exists")
        else:
            try:
                new_auth = Auth(email=args['email'], password=hash_password, salt=salt)
                new_auth.save()
                new_user = User(email=args['email'], fullname=args['fullname'])
                new_user.save()
                tokens = authentication_controller.generate_tokens(args["email"])
                return make_response(jsonify(tokens), 201)
            except:
                return None, 500

update_parser = api.parser()
update_parser.add_argument("old_password", type=str, required=True, help="The user's old password", location="json")
update_parser.add_argument("new_password", type=str, required=True, help="The user's new password", location="json")

@api.route("/update")
class UpdateAuth(Resource):
    @api.expect(update_parser)
    @jwt_required
    def put(self):
        args = update_parser.parse_args()
        is_authenticated, auth = authentication_controller.authenticate_user(get_jwt_identity(), args["old_password"])
        if auth == None or is_authenticated == False:
            return abort(401, "forbidden")
        auth.salt = secrets.token_hex(8)
        salted_pass = str(args['new_password'] + auth.salt).encode("utf8")
        auth.password = hashlib.sha256(salted_pass).hexdigest()
        auth.save()
        return make_response("Success", 200)

