from flask_restplus import Namespace, Resource, fields, abort
from controllers import authentication_controller
from flask import jsonify, request, make_response
from flask_jwt_extended import jwt_refresh_token_required,get_jwt_identity,get_raw_jwt, jwt_required
from globals import mongo_user, jwt
import hashlib, uuid, secrets

api = Namespace('auth', description='authenticate with flask')

revoked_tokens = set()


@jwt.token_in_blacklist_loader
def check_if_token_in_blacklist(decrypted_token):
    jti = decrypted_token['jti']
    return jti in revoked_tokens


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

@api.route('/refresh')
class Refresh(Resource):

    @jwt_refresh_token_required
    def post(self):
        curr_user = get_jwt_identity()
        new_access_token = authentication_controller.generate_access_token(curr_user)
        return make_response(jsonify(new_access_token), 200)


@api.route('/logout')
class Logout(Resource):

    @jwt_required
    def delete(self):
        jti = get_raw_jwt()['jti']
        revoked_tokens.add(jti)
        return make_response(jsonify({"msg": "Successfully logged out"}), 200)

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

        # Create user and attempt to add to the database
        user_register = {}
        user_register['email'] = args['email']
        user_register['fullname'] = args['fullname']
        user_register['password'] = hash_password
        user_register['salt'] = salt
        if (mongo_user.db.auth.find_one({'email' : user_register['email']})):
            # Found one, don't create new
            # Return with Error Code 409 - Conflict. No duplicate users
            return abort(409, "account with that email already exists")
        else:
            try:
                mongo_user.db.auth.insert(dict(user_register))
                # Return tokens and user id along with the success code 201
                tokens = authentication_controller.generate_tokens(args["email"])
                return make_response(jsonify(tokens), 201)
            except:
                return None, 500

