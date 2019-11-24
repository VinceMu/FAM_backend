from flask import jsonify, make_response, Response
from flask_jwt_extended import get_jwt_identity, get_raw_jwt, jwt_refresh_token_required, jwt_required
from flask_restplus import abort, Namespace, Resource
from globals import JWT

from local_config import REST_LOGGER
from models.auth import Auth, AuthRevokedToken
from models.user import User

API = Namespace('auth', description='authentication endpoint')

@JWT.token_in_blacklist_loader
def check_if_token_in_blacklist(decrypted_token: str) -> bool:
    """Method provided to JWT for checking whether a token is on the blacklist.
    
    Arguments:
        decrypted_token {str} -- The token to be checked on the blacklist.
    
    Returns:
        bool -- Returns True if the token is on the blacklist; False otherwise.
    """
    jti = decrypted_token['jti']
    return AuthRevokedToken.has_token(jti)

LOGIN_PARSER = API.parser()
LOGIN_PARSER.add_argument('email', type=str, required=True, help='The users email', location='json')
LOGIN_PARSER.add_argument('password', type=str, required=True, help='The users password', location='json')

@API.route('/login')
class Login(Resource):

    @API.expect(LOGIN_PARSER)
    def post(self) -> Response:
        """Endpoint (public) is responsible for authenticating an end user.
        
        Returns:
            Response -- The Flask response object.
        """
        args = LOGIN_PARSER.parse_args()
        if Auth.authenticate(args['email'], args['password']) is not None:
            REST_LOGGER.info("auth/login -> Authenticated login for user %s", args['email'])
            tokens = Auth.generate_tokens(args['email'])
            return make_response(jsonify(tokens), 200)
        REST_LOGGER.info("auth/login -> Denied login for user %s", args['email'])
        return abort(401, "Invalid {email} or {password} given.")

@API.route('/logout')
class Logout(Resource):

    @jwt_required
    def delete(self) -> Response:
        """Endpoint (private) is responsible for partially logging out a user - 
        disabling access token only.
        
        Returns:
            Response -- The Flask response object.
        """
        AuthRevokedToken.create(get_raw_jwt()['jti'])
        REST_LOGGER.info("auth/logout -> Logout (stage 1) for user %s", get_jwt_identity())
        return make_response(jsonify({"msg": "Successfully logged out."}), 200)

@API.route('/logout_refresh')
class LogoutRefresh(Resource):

    @jwt_refresh_token_required
    def delete(self) -> Response:
        """Endpoint (private) is responsible for finishing the logout - blacklisting
        the refresh token also.
        
        Returns:
            Response -- The Flask response object.
        """
        AuthRevokedToken.create(get_raw_jwt()['jti'])
        REST_LOGGER.info("auth/logout_refresh -> Logout (stage 2) for user %s", get_jwt_identity())
        return make_response(jsonify({"msg": "Successfully logged out."}), 200)

@API.route('/refresh')
class Refresh(Resource):

    @jwt_refresh_token_required
    def post(self) -> Response:
        """Endpoint (private) is responsible for generating the user new access tokens
        from their refresh tokens.
        
        Returns:
            Response -- The Flask response object.
        """
        REST_LOGGER.info("auth/refresh -> Refresh access token for user %s", get_jwt_identity())
        return make_response(jsonify(Auth.generate_access_token(get_jwt_identity())), 200)

REGISTER_PARSER = API.parser()
REGISTER_PARSER.add_argument('email', type=str, required=True, help='The users email', location='json')
REGISTER_PARSER.add_argument('password', type=str, required=True, help='The users password', location='json')
REGISTER_PARSER.add_argument('fullname', type=str, required=True, help='The users full name', location='json')

@API.route('/register')
class Register(Resource):

    @API.expect(REGISTER_PARSER)
    def post(self) -> Response:
        """Endpoint (public) for registering a user account on the platform.
        
        Returns:
            Response -- The Flask response object.
        """
        args = REGISTER_PARSER.parse_args()
        if args['email'] == "":
            return abort(400, "The {email} field cannot be empty.")
        if args['fullname'] == "":
            return abort(400, "The {fullname} field cannot be empty.")
        if "@" not in args['email']:
            return abort(400, "The {email} specified is invalid.")
        if len(args['password']) < 6:
            return abort(400, "The {password} given must be >= 6 characters.")
        check_auth = Auth.get_by_email(args['email'])
        if check_auth is not None:
            REST_LOGGER.info("auth/register -> Duplicate registration attempt for email %s", args['email'])
            return abort(409, "A user already exists with that {email}.")
        user_auth = Auth.create(args['email'], args['password'])
        if user_auth is None:
            REST_LOGGER.info("auth/register -> Fail on Auth.create() with email %s", args['email'])
            return abort(401, "Failed to create an account with the given {email}.")
        user = User.create(args['email'], args['fullname'])
        if user is None:
            REST_LOGGER.error("auth/register -> Fail on User.create() with email %s", args['email'])
            return abort(401, "Failed to create an account with the given {email}.")
        REST_LOGGER.info("auth/register -> User registered with email %s", args['email'])
        return make_response(jsonify(Auth.generate_tokens(args['email'])))

UPDATE_PARSER = API.parser()
UPDATE_PARSER.add_argument("old_password", type=str, required=True, help="The user's old password", location="json")
UPDATE_PARSER.add_argument("new_password", type=str, required=True, help="The user's new password", location="json")

@API.route("/update")
class UpdateAuth(Resource):
    
    @jwt_required
    @API.expect(UPDATE_PARSER)
    def put(self) -> Response:
        """Endpoint (private) responsible for updating a user's password.
        
        Returns:
            Response -- The Flask response object.
        """
        args = UPDATE_PARSER.parse_args()
        if len(args['new_password']) < 6:
            return abort(400, "Your {new_password} must be of length >= 6 characters.")
        auth = Auth.authenticate(get_jwt_identity(), args['old_password'])
        if auth is None:
            return abort(401, "Invalid credentials supplied.")
        auth.update_password(args['new_password'])
        auth.save()
        return make_response(jsonify({"msg": "The user password has been successfully updated."}), 200)
