from datetime import datetime

from flask import jsonify, make_response, Response
from flask_restplus import abort, Namespace, Resource
from flask_jwt_extended import get_jwt_identity, jwt_required
from werkzeug.datastructures import FileStorage

from models.asset import Asset
from models.user import User

API = Namespace('users', description='users endpoint')

@API.route('/portfolio/current')
class UserPortfolioCurrent(Resource):

    @jwt_required
    def get(self) -> Response:
        """Endpoint (private) provides the details of the current value of the User's portfolio.
        
        Returns:
            Response -- The Flask response object.
        """
        user = User.get_by_email(get_jwt_identity())
        if user is None:
            return abort(403, "You are not permitted to access this endpoint.")
        return make_response(jsonify(user.get_portfolio_current()), 200)

@API.route('/portfolio/historical')
class UserPortfolioHistorical(Resource):

    @jwt_required
    def get(self) -> Response:
        """Endpoint (private) provides the details of the historical value of the User's portfolio.
        
        Returns:
            Response -- The Flask response object.
        """
        user = User.get_by_email(get_jwt_identity())
        if user is None:
            return abort(403, "You are not permitted to access this endpoint.")
        historical_portfolio = user.get_portfolio_historical()
        if historical_portfolio is None:
            user.update_portfolio_historical()
            user.save()
            historical_portfolio = user.get_portfolio_historical()
            if historical_portfolio is None:
                return abort(500, "An error occurred calculating the user's historical portfolio value.")
        historical_portfolio[str(datetime.now().date())] = user.get_portfolio_current()
        return make_response(jsonify(historical_portfolio), 200)

@API.route('/transactions')
class UserTransactions(Resource):

    @jwt_required
    def get(self) -> Response:
        """Endpoint (private) provides the details of all the transactions a User has made. 
        
        Returns:
            Response -- The Flask response object.
        """
        user = User.get_by_email(get_jwt_identity())
        if user is None:
            return abort(403, "You are not permitted to access this endpoint.")
        transactions = user.get_transactions()
        transactions_json = [transaction.as_dict() for transaction in transactions]
        return make_response(jsonify(transactions_json), 200)

@API.route('/read_picture')
class ReadPictureUser(Resource):

    @jwt_required
    def get(self) -> Response:
        """Endpoint (private) provides access to a given User's profile picture.
        
        Returns:
            Response -- The Flask response object.
        """
        user = User.get_by_email(get_jwt_identity())
        if user is None:
            return abort(403, "You are not permitted to access this endpoint.")
        picture = user.get_picture()
        if picture is None:
            return abort(404, "The given user doesn't have a profile picture set.")
        return picture

@API.route('/read')
class ReadUser(Resource):

    @jwt_required
    def get(self) -> Response:
        """Endpoint (private) provides access to a User's profile details.
        
        Returns:
            Response -- The Flask response object.
        """
        user = User.get_by_email(get_jwt_identity())
        if user is None:
            return abort(401, "You are not permitted to access this endpoint.")
        return make_response(jsonify(user.as_dict()), 200)

UPDATE_PARSER = API.parser()
UPDATE_PARSER.add_argument("fullname", type=str, required=False, help="The full name of the user", location="json")
UPDATE_PARSER.add_argument("currency_id", type=str, required=False, help="The ID of the base currency to be used", location="json")

@API.route('/update')
class UpdateUser(Resource):

    @jwt_required
    @API.expect(UPDATE_PARSER)
    def patch(self) -> Response:
        """Endpoint (private) allows a User to update their profile details.
        
        Returns:
            Response -- The Flask response object.
        """
        args = UPDATE_PARSER.parse_args()
        user = User.get_by_email(get_jwt_identity())
        if user is None:
            return abort(401, "You are not permitted to access this endpoint.")
        if args['fullname'] is not None:
            user.set_name(args['fullname'])
        if args['currency_id'] is not None:
            currency = Asset.get_by_id(args['currency_id'])
            if currency is None:
                return abort(400, "An invalud {currency_id} was given as a base currency.")
            user.set_base_currency(currency)
        user.save()
        return make_response(jsonify({"msg": "The user details have been successfully updated."}), 200)

UPLOAD_PARSER = API.parser()
UPLOAD_PARSER.add_argument("profile_picture", type=FileStorage, required=True, help='A profile picture for the user', location='files')

@API.route('/upload')
class UploadUser(Resource):

    @jwt_required
    @API.expect(UPLOAD_PARSER)
    def post(self) -> Response:
        """Endpoint (private) allows a user to update their profile picture (via upload).
        
        Returns:
            Response -- The Flask response object.
        """
        args = UPLOAD_PARSER.parse_args()
        user = User.get_by_email(get_jwt_identity())
        if user is None:
            return abort(401, "You are not permitted to access this endpoint.")
        user.set_picture(args['profile_picture'])
        user.save()
        return make_response(jsonify({"msg": "The user's profile picture has been successfully updated."}), 200)
