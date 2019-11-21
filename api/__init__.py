from flask_restplus import Api
from globals import JWT
from api.assets import API as ASSETS_API
from api.authentication import API as AUTHENTICATION_API
from api.transactions import API as TRANSACTIONS_API
from api.users import API as USERS_API
from api.trends import API as TRENDS_API

API = Api(
    title='FAM REST Api',
    version='1.0',
    description='',
    doc="/swagger"
)
JWT._set_error_handler_callbacks(API)

API.add_namespace(ASSETS_API)
API.add_namespace(AUTHENTICATION_API)
API.add_namespace(TRANSACTIONS_API)
API.add_namespace(USERS_API)
API.add_namespace(TRENDS_API)
