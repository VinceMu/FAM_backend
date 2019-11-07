from flask_restplus import Api
from globals import jwt
from .assets import api as assets_api
from .authentication import api as auth_api
from .transactions import api as transactions_api
from .users import api as users_api

api = Api(
    title='FAM REST Api',
    version='1.0',
    description='',
    doc="/swagger"
)
jwt._set_error_handler_callbacks(api)
api.add_namespace(assets_api)
api.add_namespace(auth_api)
api.add_namespace(transactions_api)
api.add_namespace(users_api)
