from flask_restplus import Api
from globals import jwt
from .authentication import api as auth_api
from .userassets import api as userassets_api

api = Api(
    title='FAM REST Api',
    version='1.0',
    description='',
    doc="/swagger"
)
jwt._set_error_handler_callbacks(api)
api.add_namespace(auth_api)
api.add_namespace(userassets_api)
