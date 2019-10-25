from flask_restplus import Api
from .authentication import api as auth_api

api = Api(
    title='FAM REST Api',
    version='1.0',
    description='',
    doc="/swagger"
)
api.add_namespace(auth_api)
