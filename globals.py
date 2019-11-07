from flask import Flask
from flask_jwt_extended import JWTManager
from flask_cors import CORS
from mongoengine import connect
import local_config

app = Flask(__name__)
CORS(app)
app.config["MONGO_URI"] = local_config.MONGODB
app.config["JWT_SECRET_KEY"] = local_config.JWT_SECRET_KEY
app.config['JWT_BLACKLIST_ENABLED'] = True
app.config['JWT_BLACKLIST_TOKEN_CHECKS'] = ['access', 'refresh']
jwt = JWTManager(app)
connect('FAM', host=local_config.MONGODB + "/" + local_config.DB)
