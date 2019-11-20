from flask import Flask
from flask_jwt_extended import JWTManager
from flask_cors import CORS
from mongoengine import connect

import local_config as CONFIG

APP = Flask(__name__)
CORS(APP)
APP.config["MONGO_URI"] = CONFIG.MONGODB
APP.config["JWT_SECRET_KEY"] = CONFIG.JWT_SECRET_KEY
APP.config['JWT_BLACKLIST_ENABLED'] = True
APP.config['JWT_BLACKLIST_TOKEN_CHECKS'] = ['access', 'refresh']
APP.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
JWT = JWTManager(APP)
connect('FAM', host=CONFIG.MONGODB + "/" + CONFIG.DB)
