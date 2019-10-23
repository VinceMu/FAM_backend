from flask import Flask
from flask_pymongo import PyMongo
from flask_jwt_extended import JWTManager
from flask_cors import CORS
import local_config

app = Flask(__name__)
CORS(app)
app.config["MONGO_URI"] = local_config.MONGODB
app.config["JWT_SECRET_KEY"] = local_config.JWT_SECRET_KEY
jwt = JWTManager(app)
mongo = PyMongo(app)
mongo_user = PyMongo(app,uri=local_config.MONGODB+"/"+local_config.DB)
