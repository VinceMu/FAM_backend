from globals import mongo_user
from flask_jwt_extended import create_access_token,create_refresh_token
from local_config import TOKEN_EXPIRY
import hashlib


def authenticate_user(email,password):
    found_user = mongo_user.db.auth.find_one({"email":email})
    if found_user == None:
        return False, None
    salt = found_user["salt"]
    salted_password = str(password + salt).encode('utf8')
    hash_password = hashlib.sha256(salted_password).hexdigest()
    if hash_password == found_user["password"]:
        return True, found_user
    return False, None


def generate_tokens(user_id):
    return {
            "access_token":create_access_token(identity=user_id, expires_delta=TOKEN_EXPIRY),
            "refresh_token":create_refresh_token(identity=user_id, expires_delta=TOKEN_EXPIRY)
            }


def generate_access_token(user_id):
    return {"access_token":create_access_token(identity=user_id, expires_delta=TOKEN_EXPIRY)}
