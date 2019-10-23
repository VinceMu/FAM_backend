import os

MONGODB = "mongodb://localhost"
DB = "fam"
TOKEN_EXPIRY = False
PORT = 5000
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY","fam")
