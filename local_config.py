import os

MONGODB = "mongodb://localhost"
DB = "fam"
TOKEN_EXPIRY = False
PORT = 5000
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY","fam")
REFRESH_INTERVAL = 600
WORKER_THREADS = 5
ERROR_WAIT_TIME = 10
os.environ['ALPHAVANTAGE_API_KEY'] = "[insert key here]"
