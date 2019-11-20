import os
import logging

MONGODB = "mongodb://localhost"
DB = "fam"
TOKEN_EXPIRY = False
PORT = 5000
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "fam")
REFRESH_INTERVAL = 600
WORKER_THREADS = 4
ERROR_WAIT_TIME = 10
MAX_RETRIES = 5
LIMIT_ASSETS = True
LIMIT_ASSETS_QUANTITY = 10
logging.basicConfig()
DATA_LOGGER = logging.getLogger("DataLink")
DATA_LOGGER.setLevel(logging.ERROR)
REST_LOGGER = logging.getLogger("REST")
REST_LOGGER.setLevel(logging.ERROR)
DEFAULT_KEY = "[insert key here]"
os.environ['ALPHAVANTAGE_API_KEY'] = DEFAULT_KEY
if os.path.exists("datafeed/defaults/key.txt"):
    try:
        F = open("datafeed/defaults/key.txt", "r")
        os.environ['ALPHAVANTAGE_API_KEY'] = F.readline().strip()
    except Exception:
        DATA_LOGGER.error("Error occurred loading AlphaVantage API key.")
