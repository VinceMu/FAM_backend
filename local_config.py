import os
import logging
#########
#MongoDB#
#########

# The location to connect to MongoDB on
MONGODB = "mongodb://localhost"
# The name of the database to store information under
DB = "fam"
# The port the database server is operating on
PORT = 5000

################
#Authentication#
################

# The secret key to use for JWT tokens
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "fam")
# Whether or not tokens expire within the JWT
TOKEN_EXPIRY = False

##########
#DataLink#
##########

# The sleeping time between checking whether asset classes need refreshing
REFRESH_INTERVAL = 60
# The default live asset price updating interval - default is 60 seconds
LIVE_UPDATE_INTERVAL = 600
# The maximum number of worker threads to use when doing DataLink updates
WORKER_THREADS = 4
# The time to wait before retrying when an error occurs during obtaining data
ERROR_WAIT_TIME = 10
# The maximum number of retries for data before failing and giving up
MAX_RETRIES = 5
# Whether to limit the number of stock assets loaded into the platform
LIMIT_ASSETS = True
# The number of stock assets the platform is limited to
LIMIT_ASSETS_QUANTITY = 10

#########
#Logging#
#########

# Initialise the logging for the backend
logging.basicConfig()
# Setup the logger for the DataLink
DATA_LOGGER = logging.getLogger("DataLink")
# The logging level for the DataLink - logging.ERROR is default
DATA_LOGGER.setLevel(logging.INFO)
# Setup the logger for the REST API
REST_LOGGER = logging.getLogger("REST")
# The logging level for the REST API - logging.ERROR is default
REST_LOGGER.setLevel(logging.ERROR)

###########
#Uploading#
###########

PROFILE_PIC_TYPES = ['image/jpeg', 'image/png', 'image/gif']

##################
#AlphaVantage API#
##################

# The default key used for obtaining AlphaVantage data
DEFAULT_KEY = "[insert key here]"
# The file where the AlphaVantage key is stored
DEFAULT_KEY_LOCATION = "datafeed/defaults/key.txt"
os.environ['ALPHAVANTAGE_API_KEY'] = DEFAULT_KEY
# Load the key from the file if it exists
if os.path.exists(DEFAULT_KEY_LOCATION):
    try:
        F = open(DEFAULT_KEY_LOCATION, "r")
        os.environ['ALPHAVANTAGE_API_KEY'] = F.readline().strip()
    except Exception:
        DATA_LOGGER.error("Error occurred loading AlphaVantage API key.")
