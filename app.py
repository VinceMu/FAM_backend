from subprocess import Popen

from api import API
from globals import APP as FLASK_APP
import local_config as CONFIG

Popen("python datalink.py")

API.init_app(FLASK_APP)

if __name__ == "__main__":
    FLASK_APP.run(host='0.0.0.0', port=CONFIG.PORT, debug=False)
