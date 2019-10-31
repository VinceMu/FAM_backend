from globals import app
from api import api
import local_config, os
from subprocess import Popen

Popen("python datalink.py")

api.init_app(app)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=local_config.PORT,debug=True)
