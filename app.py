from globals import app
from api import api
import local_config

api.init_app(app)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=local_config.PORT,debug=True)
