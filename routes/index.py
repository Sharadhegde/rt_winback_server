from . import routes, VERSION, logger
import json

@routes.route("/")
def index():
	return json.dumps("Launch RT Results")
