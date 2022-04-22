from . import routes, VERSION, logger
import json

@routes.route("/")
def main():
	return json.dumps("Launch RT Results")
