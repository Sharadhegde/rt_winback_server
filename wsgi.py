from flask import Flask, make_response, jsonify
from routes import *
# from waitress import serve
# from flask_cors import CORS

app = Flask(__name__)
app.register_blueprint(routes)

# cors = CORS(app, resources={r"/*": {"origins": "*"}})

if __name__ == '__main__':
    app.run(host='0.0.0.0')
    # serve(app, port=5000)
