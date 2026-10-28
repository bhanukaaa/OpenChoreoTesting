from flask import Flask, jsonify, request
import requests
from utility.rabbit_helper import get_connection

app = Flask(__name__)


@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "up"}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)  # <------------ CHANGE
