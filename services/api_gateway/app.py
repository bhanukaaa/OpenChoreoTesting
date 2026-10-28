import os
import requests
from flask import Flask, request, jsonify, Response

app = Flask(__name__)

# Service mapping
SERVICES = {
    'auth': os.environ.get('AUTH_SERVICE_URL', 'http://auth_service:8002'),
    'orders': os.environ.get('ORDER_SERVICE_URL', 'http://order_service:8001'),
    'cms': os.environ.get('CMS_SERVICE_URL', 'http://cms_system:8003'),
    'wms': os.environ.get('WMS_SERVICE_URL', 'http://wms_system:8004'),
    'ros': os.environ.get('ROS_SERVICE_URL', 'http://ros_system:8005'),
    'dispatch': os.environ.get('DISPATCH_SERVICE_URL', 'http://dispatch_service:8006'),
    'delivery': os.environ.get('DELIVERY_SERVICE_URL', 'http://delivery_service:8007'),
}

# Paths that do not require authentication
PUBLIC_PATHS = [
    '/auth/login',
    '/auth/register',
    '/delivery/status'
]


@app.route('/health', methods=['GET'])
@app.route('/healthz', methods=['GET'])
@app.route('/live', methods=['GET'])
def liveness():
    return jsonify({"status": "healthy", "service": "api_gateway"}), 200


@app.route('/ready', methods=['GET'])
@app.route('/readyz', methods=['GET'])
def readiness():
    return jsonify({"status": "ready", "service": "api_gateway"}), 200


def check_auth(req):
    """
    Verifies the token with the Auth Service.
    Returns user info dict if valid, None otherwise.
    """
    token = req.headers.get('Authorization')
    if token and token.startswith('Bearer '):
        token = token.split(' ')[1]

    if not token:
        return None

    try:
        auth_service_url = SERVICES['auth']
        resp = requests.post(f"{auth_service_url}/verify", json={'token': token}, timeout=3)
        if resp.status_code == 200:
            return resp.json()
    except requests.RequestException as e:
        print(f"Auth Service Unreachable: {e}")
        return None

    return None


@app.route('/<service_name>/', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE'])
@app.route('/<service_name>/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def gateway(service_name, path):
    full_path = f"/{service_name}/{path}"

    if service_name not in SERVICES:
        return jsonify({'error': f'Service "{service_name}" not found'}), 404

    target_url_base = SERVICES[service_name]
    target_url = f"{target_url_base}/{path}" if path else target_url_base

    headers = {k: v for k, v in request.headers if k.lower() != 'host'}

    is_public = any(full_path.startswith(p) for p in PUBLIC_PATHS)

    if not is_public:
        user_info = check_auth(request)
        if not user_info:
            return jsonify({'error': 'Unauthorized', 'message': 'Invalid or missing token'}), 401

        headers['X-User-ID'] = str(user_info.get('userID'))
        headers['userID'] = str(user_info.get('userID'))
        headers['X-Role'] = str(user_info.get('role'))
        headers['X-Username'] = str(user_info.get('username'))

    try:
        resp = requests.request(
            method=request.method,
            url=target_url,
            headers=headers,
            data=request.get_data(),
            cookies=request.cookies,
            allow_redirects=False,
            timeout=10
        )

        excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
        headers_response = [(name, value) for (name, value) in resp.raw.headers.items()
                            if name.lower() not in excluded_headers]

        return Response(resp.content, resp.status_code, headers_response)

    except requests.exceptions.RequestException as e:
        return jsonify({'error': 'Service Unavailable', 'details': str(e)}), 503


if __name__ == '__main__':
    port = int(os.getenv('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
