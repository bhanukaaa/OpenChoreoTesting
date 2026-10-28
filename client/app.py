import os
import requests
import json
import threading
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
from flask_socketio import SocketIO
import utility.rabbit_helper as rabbit_helper

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'swift_secret_key')

socketio = SocketIO(app, cors_allowed_origins="*")

API_GATEWAY_URL = os.getenv('API_GATEWAY_URL', 'http://api_gateway:8080')
ORDER_SERVICE_URL = os.getenv('ORDER_SERVICE_URL', f'{API_GATEWAY_URL}/orders')


def start_rabbit_listener():
    def callback(ch, method, properties, body):
        payload = json.loads(body)
        print(f" [x] RabbitMQ Received: {payload}", flush=True)
        socketio.emit('status_update', payload)

    try:
        connection = rabbit_helper.get_connection()
        channel = connection.channel()

        queue_config = {
            'queue_name': 'client_portal_events',
            'exclusive': False,
            'durable': True,
            'routing_keys': ['order.status.updated'],
            'auto_ack': True
        }

        rabbit_helper.listen_to_events(channel, 'swifttrack_events', queue_config, callback)
    except Exception as e:
        print(f"[!] RabbitMQ Listener failed in client portal: {e}", flush=True)


@app.route('/health', methods=['GET'])
@app.route('/healthz', methods=['GET'])
@app.route('/live', methods=['GET'])
def liveness():
    return jsonify({"status": "healthy", "service": "client_portal"}), 200


@app.route('/ready', methods=['GET'])
@app.route('/readyz', methods=['GET'])
def readiness():
    return jsonify({"status": "ready", "service": "client_portal"}), 200


@app.route('/')
def index():
    if 'userID' not in session:
        return redirect(url_for('login'))

    if session.get('role') != 'vendor':
        return redirect(url_for('track'))

    db_orders = []
    try:
        resp = requests.get(
            f"{ORDER_SERVICE_URL}/orders/{session['userID']}",
            headers={"Authorization": session.get('token', '')},
            timeout=5
        )
        if resp.status_code == 200:
            db_orders = resp.json()
    except Exception as e:
        print(f"[!] Error fetching orders from order service: {e}", flush=True)

    return render_template('dashboard.html', orders=db_orders)


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        try:
            auth_response = requests.post(f"{API_GATEWAY_URL}/auth/login", json={
                "username": username,
                "password": password
            }, timeout=5)

            if auth_response.status_code == 200:
                data = auth_response.json()
                session['token'] = data['token']
                session['userID'] = data['userID']
                session['username'] = data['username']
                session['role'] = data.get('role')

                if session['role'] == 'vendor':
                    return redirect(url_for('index'))
                elif session['role'] == 'driver':
                    return redirect(url_for('manifest'))
                else:
                    return redirect(url_for('track'))
            else:
                flash("Invalid credentials. Please use 123.")

        except Exception as e:
            flash(f"Could not connect to Auth Service: {e}")

    return render_template('login.html')


@app.route('/place_order', methods=['POST'])
def place_order():
    if 'userID' not in session:
        return redirect(url_for('login'))

    if session.get('role') != 'vendor':
        flash("Unauthorized action.", "error")
        return redirect(url_for('track'))

    order_data = {
        "userID": str(session.get('userID')),
        "deliveryAddress": request.form.get('deliveryAddress'),
        "packageName": request.form.get('package_name'),
        "packageWeight": float(request.form.get('package_weight', 1.0)),
        "receiverName": request.form.get('receiver_name'),
        "packageType": request.form.get('package_type', 'Standard')
    }

    try:
        response = requests.post(
            f"{ORDER_SERVICE_URL}/orders",
            json=order_data,
            headers={
                "Authorization": session.get('token', ''),
                "X-User-ID": str(session.get('userID'))
            },
            timeout=5
        )

        if response.status_code == 202:
            flash("Order placed successfully!", "success")
        else:
            flash(f"Order failed: {response.text}", "error")

    except requests.exceptions.RequestException as e:
        flash(f"Service unavailable: {e}", "error")

    return redirect(url_for('index'))


@app.route("/start_delivery", methods=['POST'])
def start_delivery():
    if 'userID' not in session or session.get('role') != 'driver':
        return jsonify({"success": False, "message": "Unauthorized"}), 401

    delivery_service_url = f'{API_GATEWAY_URL}/delivery/start_delivery/{session["userID"]}'
    try:
        response = requests.post(delivery_service_url, headers={
            "Authorization": session.get('token', ''),
            "X-User-ID": str(session.get('userID')),
            "userID": str(session.get('userID'))
        }, timeout=5)
        if response.status_code == 200:
            return jsonify({"success": True, "message": "Delivery started!"})
        else:
            return jsonify({"success": False, "message": response.text}), response.status_code
    except requests.exceptions.RequestException as e:
        return jsonify({"success": False, "message": str(e)}), 503


@app.route('/mark_delivered', methods=['POST'])
def mark_delivered():
    if 'userID' not in session or session.get('role') != 'driver':
        return jsonify({"success": False, "message": "Unauthorized"}), 401

    data = request.get_json() or {}
    order_id = data.get('orderID')
    delivery_service_url = f'{API_GATEWAY_URL}/delivery/mark_delivered/{order_id}'
    try:
        response = requests.post(delivery_service_url, headers={
            "Authorization": session.get('token', ''),
            "X-User-ID": str(session.get('userID')),
            "userID": str(session.get('userID'))
        }, timeout=5)
        if response.status_code == 200:
            return jsonify({"success": True})
        else:
            return jsonify({"success": False, "message": response.text})
    except requests.exceptions.RequestException as e:
        return jsonify({"success": False, "message": str(e)})


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


@app.route('/manifest')
def manifest():
    if 'userID' not in session:
        return redirect(url_for('login'))

    if session.get('role') != 'driver':
        flash("Unauthorized access.", "error")
        return redirect(url_for('index'))

    token = session.get('token', '')
    headers = {
        'Authorization': token,
        'X-User-ID': str(session.get('userID')),
        'userID': str(session.get('userID'))
    }

    delivery_service_url = f'{API_GATEWAY_URL}/delivery/manifest'

    try:
        response = requests.get(delivery_service_url, headers=headers, timeout=5)
        if response.status_code == 200:
            session.pop('manifest_triggered', None)
            manifest_data = response.json().get('manifestData', [])
            return render_template('manifest.html', orders=manifest_data)
        elif response.status_code == 202:
            if not session.get('manifest_triggered'):
                requests.post(delivery_service_url, headers=headers, timeout=5)
                session['manifest_triggered'] = True
            return render_template('manifest.html', orders=[], is_loading=True)
        else:
            flash(f"Error fetching manifest: {response.text}", "error")
            return render_template('manifest.html', orders=[])
    except requests.exceptions.RequestException as e:
        flash(f"Delivery Service unavailable: {e}", "error")
        return render_template('manifest.html', orders=[])


@app.route('/track')
def track():
    return render_template('track.html')


@app.route('/api/track/<order_id>', methods=['GET'])
def get_order_status(order_id):
    if not order_id:
        return jsonify({"error": "Missing order ID"}), 400

    delivery_service_url = f'{API_GATEWAY_URL}/delivery/status/{order_id}'

    try:
        resp = requests.get(delivery_service_url, timeout=5)
        if resp.status_code == 200:
            return jsonify(resp.json()), 200
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to Delivery Service: {e}", flush=True)

    return jsonify({"error": "Order not found"}), 404


if __name__ == '__main__':
    threading.Thread(target=start_rabbit_listener, daemon=True).start()
    port = int(os.getenv('PORT', 8000))
    socketio.run(app, host='0.0.0.0', port=port, allow_unsafe_werkzeug=True)
