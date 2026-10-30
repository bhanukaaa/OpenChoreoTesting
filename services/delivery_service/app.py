import os
import json
import threading
from flask import Flask, jsonify, request
import requests
from utility.rabbit_helper import get_connection
from utility.db_helper import get_db_connection

app = Flask(__name__)

# Config
DISPATCH_SERVICE_URL = os.getenv(
    'DISPATCH_SERVICE_URL', 'http://dispatch_service:8006')
ORDER_SERVICE_URL = os.getenv(
    'ORDER_SERVICE_URL', 'http://order_service:8001')
EXCHANGE_NAME = os.getenv('RABBITMQ_EXCHANGE', 'swifttrack_events')


# Database connection
def get_connection_db():
    return get_db_connection(schema='delivery')


def init_db():
    with get_connection_db() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS manifests (
                "driverID"     INTEGER PRIMARY KEY,
                "manifestID"   INTEGER,
                "driverName"   TEXT,
                status         TEXT DEFAULT 'PENDING',
                "manifestData" JSONB,
                "updatedAt"    TIMESTAMP DEFAULT NOW()
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                "orderID"    TEXT PRIMARY KEY,
                "driverID"   INTEGER,
                "manifestID" INTEGER,
                status       TEXT,
                "updatedAt"  TIMESTAMP DEFAULT NOW()
            )
        ''')
        conn.commit()


# RabbitMQ publisher
def publish_event(routing_key, payload):
    try:
        connection = get_connection()
        channel = connection.channel()
        channel.exchange_declare(
            exchange=EXCHANGE_NAME, exchange_type='topic', durable=True)

        from utility import rabbit_helper
        rabbit_helper.publish_event(
            channel, EXCHANGE_NAME, routing_key, payload)

        connection.close()
    except Exception as e:
        print(f"[!] Failed to publish event: {e}", flush=True)


def publish_order_details(orderIDs, driverID):
    if not orderIDs:
        print("[*] publish_order_details called with empty list", flush=True)
        return

    payload = {
        "orderIDs": orderIDs,
        "driverID": driverID
    }
    publish_event('delivery.getOrderDetails', payload)


def fetch_driver_manifest_from_dispatch(driver_id):
    try:
        resp = requests.get(f"{DISPATCH_SERVICE_URL}/manifests/driver/{driver_id}", timeout=5)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"[!] Error contacting dispatch service for driver {driver_id}: {e}", flush=True)
    return None


def fetch_order_manifest_from_dispatch(order_id):
    try:
        resp = requests.get(f"{DISPATCH_SERVICE_URL}/manifests/order/{order_id}", timeout=5)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"[!] Error contacting dispatch service for order {order_id}: {e}", flush=True)
    return None


def createManifest(data):
    manifest_data = []
    driver_id = None
    driver_name = "Assigned Driver"
    manifest_id = None

    if data and len(data) > 0:
        first_order_id = str(data[0].get('orderID'))
        dispatch_info = fetch_order_manifest_from_dispatch(first_order_id)
        if dispatch_info:
            driver_id = dispatch_info.get('driverID')
            driver_name = dispatch_info.get('driverName')
            manifest_id = dispatch_info.get('manifestID')

    for order in data:
        manifest_data.append({
            "orderID": order.get('orderID'),
            "receiverName": order.get('receiverName'),
            "packageName": order.get('packageName'),
            "status": order.get('status', 'DISPATCHED'),
            "deliveryAddress": order.get('deliveryAddress'),
            "driverName": driver_name if driver_id else None
        })

    if driver_id:
        print(f"[*] Storing manifest for driver {driver_id} in PostgreSQL", flush=True)
        with get_connection_db() as conn:
            conn.execute('''
                INSERT INTO manifests ("driverID", "manifestID", "driverName", status, "manifestData", "updatedAt")
                VALUES (%s, %s, %s, 'DISPATCHED', %s, NOW())
                ON CONFLICT ("driverID") DO UPDATE
                SET "manifestData" = EXCLUDED."manifestData",
                    "manifestID" = EXCLUDED."manifestID",
                    "driverName" = EXCLUDED."driverName",
                    status = EXCLUDED.status,
                    "updatedAt" = NOW()
            ''', (driver_id, manifest_id, driver_name, json.dumps(manifest_data)))

            for order in manifest_data:
                conn.execute('''
                    INSERT INTO orders ("orderID", "driverID", "manifestID", status, "updatedAt")
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT ("orderID") DO UPDATE
                    SET status = EXCLUDED.status,
                        "driverID" = EXCLUDED."driverID",
                        "manifestID" = EXCLUDED."manifestID",
                        "updatedAt" = NOW()
                ''', (str(order['orderID']), driver_id, manifest_id, order.get('status', 'DISPATCHED')))
            conn.commit()

    publish_event('delivery.manifestCreated', {
        "manifestData": manifest_data, "driverID": driver_id})


def start_rabbit_listener():
    def callback(ch, method, properties, body):
        if method.routing_key == 'cms.returnOrderDetails':
            data = json.loads(body)
            print(data, flush=True)
            createManifest(data)

    connection = get_connection()
    channel = connection.channel()

    queue_config = {
        'queue_name': 'delivery_order_status_updates',
        'durable': True,
        'routing_keys': ['cms.returnOrderDetails', 'wms.returnPackageDetails'],
        'auto_ack': True
    }

    from utility import rabbit_helper as rh
    rh.listen_to_events(channel, EXCHANGE_NAME, queue_config, callback)
    print("[*] Delivery Service Listener Started.", flush=True)


# --- Routes ---

@app.route('/health', methods=['GET'])
@app.route('/healthz', methods=['GET'])
@app.route('/live', methods=['GET'])
def liveness():
    return jsonify({"status": "healthy", "service": "delivery"}), 200


@app.route('/ready', methods=['GET'])
@app.route('/readyz', methods=['GET'])
def readiness():
    try:
        with get_connection_db() as conn:
            conn.execute("SELECT 1")
        return jsonify({"status": "ready", "service": "delivery"}), 200
    except Exception as e:
        return jsonify({"status": "not ready", "error": str(e)}), 503


@app.route('/manifest', methods=['GET', 'POST'])
def manifest():
    token = request.headers.get('Authorization')
    if not token:
        return jsonify({"error": "Unauthorized"}), 401

    raw_user_id = request.headers.get('X-User-ID') or request.headers.get('userID')
    if not raw_user_id:
        return jsonify({"error": "Missing user ID header"}), 400

    try:
        driver_id = int(raw_user_id)
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid driver ID"}), 400

    if request.method == 'GET':
        with get_connection_db() as conn:
            row = conn.execute(
                'SELECT "manifestData" FROM manifests WHERE "driverID" = %s', (driver_id,)).fetchone()
            if row and row['manifestData']:
                return jsonify({"manifestData": row['manifestData']}), 200

        dispatch_data = fetch_driver_manifest_from_dispatch(driver_id)
        if dispatch_data and dispatch_data.get('stops'):
            return jsonify({"status": "processing"}), 202
        else:
            return jsonify({"manifestData": []}), 200

    # POST - trigger manifest detail generation
    dispatch_data = fetch_driver_manifest_from_dispatch(driver_id)
    if dispatch_data and dispatch_data.get('stops'):
        order_ids = [stop['orderID'] for stop in dispatch_data['stops']]
        publish_order_details(order_ids, driver_id)
        return jsonify({"message": "Manifest received and processed."}), 200
    else:
        return jsonify({"message": "No manifest found for this driver."}), 200


@app.route("/start_delivery/<driver_id>", methods=['POST'])
def start_delivery(driver_id):
    try:
        d_id = int(driver_id)
    except ValueError:
        return jsonify({"error": "Invalid driver ID"}), 400

    print(f"[*] Starting delivery for driver {d_id}", flush=True)

    order_ids = []
    with get_connection_db() as conn:
        row = conn.execute(
            'SELECT "manifestData" FROM manifests WHERE "driverID" = %s', (d_id,)).fetchone()
        if row and row['manifestData']:
            manifest_list = row['manifestData']
            for item in manifest_list:
                item['status'] = 'IN_TRANSIT'
                order_ids.append(str(item.get('orderID')))
                conn.execute(
                    'UPDATE orders SET status = \'IN_TRANSIT\', "updatedAt" = NOW() WHERE "orderID" = %s',
                    (str(item.get('orderID')),)
                )

            conn.execute('''
                UPDATE manifests 
                SET "manifestData" = %s, status = 'IN_TRANSIT', "updatedAt" = NOW() 
                WHERE "driverID" = %s
            ''', (json.dumps(manifest_list), d_id))
            conn.commit()

    if not order_ids:
        dispatch_data = fetch_driver_manifest_from_dispatch(d_id)
        if dispatch_data and dispatch_data.get('stops'):
            order_ids = [str(s['orderID']) for s in dispatch_data['stops']]

    publish_event('delivery.startDelivery', {"orderIDs": order_ids})
    return jsonify({"message": f"Delivery started for driver {d_id}.", "success": True}), 200


@app.route("/mark_delivered/<order_id>", methods=['POST'])
def mark_delivered(order_id):
    print(f"[*] Delivery complete for order {order_id}", flush=True)
    publish_event('delivery.completed', {"orderID": order_id})

    with get_connection_db() as conn:
        conn.execute(
            'UPDATE orders SET status = \'DELIVERED\', "updatedAt" = NOW() WHERE "orderID" = %s',
            (str(order_id),)
        )
        row = conn.execute(
            'SELECT "driverID" FROM orders WHERE "orderID" = %s', (str(order_id),)).fetchone()
        if row and row['driverID']:
            driver_id = row['driverID']
            m_row = conn.execute(
                'SELECT "manifestData" FROM manifests WHERE "driverID" = %s', (driver_id,)).fetchone()
            if m_row and m_row['manifestData']:
                manifest_list = m_row['manifestData']
                for item in manifest_list:
                    if str(item.get('orderID')) == str(order_id):
                        item['status'] = 'DELIVERED'
                        break
                conn.execute(
                    'UPDATE manifests SET "manifestData" = %s, "updatedAt" = NOW() WHERE "driverID" = %s',
                    (json.dumps(manifest_list), driver_id)
                )
        conn.commit()

    return jsonify({"message": f"Order {order_id} marked as delivered."}), 200


@app.route('/status/<order_id>', methods=['GET'])
def get_status(order_id):
    str_order_id = str(order_id)

    with get_connection_db() as conn:
        o_row = conn.execute(
            'SELECT "orderID", "driverID", "manifestID", status FROM orders WHERE "orderID" = %s',
            (str_order_id,)
        ).fetchone()

        if o_row and o_row['driverID']:
            driver_id = o_row['driverID']
            m_row = conn.execute(
                'SELECT "driverName", "manifestData" FROM manifests WHERE "driverID" = %s',
                (driver_id,)
            ).fetchone()

            if m_row and m_row['manifestData']:
                manifest_list = m_row['manifestData']
                for i, order in enumerate(manifest_list):
                    if str(order.get('orderID')) == str_order_id:
                        delivered_before = sum(1 for prev in manifest_list[:i] if prev.get('status') == 'DELIVERED')
                        progress_pct = int((delivered_before / i) * 100) if i > 0 else (100 if order.get('status') in ['DISPATCHED', 'IN_TRANSIT'] else 0)

                        return jsonify({
                            "orderID": order.get('orderID'),
                            "status": order.get('status'),
                            "packageName": order.get('packageName'),
                            "driverID": driver_id,
                            "deliveryAddress": order.get('deliveryAddress'),
                            "driverName": m_row['driverName'],
                            "progress": progress_pct,
                            "stopNumber": i + 1,
                            "totalStops": len(manifest_list)
                        }), 200

        if o_row:
            return jsonify({"orderID": str_order_id, "status": o_row['status']}), 200

    # Fallback: check Order Service directly via REST
    try:
        resp = requests.get(f"{ORDER_SERVICE_URL}/orders/detail/{str_order_id}", timeout=3)
        if resp.status_code == 200:
            data = resp.json()
            return jsonify({"orderID": str_order_id, "status": data.get('status')}), 200
    except Exception as e:
        print(f"[!] Error querying order service for order {str_order_id}: {e}", flush=True)

    return jsonify({"error": "Order not found"}), 404


if __name__ == '__main__':
    init_db()
    threading.Thread(target=start_rabbit_listener, daemon=True).start()
    port = int(os.getenv('PORT', 8007))
    app.run(host='0.0.0.0', port=port)
