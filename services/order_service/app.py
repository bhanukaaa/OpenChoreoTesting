import os
import json
import threading
import requests
import datetime
from flask import Flask, request, jsonify
import utility.rabbit_helper as rabbit_helper
from utility.db_helper import get_db_connection

app = Flask(__name__)

EXCHANGE_NAME = os.getenv('RABBITMQ_EXCHANGE', 'swifttrack_events')
AUTH_SERVICE_URL = os.getenv(
    'AUTH_SERVICE_URL', 'http://auth_service:8002/verify')

# --- Database Setup ---

def get_connection():
    return get_db_connection(schema='orders')


def init_db():
    with get_connection() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                "orderID"         SERIAL PRIMARY KEY,
                "userID"          TEXT,
                "deliveryAddress" TEXT,
                "receiverName"    TEXT,
                "packageName"     TEXT,
                status            TEXT DEFAULT 'PROCESSING',
                "createdAt"       TEXT,
                "cancelledAt"     TEXT,
                "cmsRefID"        INTEGER
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS order_packages (
                id              SERIAL PRIMARY KEY,
                "orderID"       INTEGER NOT NULL REFERENCES orders("orderID"),
                "packageID"     TEXT NOT NULL,
                "packageType"   TEXT,
                "packageWeight" REAL
            )
        ''')
        conn.commit()


# --- RabbitMQ Helper ---

def publish_event(routing_key, payload):
    try:
        connection = rabbit_helper.get_connection()
        channel = connection.channel()
        channel.exchange_declare(
            exchange=EXCHANGE_NAME, exchange_type='topic', durable=True)

        rabbit_helper.publish_event(
            channel, EXCHANGE_NAME, routing_key, payload)

        connection.close()
    except Exception as e:
        print(f"[!] Failed to publish {routing_key}: {e}", flush=True)


# --- Event Listener Logic (Caching Mode) ---

def start_event_listener():
    def callback(ch, method, properties, body):
        try:
            data = json.loads(body)
            rk = method.routing_key

            if rk == 'ros.address.added':
                order_id = int(data.get('orderID'))
                cms_ref_id = data.get('cmsRefID')
                package_ids = data.get('packageIDs', [])

                with get_connection() as conn:
                    conn.execute(
                        'UPDATE orders SET status = \'READY\', "cmsRefID" = %s WHERE "orderID" = %s',
                        (cms_ref_id, order_id)
                    )
                    conn.execute(
                        'DELETE FROM order_packages WHERE "orderID" = %s', (order_id,))
                    conn.executemany(
                        'INSERT INTO order_packages ("orderID", "packageID", "packageType", "packageWeight") VALUES (%s, %s, %s, %s)',
                        [(order_id, pid, "Standard", 1.0) for pid in package_ids]
                    )
                    conn.commit()
                publish_event('order.status.updated', {
                    "orderID": order_id, "status": "READY"})

            elif rk == 'wms.order.cancelled':
                order_id = int(data.get('orderID'))
                reason = data.get('reason', 'Unknown')
                print(
                    f"[!] Order Service: Order {order_id} cancelled — {reason}", flush=True)
                with get_connection() as conn:
                    conn.execute(
                        'UPDATE orders SET status = \'FAILED\', "cancelledAt" = %s WHERE "orderID" = %s',
                        (datetime.datetime.now().isoformat(), order_id)
                    )
                    conn.commit()
                publish_event('order.status.updated', {
                    "orderID": order_id, "status": "FAILED"})

            elif rk == 'delivery.getOrderDetails':
                requested_ids = data.get('orderIDs', [])
                cached_results = []
                with get_connection() as conn:
                    for oid in requested_ids:
                        order_row = conn.execute(
                            'SELECT * FROM orders WHERE "orderID" = %s', (int(oid),)).fetchone()
                        if order_row:
                            order_dict = dict(order_row)
                            pkgs = conn.execute(
                                'SELECT "packageID" FROM order_packages WHERE "orderID" = %s', (int(oid),)).fetchall()
                            order_dict['packageIDs'] = [p['packageID'] for p in pkgs]
                            cached_results.append(order_dict)

                print(cached_results)
                publish_event('cms.returnOrderDetails', cached_results)

            elif rk == 'delivery.startDelivery':
                order_ids = data.get('orderIDs', [])
                with get_connection() as conn:
                    for oid in order_ids:
                        conn.execute(
                            'UPDATE orders SET status = \'IN_TRANSIT\' WHERE "orderID" = %s', (int(oid),))
                        publish_event('order.status.updated', {
                            "orderID": oid, "status": "IN_TRANSIT"})
                    conn.commit()
                print("[*] Updated orders to IN_TRANSIT in DB", flush=True)

            elif rk == 'delivery.completed':
                order_id = int(data.get('orderID'))
                with get_connection() as conn:
                    conn.execute(
                        'UPDATE orders SET status = \'DELIVERED\' WHERE "orderID" = %s', (order_id,))
                    conn.commit()
                print("[*] Updated order:", order_id,
                      "to DELIVERED in DB", flush=True)
                publish_event('order.status.updated', {
                    "orderID": order_id, "status": "DELIVERED"})

            elif rk == 'dispatch.getRoutes':
                orderIDs = data.get('orderIDs', [])
                with get_connection() as conn:
                    for oid in orderIDs:
                        conn.execute(
                            'UPDATE orders SET status = \'DISPATCHED\' WHERE "orderID" = %s', (int(oid),))
                        publish_event('order.status.updated', {
                            "orderID": oid, "status": "DISPATCHED"})
                    conn.commit()
                    print("[*] Updated orders to DISPATCHED in DB", flush=True)

            elif rk == "dummy.data.seed":
                with get_connection() as conn:
                    vendors = ['4', '5', '6']
                    for v_id in vendors:
                        count = conn.execute(
                            'SELECT COUNT(*) FROM orders WHERE "userID" = %s', (v_id,)).fetchone()[0]
                        if count == 0:
                            print(
                                f"[*] Seeding 5 orders for Vendor {v_id}...", flush=True)
                            base_addr = {
                                '4': "123 Tech Park", '5': "456 Industrial Zone", '6': "789 Market St"}.get(v_id)
                            item_name = {'4': "High-End Laptop", '5': "Auto Parts Bundle",
                                         '6': "Books & Stationery"}.get(v_id)

                            for i in range(1, 6):
                                created_at = datetime.datetime.now().isoformat()
                                cursor = conn.execute(
                                    'INSERT INTO orders ("userID", "deliveryAddress", "receiverName", "packageName", "createdAt") VALUES (%s, %s, %s, %s, %s) RETURNING "orderID"',
                                    (v_id, f"{base_addr}, Bldg {i}", f"Customer {v_id}-{i}",
                                     f"{item_name} - Batch {i}", created_at)
                                )
                                new_order_id = cursor.fetchone()['orderID']

                                pkg_id = f"PKG-{v_id}-{new_order_id}"
                                conn.execute(
                                    'INSERT INTO order_packages ("orderID", "packageID", "packageType", "packageWeight") VALUES (%s, %s, %s, %s)',
                                    (new_order_id, pkg_id, "Standard", 1.5)
                                )

                                publish_event('order.initiated', {
                                    "orderID": new_order_id,
                                    "userID": v_id,
                                    "deliveryAddress": f"{base_addr}, Bldg {i}",
                                    "packageName": f"{item_name} - Batch {i}",
                                    "packageWeight": 1.5,
                                    "receiverName": f"Customer {v_id}-{i}",
                                    "packageType": "Standard"
                                })
                    conn.commit()

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"[!] Listener Error: {e}", flush=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    connection = rabbit_helper.get_connection()
    channel = connection.channel()

    queue_config = {
        'queue_name': 'order_service_cache_queue',
        'durable': True,
        'routing_keys': [
            'ros.address.added',
            'delivery.getOrderDetails',
            'dispatch.getRoutes',
            'delivery.startDelivery',
            'delivery.completed',
            'wms.order.cancelled',
            'dummy.data.seed'
        ],
        'auto_ack': False
    }

    rabbit_helper.listen_to_events(
        channel, EXCHANGE_NAME, queue_config, callback)


# --- Flask Routes ---

@app.route('/health', methods=['GET'])
@app.route('/healthz', methods=['GET'])
@app.route('/live', methods=['GET'])
def liveness():
    return jsonify({"status": "healthy", "service": "order"}), 200


@app.route('/ready', methods=['GET'])
@app.route('/readyz', methods=['GET'])
def readiness():
    try:
        with get_connection() as conn:
            conn.execute("SELECT 1")
        return jsonify({"status": "ready", "service": "order"}), 200
    except Exception as e:
        return jsonify({"status": "not ready", "error": str(e)}), 503


@app.route('/orders', methods=['POST'])
def place_order():
    token = request.headers.get('Authorization')
    if token:
        try:
            auth_resp = requests.post(AUTH_SERVICE_URL, json={"token": token}, timeout=5)
            if auth_resp.status_code != 200:
                return jsonify({"error": "Unauthorized"}), 401
        except Exception as e:
            print(f"[!] Auth Service check error: {e}", flush=True)

    data = request.get_json() or {}
    user_id = str(data.get('userID', request.headers.get('X-User-ID', 'guest_user')))
    package_name = data.get('packageName', 'Unknown Item')

    with get_connection() as conn:
        cursor = conn.execute(
            'INSERT INTO orders ("userID", "deliveryAddress", "receiverName", "packageName", "createdAt") VALUES (%s, %s, %s, %s, %s) RETURNING "orderID"',
            (user_id, data.get('deliveryAddress'), data.get('receiverName'),
             package_name, datetime.datetime.now().isoformat())
        )
        conn.commit()
        order_id = cursor.fetchone()['orderID']

    publish_event('order.initiated', {
        "orderID": order_id,
        "userID": user_id,
        "deliveryAddress": data.get('deliveryAddress'),
        "packageName": package_name,
        "receiverName": data.get('receiverName'),
        "packageWeight": float(data.get('packageWeight', 1.0)),
        "packageType": data.get('packageType', 'Standard')
    })

    return jsonify({"orderID": order_id, "status": "Accepted"}), 202


@app.route('/orders/<userID>', methods=['GET'])
def get_orders(userID):
    """Simple list for the UI, including packageName at the top level."""
    with get_connection() as conn:
        cursor = conn.execute(
            'SELECT * FROM orders WHERE "userID" = %s ORDER BY "orderID" DESC', (str(userID),))
        orders = [dict(row) for row in cursor.fetchall()]
    return jsonify(orders), 200


@app.route('/orders/detail/<int:orderID>', methods=['GET'])
def get_order_detail(orderID):
    with get_connection() as conn:
        row = conn.execute(
            'SELECT * FROM orders WHERE "orderID" = %s', (orderID,)).fetchone()
        if row:
            return jsonify(dict(row)), 200
        return jsonify({"error": "Order not found"}), 404


if __name__ == '__main__':
    init_db()
    threading.Thread(target=start_event_listener, daemon=True).start()
    port = int(os.getenv('PORT', 8001))
    app.run(host='0.0.0.0', port=port)
